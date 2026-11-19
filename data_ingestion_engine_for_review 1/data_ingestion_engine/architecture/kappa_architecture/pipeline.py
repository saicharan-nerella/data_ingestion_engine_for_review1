"""
architecture/kappa_architecture/pipeline.py
────────────────────────────────────────────
Kappa Architecture: One processing path for everything.

Unlike Lambda (a batch layer for historical accuracy + a separate speed
layer for low latency — two code paths that can drift out of sync), Kappa
treats every record the same way regardless of where it came from: a
batch reader's historical rows and a streaming reader's live events run
through the exact same tag/normalize/mask/dedupe logic below. "Historical
reprocessing" in a true Kappa system just means replaying the stream from
the beginning through that one path — there's no separate batch recompute
to maintain.

Rebuilt from scratch — the previous version had four real problems:
  1. It imported `from architecture.base import BaseArchitecture`, a
     module that doesn't exist anywhere in this repo — ModuleNotFoundError
     before a single line of its own logic ran.
  2. It had no execute() method at all (only setup()/run()/teardown()),
     but main/aimatic.py calls pipeline.execute() — the one method every
     other architecture actually implements.
  3. It called writer.connect() / writer.commit() / writer.close() —
     methods that don't exist on CloudWriter, DataWarehouseWriter, or
     DatabaseWriter (all three only implement write(record)/flush()).
  4. The transform step was a commented-out placeholder
     (`# result = validate(clean(transform(batch)))`) — raw batches were
     written through completely unchanged. Replay mode was also a no-op
     (`# reader.reset_offset("earliest")`, never actually called).

Streaming readers are drained for a bounded window (kappa.window_seconds,
set from --kappa_window_seconds — default 30s), one background thread per
reader, the same concurrency shape as
architecture/lambda_architecture/speed_layer.py: a real Kappa system runs
forever, but a Glue job runs to completion, so this is the same accepted
compromise already used for Lambda's Speed layer, not a new one invented
here. Batch readers need no such bounding — their read() terminates on
its own.

Replay (--kappa_replay true) resets a streaming reader's consumer position
to the beginning, but only when the reader actually supports it (checked
via hasattr, never assumed) — no reader class in this repo defines
reset_offset() today, so replay currently always logs a clear "not
supported" warning rather than silently doing nothing. Adding real offset
management to StreamingKafkaReader is separate work (consumer-group
rewind via confluent-kafka's assign()/seek()), not done here.

Interface: write(record)/flush() — identical contract to every other
writer-consuming architecture in this codebase.
"""

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

from architecture.lambda_architecture.record_keys import extract_source, get_record_key
from core.null_handling import normalize_nulls
from core.pii_masking import PIIMasker
from alerts.channel_senders import create_notification_service
from core.alert_rules import check_alert_rules

log = logging.getLogger(__name__)


class KappaArchitecture:
    """
    Single-path stream processor: collect (batch readers to exhaustion,
    streaming readers for a bounded window), tag + normalize + mask +
    dedupe once, write once. No separate batch/speed views to reconcile.
    """

    def __init__(self, config: dict, writers: List = None):
        self.config = config
        self.writers = writers or []
        self.pii_masker = PIIMasker(config)

        kappa_cfg = config.get("kappa", {})
        self.window_seconds = kappa_cfg.get("window_seconds", 30)
        self.replay = kappa_cfg.get("replay", False)

        # Force batch mode so writers buffer and emit one file/load per
        # run, same as Lambda/Medallion.
        for writer in self.writers:
            writer._batch_mode = True

        log.info("KappaArchitecture: initialized with %d writer(s) (window=%ss, replay=%s)",
                 len(self.writers), self.window_seconds, self.replay)

    def execute(self, batch_readers: List, streaming_readers: List = None) -> List[Dict[str, Any]]:
        batch_readers = batch_readers or []
        streaming_readers = streaming_readers or []
        log.info("KappaArchitecture: executing with %d batch reader(s), %d streaming reader(s)",
                 len(batch_readers), len(streaming_readers))

        raw_records = self._read_batch(batch_readers)
        raw_records.extend(self._read_streaming(streaming_readers))

        processed = self._process(raw_records)
        self._write_output(processed)

        log.info("KappaArchitecture: completed successfully")
        return processed

    # ── Reading ──────────────────────────────────────────────────────────────

    def _read_batch(self, readers: List) -> List[Dict[str, Any]]:
        raw = []
        for reader in readers:
            try:
                reader.connect()
                for batch in reader.read():
                    if isinstance(batch, list):
                        raw.extend(batch)
                    elif isinstance(batch, dict):
                        raw.append(batch)
            finally:
                reader.close()
        log.info("KappaArchitecture: collected %d record(s) from %d batch reader(s)",
                 len(raw), len(readers))
        return raw

    def _read_streaming(self, readers: List) -> List[Dict[str, Any]]:
        if not readers:
            return []

        if self.replay:
            for reader in readers:
                if hasattr(reader, "reset_offset"):
                    reader.reset_offset("earliest")
                    log.info("KappaArchitecture: replay — reset %s to earliest",
                             reader.__class__.__name__)
                else:
                    log.warning(
                        "KappaArchitecture: --kappa_replay=true was requested but %s has "
                        "no reset_offset() — draining from its current position instead",
                        reader.__class__.__name__,
                    )

        collected: List[Dict[str, Any]] = []
        lock = threading.Lock()
        stop_event = threading.Event()
        deadline = time.monotonic() + self.window_seconds

        def drain(reader):
            try:
                reader.connect()
            except Exception as exc:
                log.warning("KappaArchitecture: reader %s failed to connect — %s",
                           reader.__class__.__name__, exc)
                return
            try:
                for batch in reader.read():
                    if stop_event.is_set() or time.monotonic() >= deadline:
                        break
                    if batch:
                        with lock:
                            collected.extend(batch)
                    else:
                        time.sleep(0.1)
            except Exception as exc:
                log.error("KappaArchitecture: reader %s failed while draining — %s",
                          reader.__class__.__name__, exc, exc_info=True)
            finally:
                reader.close()

        threads = [threading.Thread(target=drain, args=(reader,), daemon=True) for reader in readers]
        for t in threads:
            t.start()

        # Wait out the full window even if every thread's own read() loop
        # exits early — same reasoning as SpeedLayer.stop_and_join().
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        stop_event.set()
        for t in threads:
            t.join(timeout=self.window_seconds + 5)

        log.info("KappaArchitecture: drained %d record(s) from %d streaming reader(s) over %ss",
                 len(collected), len(readers), self.window_seconds)
        return collected

    # ── Processing (the one path every record goes through) ────────────────

    def _process(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        process_time = datetime.utcnow().isoformat()
        by_key: Dict[Any, Dict[str, Any]] = {}
        unkeyed: List[Dict[str, Any]] = []

        for idx, record in enumerate(raw_records):
            source = extract_source(record)
            tagged = {
                "_kappa_layer": "stream",
                "_kappa_process_time": process_time,
                "_kappa_source": source,
                **record,
            }
            tagged = normalize_nulls(tagged)
            tagged = self.pii_masker.mask(tagged)
            tagged["_kappa_pii_masked"] = self.pii_masker.enabled

            key = get_record_key(tagged, source)
            if key is None:
                tagged["_kappa_record_index"] = idx
                unkeyed.append(tagged)
            else:
                by_key[key] = tagged

        result = list(by_key.values()) + unkeyed
        log.info(
            "KappaArchitecture: processed %d record(s) -> %d (%d keyed, %d unkeyed, %d duplicate(s) collapsed)",
            len(raw_records), len(result), len(by_key), len(unkeyed),
            len(raw_records) - len(unkeyed) - len(by_key),
        )
        return result

    # ── Writing ──────────────────────────────────────────────────────────────

    def _write_output(self, records: List[Dict[str, Any]]) -> None:
        if not self.writers:
            log.debug("KappaArchitecture: no writers configured, skipping output")
            return

        for writer in self.writers:
            try:
                for record in records:
                    if isinstance(record, dict):
                        writer.write(record)
                    else:
                        log.warning("KappaArchitecture: skipping non-dict record: %s", type(record))
                writer.flush()
                log.info("KappaArchitecture: wrote %d record(s) via %s",
                         len(records), writer.__class__.__name__)
            except Exception as exc:
                log.error("KappaArchitecture: writer %s failed — %s",
                          writer.__class__.__name__, exc)
                raise


class Pipeline:
    """
    Entry point the UI/CLI runs when --architecture kappa is selected.
    Resolved by core/registry.py and instantiated by
    core/pipeline_builder.py with the same five arguments every other
    architecture gets.
    """

    def __init__(self, config: dict, readers: List, writers: List,
                 orchestrator=None, alert_handlers: List = None):
        self.config = config
        self.readers = readers
        self.writers = writers
        self.orchestrator = orchestrator
        self.alert_handlers = alert_handlers or []
        self.architecture = KappaArchitecture(config, writers=writers)
        self.notifier = create_notification_service(config.get("alerting", {}))

        log.info("KappaPipeline: initialized with %d readers, %d writers",
                 len(readers), len(writers))

    def _reader_source_label(self) -> str:
        return ", ".join(r.__class__.__name__ for r in self.readers) or "unknown"

    def _writer_label(self) -> str:
        return ", ".join(w.__class__.__name__ for w in self.writers) or "none"

    def _split_readers(self):
        streaming = [r for r in self.readers if "Streaming" in r.__class__.__name__]
        batch = [r for r in self.readers if r not in streaming]
        return batch, streaming

    def execute(self) -> Dict[str, Any]:
        start_time = time.time()
        try:
            batch_readers, streaming_readers = self._split_readers()
            log.info("KappaPipeline: %d batch reader(s), %d streaming reader(s)",
                     len(batch_readers), len(streaming_readers))

            if not batch_readers and not streaming_readers:
                log.warning("KappaPipeline: no readers configured")
                return {"status": "warning", "records": 0}

            processed = self.architecture.execute(batch_readers, streaming_readers)

            self.notifier.notify(
                "ingestion_success",
                source=self._reader_source_label(),
                architecture="kappa",
                writer=self._writer_label(),
                record_count=len(processed),
                duration_seconds=round(time.time() - start_time, 2),
            )
            check_alert_rules(
                self.config, self.notifier,
                source=self._reader_source_label(),
                start_time=start_time,
                total_records=len(processed),
                failed_records=0,
            )

            return {"status": "success", "records": len(processed)}

        except Exception as exc:
            log.error("KappaPipeline: execution failed — %s", exc, exc_info=True)
            self.notifier.notify(
                "ingestion_failure",
                source=self._reader_source_label(),
                architecture="kappa",
                writer=self._writer_label(),
                error=str(exc),
                duration_seconds=round(time.time() - start_time, 2),
            )
            raise
