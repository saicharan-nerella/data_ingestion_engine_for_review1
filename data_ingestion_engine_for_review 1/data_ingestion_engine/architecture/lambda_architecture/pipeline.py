"""
architecture/lambda_architecture/pipeline.py
──────────────────────────────────────────────────
Lambda Architecture: Batch Layer (accuracy) + Speed Layer (low latency) + Serving Layer (merge)

Responsibility:
- Orchestrates Batch, Speed, and Serving layers
- Batch layer reads everything from the batch readers and recomputes the
  authoritative view; Speed layer drains the streaming readers concurrently
  for a bounded window
- Serving layer merges the two, preferring the Speed layer on overlapping keys
- Each layer writes its own output to target storage — same convention as
  Medallion's Bronze/Silver/Gold, so the UI's writer/target parameters
  (bucket, prefix, format, provider, etc.) work identically regardless of
  which architecture the user picked

Wired into the UI/CLI exactly like every other architecture:
    --architecture lambda --reader <readers> --writer <writer> --writer_bucket ... --writer_prefix ...
core/pipeline_builder.py resolves "lambda" via core/registry.py to the
`Pipeline` class below and instantiates it with
config/readers/writers/orchestrator/alert_handlers — the user never
chooses Batch vs. Speed directly, that split happens automatically from
which reader(s) they picked:
    --reader batch_database                    -> batch view only (speed view empty)
    --reader streaming_kafka                   -> speed view only (batch view empty)
    --reader batch_database,streaming_kafka    -> both views, merged by Serving layer

When to use:
  - Need BOTH historical accuracy AND real-time updates in the same query
  - E.g. fraud scoring: 90 days of history + the last few seconds of events
  - Acceptable trade-off: complexity of two code paths

Trade-offs:
  PRO: batch self-corrects any speed-layer errors on its next full recompute
  CON: duplicate transform logic; higher infra cost; the speed layer's
       window is a tunable guess, not a guarantee every recent event was
       captured

Data Flow:
    batch readers     -> Batch Layer  (full recompute)        -> Write to Batch Target
    streaming readers -> Speed Layer (bounded incremental)    -> Write to Speed Target
                              |                  |
                        Serving Layer (merge, speed wins on overlap)
                                       -> Write to Serving Target
"""

import logging
import time
from typing import Any, Dict, List

from architecture.lambda_architecture.batch_layer import BatchLayer
from architecture.lambda_architecture.speed_layer import SpeedLayer
from architecture.lambda_architecture.serving_layer import ServingLayer
from alerts.channel_senders import create_notification_service
from core.alert_rules import check_alert_rules

log = logging.getLogger(__name__)


class LambdaArchitecture:
    """
    Lambda Architecture: dual-path processing merged into one serving view.

    Orchestrates three layers:
    1. Batch: full recompute from all raw records -> write to batch target
    2. Speed: bounded incremental drain of streaming readers -> write to speed target
    3. Serving: merge batch + speed (speed wins on overlap) -> write to serving target

    Each layer is self-contained and writes its own output, mirroring
    MedallionArchitecture's per-layer output convention.
    """

    def __init__(self, config: dict, writers: List = None):
        """
        Initialize the Lambda architecture.

        Args:
            config: Full configuration from config.yaml
            writers: List of writer instances (built by PipelineBuilder from
                the user's --writer/--writer_* parameters)
        """
        self.config = config
        self.writers = writers or []
        self.batch_layer = BatchLayer(config)
        self.speed_layer = SpeedLayer(config)
        self.serving_layer = ServingLayer(config)

        # Force batch mode on all writers so records are buffered and
        # written as a single file per layer (same as Medallion).
        for writer in self.writers:
            writer._batch_mode = True

        log.info("LambdaArchitecture: initialized with %d writer(s)", len(self.writers))

    def execute(self, batch_readers: List,
                streaming_readers: List = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute the full Lambda architecture with separate layer outputs.

        The Speed layer is started FIRST, before the batch readers are even
        connected — that's what makes this genuinely concurrent. Batch I/O
        (a DB query, a paginated API pull, S3 downloads) is normally the
        slow part of a run, so it needs to happen *inside* the Speed
        layer's drain window, not before it starts.

        Args:
            batch_readers: Reader instances to read synchronously for the
                Batch layer (may be empty)
            streaming_readers: Streaming reader instances to drain
                concurrently on background threads for the Speed layer
                (may be empty)

        Returns:
            Dictionary with outputs from each layer:
            {
                "batch": [batch_records],
                "speed": [speed_records],
                "serving": [serving_records]
            }
        """
        try:
            batch_readers = batch_readers or []
            streaming_readers = streaming_readers or []
            log.info("LambdaArchitecture: executing with %d batch reader(s), %d streaming reader(s)",
                      len(batch_readers), len(streaming_readers))

            # ── SPEED LAYER: start draining in the background FIRST ───────────
            # Everything below — including the batch readers' I/O — now runs
            # concurrently with the streaming drain, not after it.
            log.info("LambdaArchitecture: STEP 1 - starting Speed layer")
            self.speed_layer.start(streaming_readers)

            # ── BATCH LAYER: read + full recompute, overlapping the drain ─────
            log.info("LambdaArchitecture: STEP 2 - Batch layer (read + full recompute)")
            raw_records = self._collect_batch_records(batch_readers)
            batch_records = self.batch_layer.process(raw_records)
            log.info("  -> %d records", len(batch_records))
            self._write_layer_output("batch", batch_records)

            # ── Close out the Speed layer's window ─────────────────────────────
            self.speed_layer.stop_and_join()
            speed_records = self.speed_layer.snapshot()
            log.info("LambdaArchitecture: Speed layer produced %d record(s)", len(speed_records))
            self._write_layer_output("speed", speed_records)

            # ── SERVING LAYER: merge ────────────────────────────────────────────
            log.info("LambdaArchitecture: STEP 3 - Serving layer (merge)")
            serving_records = self.serving_layer.merge(batch_records, speed_records)
            log.info("  -> %d records", len(serving_records))
            self._write_layer_output("serving", serving_records)

            log.info("LambdaArchitecture: completed successfully")

            return {
                "batch": batch_records,
                "speed": speed_records,
                "serving": serving_records,
            }

        except Exception as exc:
            log.error("LambdaArchitecture: execution failed — %s", exc, exc_info=True)
            raise

    def _collect_batch_records(self, batch_readers: List) -> List[Dict[str, Any]]:
        """Read every batch reader to exhaustion — reader.read() yields List[Dict] batches."""
        raw_records = []
        for reader in batch_readers:
            try:
                reader.connect()
                for batch in reader.read():
                    if isinstance(batch, list):
                        raw_records.extend(batch)
                    elif isinstance(batch, dict):
                        raw_records.append(batch)
            finally:
                reader.close()

        log.info("LambdaArchitecture: collected %d raw record(s) from %d batch reader(s)",
                  len(raw_records), len(batch_readers))
        return raw_records

    def _write_layer_output(self, layer_name: str, records: List[Dict[str, Any]]) -> None:
        """
        Write layer output to target storage, routed per layer the same way
        DataVaultArchitecture routes Hub/Link/Satellite: a table-name
        override on flush() for table-based writers (DatabaseWriter/
        DataWarehouseWriter — which otherwise always write to the ONE
        configured table), or prefix reassignment (batch/, speed/,
        serving/) for CloudWriter, which has no table concept at all.

        Serving largely overlaps what Batch/Speed already produced (it's
        their merge) — sending all three into one shared table would both
        crash (Speed/Serving add columns Batch's auto-created table
        doesn't have) and silently double/triple-count every record if it
        didn't crash. Each layer gets its own table/prefix instead.

        Args:
            layer_name: Name of the layer (batch, speed, serving)
            records: Records to write
        """
        if not self.writers:
            log.debug("LambdaArchitecture: no writers configured, skipping %s layer output", layer_name)
            return

        for writer in self.writers:
            try:
                for record in records:
                    if isinstance(record, dict):
                        writer.write(record)
                    else:
                        log.warning("LambdaArchitecture: skipping non-dict record in %s layer: %s",
                                    layer_name, type(record))

                base_table = getattr(writer, "table_name", None)
                if base_table:
                    writer.flush(table_name=f"{base_table}_{layer_name}")
                else:
                    original_prefix = getattr(writer, "prefix", "") or ""
                    writer.prefix = f"{original_prefix}{layer_name}/"
                    writer.flush()
                    writer.prefix = original_prefix

                log.info("LambdaArchitecture: wrote %d %s record(s) via %s",
                          len(records), layer_name, writer.__class__.__name__)

            except Exception as exc:
                log.error("LambdaArchitecture: writer %s failed for %s layer — %s",
                           writer.__class__.__name__, layer_name, exc)
                raise


class Pipeline:
    """
    Entry point the UI/CLI actually runs when --architecture lambda is
    selected. Resolved by core/registry.py and instantiated by
    core/pipeline_builder.py with the same five arguments every other
    architecture gets — no lambda-specific wiring needed on the UI side.

    Splits the readers the user passed via --reader into batch vs.
    streaming by class name (streaming readers have "Streaming" in their
    class name — see structured_readers/streaming_*.py), collects all
    batch data up front, drains streaming readers concurrently for the
    configured window, then hands both to LambdaArchitecture to merge and
    write through whichever --writer the user chose.
    """

    def __init__(self, config: dict, readers: List, writers: List,
                 orchestrator=None, alert_handlers: List = None):
        """
        Initialize the Lambda pipeline.

        Args:
            config: Full configuration from config.yaml
            readers: Reader instances built from --reader (batch and/or streaming)
            writers: Writer instances built from --writer/--writer_*
            orchestrator: Orchestrator instance (optional, from --orchestrator)
            alert_handlers: Alert handler instances (optional, from --alerts)
        """
        self.config = config
        self.readers = readers
        self.writers = writers
        self.orchestrator = orchestrator
        self.alert_handlers = alert_handlers or []
        self.architecture = LambdaArchitecture(config, writers=writers)

        # Real, working notification path — driven entirely by config.yaml's
        # `alerting` section, same as Medallion (see
        # architecture/medallion_architecture/pipeline.py for why this is
        # separate from self.alert_handlers above).
        self.notifier = create_notification_service(config.get("alerting", {}))

        log.info("LambdaPipeline: initialized with %d readers, %d writers",
                  len(readers), len(writers))

    def _reader_source_label(self) -> str:
        """Best-effort, human-readable label for notification messages."""
        return ", ".join(r.__class__.__name__ for r in self.readers) or "unknown"

    def _writer_label(self) -> str:
        """Best-effort, human-readable label for notification messages."""
        return ", ".join(w.__class__.__name__ for w in self.writers) or "none"

    def _split_readers(self):
        """Streaming readers feed the Speed layer; everything else feeds Batch."""
        streaming = [r for r in self.readers if "Streaming" in r.__class__.__name__]
        batch = [r for r in self.readers if r not in streaming]
        return batch, streaming

    def execute(self) -> Dict[str, Any]:
        """
        Execute the full Lambda pipeline.

        Returns:
            Dictionary with execution results and layer record counts
        """
        start_time = time.time()
        try:
            batch_readers, streaming_readers = self._split_readers()
            log.info("LambdaPipeline: %d batch reader(s), %d streaming reader(s)",
                      len(batch_readers), len(streaming_readers))

            if not batch_readers and not streaming_readers:
                log.warning("LambdaPipeline: no readers configured")
                return {"status": "warning", "records": 0}

            # LambdaArchitecture owns reading now — it starts the Speed
            # layer's background drain BEFORE connecting the batch readers,
            # so the batch readers' I/O genuinely overlaps the streaming
            # drain instead of running before it starts.
            layer_outputs = self.architecture.execute(batch_readers, streaming_readers)

            self.notifier.notify(
                "ingestion_success",
                source=self._reader_source_label(),
                architecture="lambda",
                writer=self._writer_label(),
                record_count=len(layer_outputs["serving"]),
                duration_seconds=round(time.time() - start_time, 2),
            )

            # alerting.rules — error rate / run duration / throughput checks.
            # failed_records is always 0 here: unlike Medallion's Silver
            # layer, Batch/Speed don't classify any record as failed today
            # (they tag and pass everything through) — this is an honest
            # 0%, not a claim that nothing can go wrong.
            check_alert_rules(
                self.config, self.notifier,
                source=self._reader_source_label(),
                start_time=start_time,
                total_records=len(layer_outputs["serving"]),
                failed_records=0,
            )

            return {
                "status": "success",
                "batch_records": len(layer_outputs["batch"]),
                "speed_records": len(layer_outputs["speed"]),
                "serving_records": len(layer_outputs["serving"]),
            }

        except Exception as exc:
            log.error("LambdaPipeline: execution failed — %s", exc, exc_info=True)
            self.notifier.notify(
                "ingestion_failure",
                source=self._reader_source_label(),
                architecture="lambda",
                writer=self._writer_label(),
                error=str(exc),
                duration_seconds=round(time.time() - start_time, 2),
            )
            raise
