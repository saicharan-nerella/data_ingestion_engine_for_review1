"""
architecture/lambda_architecture/speed_layer.py
──────────────────────────────────────────────────
Speed Layer: Incremental View Over New Streaming Events

Responsibility:
- Drain the streaming readers (anything with "Streaming" in its class
  name — Kafka/PubSub/Kinesis/EventHub/socket/webhook) for a bounded
  window, one background thread per reader so multiple streams are
  actually drained concurrently
- Tag each event the same way the Batch layer tags records, so keys line
  up for the Serving layer's merge
- Normalize blanks to real None and mask PII (optional, config-driven) —
  same core/null_handling.py and core/pii_masking.py the Batch layer uses,
  so both views are cleaned identically
- Keep only the latest event per key — this is a rolling, most-recent-wins
  view, not a full history. Key detection (core/record_identity.py)
  recognizes both "employee_id" and "EmployeeID" style columns.

Streaming readers' read() generators never end on their own (they yield
[] between events and keep polling forever) — that's fine for a
long-running stream processor, but this pipeline runs to completion, so
draining is bounded by `lambda.speed_layer_window_seconds` (config key —
set from the --speed_layer_window_seconds Glue parameter by
core/pipeline_builder.py, not read from config.yaml) rather than run
until the generator stops.

This view is intentionally approximate. It gets corrected the next time
the Batch layer runs its full recompute — that reconciliation is the
entire point of pairing the two layers.

Data Flow:
    streaming events (last N seconds) → Speed Layer (tag + latest-wins) → speed_view
"""

import logging
import threading
import time
from datetime import datetime
from typing import Any, Dict, List

from architecture.lambda_architecture.record_keys import extract_source, get_record_key
from core.null_handling import normalize_nulls
from core.pii_masking import PIIMasker

log = logging.getLogger(__name__)


class SpeedLayer:
    """
    Speed layer: incremental, low-latency view built by draining streaming
    readers for a bounded window, each on its own background thread.
    """

    def __init__(self, config: dict):
        self.config = config
        lambda_cfg = config.get("lambda", {})
        self.window_seconds = lambda_cfg.get("speed_layer_window_seconds", 30)

        # Shared masking implementation — see core/pii_masking.py. PIIMasker
        # only reads its own config-derived attributes in mask(), never
        # mutates shared state, so it's safe to call from the concurrent
        # drain threads below without its own lock.
        self.pii_masker = PIIMasker(config)

        self._lock = threading.Lock()
        self._speed_view: Dict[Any, Dict[str, Any]] = {}
        self._unkeyed: List[Dict[str, Any]] = []
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._deadline = None

        log.info("SpeedLayer: initialized (window=%ss)", self.window_seconds)

    def start(self, streaming_readers: List) -> None:
        """Start draining each streaming reader on its own background thread."""
        self._deadline = None
        if not streaming_readers:
            log.info("SpeedLayer: no streaming readers — speed view stays empty")
            return

        self._stop_event.clear()
        self._deadline = time.monotonic() + self.window_seconds
        self._threads = [
            threading.Thread(target=self._drain_one, args=(reader, self._deadline), daemon=True)
            for reader in streaming_readers
        ]
        for thread in self._threads:
            thread.start()

        log.info("SpeedLayer: draining %d streaming reader(s) for %ss",
                  len(streaming_readers), self.window_seconds)

    def stop_and_join(self) -> None:
        """
        Signal every drain thread to stop and wait for them to exit.

        Waits out the rest of the configured window first if the caller
        gets here early — e.g. batch processing finished fast, or there
        were no batch readers at all, which is a normal, supported
        configuration (--reader streaming_kafka alone). Without this, the
        drain would be cut short by however long batch processing happened
        to take instead of running for speed_layer_window_seconds like the
        config says, silently losing however much of the window never
        actually elapsed.
        """
        if self._threads and self._deadline is not None:
            remaining = self._deadline - time.monotonic()
            if remaining > 0:
                log.info("SpeedLayer: batch layer finished early — waiting %.1fs more "
                          "to honor the full %ss drain window", remaining, self.window_seconds)
                time.sleep(remaining)

        self._stop_event.set()
        for thread in self._threads:
            thread.join(timeout=self.window_seconds + 5)

        with self._lock:
            keyed, unkeyed = len(self._speed_view), len(self._unkeyed)
        log.info("SpeedLayer: stopped (view has %d keyed + %d unkeyed record(s))", keyed, unkeyed)

    def snapshot(self) -> List[Dict[str, Any]]:
        """Return the current speed view without disturbing it."""
        with self._lock:
            return list(self._speed_view.values()) + list(self._unkeyed)

    def _drain_one(self, reader, deadline: float) -> None:
        try:
            reader.connect()
        except Exception as exc:
            log.warning("SpeedLayer: reader %s failed to connect — %s",
                        reader.__class__.__name__, exc)
            return

        try:
            for events in reader.read():
                if self._stop_event.is_set() or time.monotonic() >= deadline:
                    break
                if events:
                    self._ingest(events)
                else:
                    time.sleep(0.1)  # avoid busy-looping while the stream is idle
        except Exception as exc:
            log.error("SpeedLayer: reader %s failed while draining — %s",
                       reader.__class__.__name__, exc, exc_info=True)
        finally:
            reader.close()

    def _ingest(self, events: List[Dict[str, Any]]) -> None:
        event_time = datetime.utcnow().isoformat()
        with self._lock:
            for event in events:
                source = extract_source(event)
                tagged = {
                    "_lambda_layer": "speed",
                    "_speed_event_time": event_time,
                    "_speed_source": source,
                    **event,
                }
                tagged = normalize_nulls(tagged)
                tagged = self.pii_masker.mask(tagged)
                tagged["_speed_pii_masked"] = self.pii_masker.enabled

                key = get_record_key(tagged, source)
                if key is None:
                    self._unkeyed.append(tagged)
                else:
                    self._speed_view[key] = tagged