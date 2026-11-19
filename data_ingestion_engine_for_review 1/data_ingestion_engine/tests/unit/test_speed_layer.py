import time
from unittest.mock import MagicMock

import pytest

from architecture.lambda_architecture.speed_layer import SpeedLayer


def make_config(window_seconds=2):
    return {"lambda": {"speed_layer_window_seconds": window_seconds},
            "pii_masking": {"enabled": False}}


class _FastReader:
    """A streaming reader whose read() yields one batch immediately, then
    keeps yielding empty batches forever — mirrors the real streaming
    readers' `while True:` shape without any real I/O."""

    def __init__(self, records):
        self._records = records
        self._connected = False
        self.yielded_once = False

    def connect(self):
        self._connected = True

    def read(self):
        if not self.yielded_once:
            self.yielded_once = True
            yield self._records
        while True:
            yield []

    def close(self):
        self._connected = False


def test_stop_and_join_waits_out_the_full_window_when_called_early():
    """
    Regression test for a real bug: stop_and_join() used to set the stop
    event immediately regardless of how much of the configured window had
    actually elapsed. When batch processing finishes fast (or there are no
    batch readers at all — a normal, documented configuration), this cut
    the streaming drain short after whatever the first reader.read() yield
    produced, instead of running for the full speed_layer_window_seconds.
    """
    window = 2
    layer = SpeedLayer(make_config(window_seconds=window))
    reader = _FastReader([{"id": 1}])

    start = time.monotonic()
    layer.start([reader])
    layer.stop_and_join()  # called immediately, like a 0-duration batch layer would
    elapsed = time.monotonic() - start

    assert elapsed >= window - 0.2, (
        f"stop_and_join() returned after {elapsed:.2f}s, before the configured "
        f"{window}s window elapsed — the drain was cut short"
    )
    assert len(layer.snapshot()) == 1


def test_stop_and_join_does_not_oversleep_when_window_already_elapsed():
    """If the caller reaches stop_and_join() AFTER the window has already
    elapsed (slow batch processing), it must return promptly — not sleep
    for the window a second time."""
    window = 1
    layer = SpeedLayer(make_config(window_seconds=window))
    reader = _FastReader([{"id": 1}])

    layer.start([reader])
    time.sleep(window + 0.3)  # simulate slow batch processing past the deadline

    start = time.monotonic()
    layer.stop_and_join()
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, f"stop_and_join() took {elapsed:.2f}s after the window already elapsed"


def test_stop_and_join_noop_when_no_streaming_readers():
    """start([]) (no streaming readers) must not make stop_and_join() hang
    trying to read an unset deadline."""
    layer = SpeedLayer(make_config(window_seconds=5))
    layer.start([])

    start = time.monotonic()
    layer.stop_and_join()
    elapsed = time.monotonic() - start

    assert elapsed < 1.0
    assert layer.snapshot() == []


def test_drain_still_stops_promptly_on_stop_event_before_deadline():
    """A reader that keeps producing records shouldn't be cut off until the
    window elapses — confirms the wait doesn't just always sleep the full
    window regardless of reader behavior."""
    window = 3
    layer = SpeedLayer(make_config(window_seconds=window))
    reader = _FastReader([{"id": 1}, {"id": 2}])

    start = time.monotonic()
    layer.start([reader])
    layer.stop_and_join()
    elapsed = time.monotonic() - start

    assert elapsed >= window - 0.2
    assert elapsed < window + 2  # thread.join timeout headroom, not an extra full window
