import time
from unittest.mock import MagicMock

import pytest

from architecture.kappa_architecture.pipeline import KappaArchitecture, Pipeline


def make_config(window_seconds=1, replay=False):
    return {
        "kappa": {"window_seconds": window_seconds, "replay": replay},
        "pii_masking": {"enabled": False},
        "alerting": {},
    }


class _FakeBatchReader:
    def __init__(self, records):
        self._records = records
        self.closed = False

    def connect(self):
        pass

    def read(self):
        yield self._records

    def close(self):
        self.closed = True


class _FakeStreamingReader:
    """Mirrors the real streaming readers' shape: one real batch, then
    empty batches forever."""

    def __init__(self, records):
        self._records = records
        self.yielded = False
        self.reset_called_with = None

    def connect(self):
        pass

    def read(self):
        if not self.yielded:
            self.yielded = True
            yield self._records
        while True:
            yield []

    def close(self):
        pass


class _FakeStreamingReaderWithReset(_FakeStreamingReader):
    def reset_offset(self, position):
        self.reset_called_with = position


def make_writer():
    writer = MagicMock()
    writer._buffer = []
    writer.write.side_effect = lambda r: writer._buffer.append(r)
    return writer


# --- Batch reading ---

def test_batch_reader_read_to_exhaustion():
    arch = KappaArchitecture(make_config())
    reader = _FakeBatchReader([{"id": 1}, {"id": 2}])
    raw = arch._read_batch([reader])
    assert raw == [{"id": 1}, {"id": 2}]
    assert reader.closed


# --- Streaming bounded drain (regression: must not hang forever) ---

def test_streaming_drain_is_bounded_by_window():
    arch = KappaArchitecture(make_config(window_seconds=1))
    reader = _FakeStreamingReader([{"id": 1}])
    start = time.monotonic()
    raw = arch._read_streaming([reader])
    elapsed = time.monotonic() - start
    assert raw == [{"id": 1}]
    assert elapsed >= 0.9  # honored the window, didn't cut it short
    assert elapsed < 3.0   # didn't hang past it either


def test_streaming_drain_empty_when_no_readers():
    arch = KappaArchitecture(make_config())
    assert arch._read_streaming([]) == []


# --- Replay ---

def test_replay_calls_reset_offset_when_supported():
    arch = KappaArchitecture(make_config(window_seconds=1, replay=True))
    reader = _FakeStreamingReaderWithReset([{"id": 1}])
    arch._read_streaming([reader])
    assert reader.reset_called_with == "earliest"


def test_replay_logs_warning_when_not_supported(caplog):
    import logging
    arch = KappaArchitecture(make_config(window_seconds=1, replay=True))
    reader = _FakeStreamingReader([{"id": 1}])  # no reset_offset method
    with caplog.at_level(logging.WARNING, logger="architecture.kappa_architecture.pipeline"):
        arch._read_streaming([reader])
    assert any("no reset_offset()" in m for m in caplog.messages)


# --- Processing: one path, real dedup/null/PII ---

def test_process_collapses_duplicate_keys_last_wins():
    arch = KappaArchitecture(make_config())
    raw = [{"employee_id": "E1", "name": "old"}, {"employee_id": "E1", "name": "new"}]
    result = arch._process(raw)
    assert len(result) == 1
    assert result[0]["name"] == "new"


def test_process_tags_records_with_kappa_metadata():
    arch = KappaArchitecture(make_config())
    result = arch._process([{"employee_id": "E1"}])
    assert result[0]["_kappa_layer"] == "stream"
    assert "_kappa_process_time" in result[0]
    assert result[0]["_kappa_pii_masked"] is False


def test_process_keeps_unkeyed_records():
    arch = KappaArchitecture(make_config())
    result = arch._process([{"no_id_field": "x"}])
    assert len(result) == 1
    assert "_kappa_record_index" in result[0]


# --- Writing ---

def test_write_output_calls_write_then_flush():
    arch = KappaArchitecture(make_config())
    writer = make_writer()
    arch.writers = [writer]
    arch._write_output([{"id": 1}, {"id": 2}])
    assert writer.write.call_count == 2
    writer.flush.assert_called_once()


def test_write_output_noop_with_no_writers():
    arch = KappaArchitecture(make_config())
    arch.writers = []
    arch._write_output([{"id": 1}])  # should not raise


# --- End-to-end execute() ---

def test_execute_batch_only():
    arch = KappaArchitecture(make_config())
    writer = make_writer()
    arch.writers = [writer]
    reader = _FakeBatchReader([{"employee_id": "E1"}, {"employee_id": "E2"}])
    result = arch.execute([reader], [])
    assert len(result) == 2
    assert writer.flush.called


def test_pipeline_no_readers_returns_warning():
    pipeline = Pipeline(make_config(), readers=[], writers=[])
    result = pipeline.execute()
    assert result == {"status": "warning", "records": 0}


def test_pipeline_splits_batch_and_streaming_readers():
    reader1 = _FakeBatchReader([{"id": 1}])
    reader1.__class__.__name__ = "FakeBatchReader"
    pipeline = Pipeline(make_config(window_seconds=1), readers=[reader1], writers=[])
    batch, streaming = pipeline._split_readers()
    assert batch == [reader1]
    assert streaming == []
