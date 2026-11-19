import json
import pytest
from unittest.mock import MagicMock, patch

from target.write_s3_json import S3JsonWriter


BASE_CONFIG = {
    "bucket": "test-bucket",
    "output_prefix": "output/",
    "region": "us-east-1",
}


def make_writer(extra=None):
    config = {**BASE_CONFIG, **(extra or {})}
    with patch("boto3.client"):
        writer = S3JsonWriter(config)
        writer._client = MagicMock()
    return writer


# --- Single mode ---

def test_write_single_mode_calls_put_immediately():
    writer = make_writer()
    record = {"source_uri": "s3://bucket/docs/file.pdf", "format": "pdf", "text": "hello"}
    writer.write(record)
    writer._client.put_object.assert_called_once()


def test_write_single_constructs_correct_key():
    writer = make_writer()
    record = {"source_uri": "s3://bucket/docs/report.pdf", "format": "pdf"}
    writer.write(record)
    call_kwargs = writer._client.put_object.call_args[1]
    assert call_kwargs["Key"] == "output/pdf/report.pdf.json"
    assert call_kwargs["Bucket"] == "test-bucket"


def test_write_single_key_uses_format_and_filename():
    writer = make_writer()
    record = {"source_uri": "s3://my-bucket/path/to/image.png", "format": "image"}
    writer.write(record)
    call_kwargs = writer._client.put_object.call_args[1]
    assert call_kwargs["Key"] == "output/image/image.png.json"


def test_write_single_unknown_format_fallback():
    writer = make_writer()
    record = {"source_uri": "s3://bucket/file.bin"}
    writer.write(record)
    call_kwargs = writer._client.put_object.call_args[1]
    assert "/unknown/" in call_kwargs["Key"]


# --- Batch mode ---

def test_write_batch_mode_buffers_records():
    writer = make_writer({"batch_mode": True})
    writer.write({"id": 1})
    writer.write({"id": 2})
    writer._client.put_object.assert_not_called()
    assert len(writer._batch_buffer) == 2


def test_flush_writes_jsonl_and_clears_buffer():
    writer = make_writer({"batch_mode": True})
    writer.write({"id": 1, "text": "a"})
    writer.write({"id": 2, "text": "b"})
    writer.flush()
    writer._client.put_object.assert_called_once()
    call_kwargs = writer._client.put_object.call_args[1]
    body = call_kwargs["Body"].decode("utf-8")
    lines = body.strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1, "text": "a"}
    assert json.loads(lines[1]) == {"id": 2, "text": "b"}
    assert writer._batch_buffer == []


def test_flush_key_contains_batch_prefix():
    writer = make_writer({"batch_mode": True})
    writer.write({"id": 1})
    writer.flush()
    call_kwargs = writer._client.put_object.call_args[1]
    assert call_kwargs["Key"].startswith("output/batch_")
    assert call_kwargs["Key"].endswith(".jsonl")


def test_flush_noop_when_buffer_empty():
    writer = make_writer({"batch_mode": True})
    writer.flush()
    writer._client.put_object.assert_not_called()


def test_flush_clears_buffer_after_write():
    writer = make_writer({"batch_mode": True})
    writer.write({"id": 1})
    writer.flush()
    assert writer._batch_buffer == []


# --- Serialization ---

def test_serialize_normal_record():
    writer = make_writer()
    record = {"key": "value", "num": 42}
    result = writer._serialize(record)
    assert json.loads(result) == record


def test_serialize_non_serializable_uses_default_str():
    writer = make_writer()

    class Custom:
        def __str__(self):
            return "custom_str"

    record = {"obj": Custom()}
    result = writer._serialize(record)
    parsed = json.loads(result)
    assert parsed["obj"] == "custom_str"


def test_serialize_non_serializable_logs_warning(caplog):
    import logging
    writer = make_writer()

    class Custom:
        pass

    record = {"obj": Custom()}
    with caplog.at_level(logging.WARNING, logger="target.write_s3_json"):
        writer._serialize(record)
    assert any("non-serializable" in m for m in caplog.messages)


# --- _put error handling ---

def test_put_reraises_on_s3_failure():
    writer = make_writer()
    writer._client.put_object.side_effect = Exception("S3 error")
    with pytest.raises(Exception, match="S3 error"):
        writer._put("some/key.json", b"{}")


def test_put_logs_error_on_failure(caplog):
    import logging
    writer = make_writer()
    writer._client.put_object.side_effect = Exception("boom")
    with caplog.at_level(logging.ERROR, logger="target.write_s3_json"):
        with pytest.raises(Exception):
            writer._put("some/key.json", b"{}")
    assert any("failed to write" in m for m in caplog.messages)
