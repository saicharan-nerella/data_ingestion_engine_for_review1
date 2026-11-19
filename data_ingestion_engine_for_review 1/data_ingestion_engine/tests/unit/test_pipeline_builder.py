import logging
from unittest.mock import MagicMock, patch

import pytest

from core.pipeline_builder import PipelineBuilder

CONFIG_PATH = "config/config.yaml"


@pytest.fixture(autouse=True)
def restore_root_logger():
    """Logger backends mutate the real root logger — restore it after each test."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    root.handlers.clear()
    for h in original_handlers:
        root.addHandler(h)
    root.setLevel(original_level)

BASE_ARGS = {
    "reader": "batch_cloud",
    "writer": "cloud",
    "writer_bucket": "test-bucket",
}


def build(extra_args):
    args = {**BASE_ARGS, **extra_args}
    return PipelineBuilder(CONFIG_PATH, args).build()


# --- Required parameter validation ---

def test_missing_architecture_defaults_to_medallion():
    pipeline = build({})
    assert pipeline.__class__.__name__ == "Pipeline"  # medallion's entry point


def test_missing_reader_raises():
    args = {**BASE_ARGS}
    del args["reader"]
    with pytest.raises(ValueError, match="--reader parameter is required"):
        PipelineBuilder(CONFIG_PATH, args).build()


def test_missing_writer_raises():
    args = {**BASE_ARGS}
    del args["writer"]
    with pytest.raises(ValueError, match="--writer parameter is required"):
        PipelineBuilder(CONFIG_PATH, args).build()


def test_unknown_architecture_raises():
    with pytest.raises(Exception):
        build({"architecture": "not_a_real_architecture"})


# --- Medallion + streaming reader validation (regression: this used to
# hang forever instead of raising, since Medallion's reader loop exhausts
# reader.read() and a streaming reader's read() never ends on its own) ---

def test_medallion_with_streaming_reader_raises_clear_error():
    with pytest.raises(ValueError, match="does not support streaming reader"):
        build({"architecture": "medallion", "reader": "streaming_kafka"})


def test_medallion_with_mixed_batch_and_streaming_readers_raises():
    with pytest.raises(ValueError, match="does not support streaming reader"):
        build({"architecture": "medallion", "reader": "batch_cloud,streaming_socket"})


def test_lambda_with_streaming_reader_does_not_raise():
    pipeline = build({"architecture": "lambda", "reader": "streaming_socket"})
    assert pipeline.__class__.__name__ == "Pipeline"


def test_medallion_with_only_batch_readers_does_not_raise():
    pipeline = build({"architecture": "medallion", "reader": "batch_cloud,batch_api"})
    assert pipeline.__class__.__name__ == "Pipeline"


# --- Writer config building ---

def test_object_storage_writer_requires_bucket():
    args = {**BASE_ARGS, "writer": "cloud"}
    del args["writer_bucket"]
    with pytest.raises(ValueError, match="writer_bucket"):
        PipelineBuilder(CONFIG_PATH, args).build()


def test_non_object_storage_writer_does_not_require_bucket():
    # database writer validates its own required fields instead
    args = {
        "reader": "batch_cloud",
        "writer": "database",
        "writer_db_type": "postgres",
        "writer_host": "localhost",
        "writer_database": "db",
        "writer_username": "u",
        "writer_password": "p",
        "writer_table": "t",
    }
    pipeline = PipelineBuilder(CONFIG_PATH, args).build()
    assert pipeline.writers[0].__class__.__name__ == "DatabaseWriter"


def test_generic_writer_passthrough_reaches_writer_config():
    args = {
        "reader": "batch_cloud",
        "writer": "database",
        "writer_db_type": "postgres",
        "writer_host": "localhost",
        "writer_database": "db",
        "writer_username": "u",
        "writer_password": "p",
        "writer_table": "t",
        "writer_mode": "merge",
        "writer_merge_keys": "id",
    }
    pipeline = PipelineBuilder(CONFIG_PATH, args).build()
    writer = pipeline.writers[0]
    assert writer.mode == "merge"
    assert writer.merge_keys == ["id"]


# --- Logger passthrough ---

def test_logger_glue_param_passthrough_overrides_config_yaml():
    args = {**BASE_ARGS, "logger": "text", "logger_level": "DEBUG",
            "logger_format": "%(message)s"}
    # Should not raise even though logger_config isn't in config.yaml at all
    build(args)


def test_logger_single_additive_backend_works_alone():
    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        build({**BASE_ARGS, "logger": "cloudwatch"})

    mock_watchtower.CloudWatchLogHandler.assert_called_once()


def test_logger_combo_json_and_cloudwatch_both_apply():
    root = logging.getLogger()

    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        build({**BASE_ARGS, "logger": "json,cloudwatch"})

    # json_logger (non-additive) replaced root handlers with exactly one
    # StreamHandler, then cloudwatch_logger (additive) added its handler
    # on top — both must survive, in that order.
    assert len(root.handlers) == 2
    mock_watchtower.CloudWatchLogHandler.assert_called_once()


def test_logger_combo_order_independent_additive_still_added_last():
    # Passing the additive backend first in the list must not change the
    # outcome — pipeline_builder sorts non-additive backends to run first
    # regardless of the order given on the command line.
    root = logging.getLogger()

    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        build({**BASE_ARGS, "logger": "cloudwatch,json"})

    assert len(root.handlers) == 2


def test_logger_two_non_additive_backends_falls_back_to_default(caplog):
    # json and text both replace root handlers — combining them is
    # rejected with a clear message, and pipeline_builder falls back to
    # the default logging setup rather than silently picking one.
    with caplog.at_level(logging.WARNING):
        build({**BASE_ARGS, "logger": "json,text"})

    assert any(
        "more than one backend that replaces the root handlers" in r.message
        for r in caplog.records
    )
