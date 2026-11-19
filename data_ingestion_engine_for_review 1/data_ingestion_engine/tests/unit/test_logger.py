import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from logger.base_logger import BaseLogger
from logger.json_logger import JSONLogger
from logger.text_logger import TextLogger
from logger.cloudwatch_logger import CloudWatchLogger


@pytest.fixture(autouse=True)
def restore_root_logger():
    """Every backend mutates the real root logger — restore it after each test."""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    yield
    root.handlers.clear()
    for h in original_handlers:
        root.addHandler(h)
    root.setLevel(original_level)


# --- BaseLogger.level_int ---

def test_level_int_parses_known_level():
    class Dummy(BaseLogger):
        def configure(self): pass
    assert Dummy({"level": "DEBUG"}).level_int == logging.DEBUG
    assert Dummy({"level": "warning"}).level_int == logging.WARNING


def test_level_int_defaults_to_info_for_unknown_level():
    class Dummy(BaseLogger):
        def configure(self): pass
    assert Dummy({"level": "NOT_A_LEVEL"}).level_int == logging.INFO


def test_level_defaults_to_info_when_unset():
    class Dummy(BaseLogger):
        def configure(self): pass
    assert Dummy({}).level == "INFO"


# --- JSONLogger ---

def test_json_logger_replaces_root_handlers():
    root = logging.getLogger()
    root.addHandler(logging.NullHandler())
    before_count = len(root.handlers)

    JSONLogger({"level": "INFO"}).configure()
    assert len(root.handlers) == 1
    assert root.handlers[0] is not None


def test_json_logger_emits_valid_json_with_expected_fields(capsys):
    JSONLogger({"level": "INFO"}).configure()
    test_log = logging.getLogger("test.json")
    test_log.info("hello %s", "world")

    captured = capsys.readouterr()
    line = [l for l in captured.err.strip().split("\n") if l][-1]
    parsed = json.loads(line)
    assert parsed["message"] == "hello world"
    assert parsed["level"] == "INFO"
    assert parsed["logger"] == "test.json"
    assert "timestamp" in parsed


def test_json_logger_includes_exception_info(capsys):
    JSONLogger({"level": "INFO"}).configure()
    test_log = logging.getLogger("test.json.exc")
    try:
        raise ValueError("boom")
    except ValueError:
        test_log.exception("failed")

    captured = capsys.readouterr()
    line = [l for l in captured.err.strip().split("\n") if l][-1]
    parsed = json.loads(line)
    assert "exception" in parsed
    assert "ValueError: boom" in parsed["exception"]


def test_json_logger_sets_root_level():
    JSONLogger({"level": "WARNING"}).configure()
    assert logging.getLogger().level == logging.WARNING


def test_json_logger_includes_extra_fields_when_configured(capsys):
    JSONLogger({"level": "INFO", "include_fields": ["custom_field"]}).configure()
    test_log = logging.getLogger("test.json.extra")
    test_log.info("hi", extra={"custom_field": "abc"})

    captured = capsys.readouterr()
    line = [l for l in captured.err.strip().split("\n") if l][-1]
    parsed = json.loads(line)
    assert parsed["custom_field"] == "abc"


# --- TextLogger ---

def test_text_logger_replaces_root_handlers_with_default_format(capsys):
    TextLogger({"level": "INFO"}).configure()
    test_log = logging.getLogger("test.text")
    test_log.info("hello text")

    captured = capsys.readouterr()
    assert "INFO" in captured.err
    assert "test.text" in captured.err
    assert "hello text" in captured.err


def test_text_logger_respects_custom_format(capsys):
    TextLogger({"level": "INFO", "format": "%(levelname)s:%(message)s"}).configure()
    test_log = logging.getLogger("test.text.custom")
    test_log.info("custom fmt")

    captured = capsys.readouterr()
    assert "INFO:custom fmt" in captured.err


def test_text_logger_sets_root_level():
    TextLogger({"level": "ERROR"}).configure()
    assert logging.getLogger().level == logging.ERROR


# --- CloudWatchLogger ---

def test_cloudwatch_logger_raises_clear_import_error_when_watchtower_missing():
    with patch.dict("sys.modules", {"watchtower": None}):
        with pytest.raises(ImportError, match="watchtower"):
            CloudWatchLogger({"level": "INFO"}).configure()


def test_cloudwatch_logger_adds_handler_without_clearing_existing_ones():
    root = logging.getLogger()
    sentinel_handler = logging.NullHandler()
    root.addHandler(sentinel_handler)

    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        CloudWatchLogger({
            "level": "INFO", "log_group": "/test/group",
            "log_stream": "test-stream", "region": "us-west-2",
        }).configure()

    assert sentinel_handler in root.handlers
    mock_boto3.Session.assert_called_once_with(region_name="us-west-2", profile_name=None)
    mock_watchtower.CloudWatchLogHandler.assert_called_once()
    call_kwargs = mock_watchtower.CloudWatchLogHandler.call_args[1]
    assert call_kwargs["log_group_name"] == "/test/group"
    assert call_kwargs["log_stream_name"] == "test-stream"


def test_cloudwatch_logger_uses_defaults_when_not_configured():
    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        CloudWatchLogger({}).configure()

    call_kwargs = mock_watchtower.CloudWatchLogHandler.call_args[1]
    assert call_kwargs["log_group_name"] == "/aimatic/pipeline"
    # Regression: this used to be a fixed "pipeline" string — two runs
    # writing to the same fixed CloudWatch log stream risks a real
    # InvalidSequenceTokenException, not just mixed-together log lines.
    assert call_kwargs["log_stream_name"].startswith("pipeline-")
    assert call_kwargs["log_stream_name"] != "pipeline"


def test_cloudwatch_logger_log_stream_is_unique_across_instances():
    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        CloudWatchLogger({}).configure()
        stream_1 = mock_watchtower.CloudWatchLogHandler.call_args[1]["log_stream_name"]
        CloudWatchLogger({}).configure()
        stream_2 = mock_watchtower.CloudWatchLogHandler.call_args[1]["log_stream_name"]

    assert stream_1 != stream_2


def test_cloudwatch_logger_respects_explicit_log_stream_override():
    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        CloudWatchLogger({"log_stream": "my-fixed-stream"}).configure()

    call_kwargs = mock_watchtower.CloudWatchLogHandler.call_args[1]
    assert call_kwargs["log_stream_name"] == "my-fixed-stream"


# --- ADDITIVE flag ---

def test_json_and_text_loggers_are_not_additive():
    assert JSONLogger.ADDITIVE is False
    assert TextLogger.ADDITIVE is False


def test_cloudwatch_logger_is_additive():
    assert CloudWatchLogger.ADDITIVE is True


def test_cloudwatch_logger_passes_aws_profile_when_given():
    mock_watchtower = MagicMock()
    mock_watchtower.CloudWatchLogHandler.return_value.level = logging.NOTSET
    mock_boto3 = MagicMock()
    with patch.dict("sys.modules", {"watchtower": mock_watchtower, "boto3": mock_boto3}):
        CloudWatchLogger({"aws_profile": "my-dev-profile"}).configure()

    mock_boto3.Session.assert_called_once_with(region_name="us-east-1", profile_name="my-dev-profile")
