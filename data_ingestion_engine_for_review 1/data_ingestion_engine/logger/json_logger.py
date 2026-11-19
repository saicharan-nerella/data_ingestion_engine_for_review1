"""
logger/json_logger.py
──────────────────────
Structured JSON logging — one JSON object per line, replacing the root
logger's handler(s) rather than adding to them.

This is the highest-value, lowest-cost logging enhancement available: a
Glue Python-shell job already ships everything written to stdout/stderr to
CloudWatch Logs automatically (/aws-glue/python-jobs/output and .../error),
with no extra IAM permission, dependency, or log group needed. Formatting
those same lines as JSON is what makes them queryable in CloudWatch Logs
Insights (filter/aggregate by level, logger name, etc.) instead of being
plain text that only supports substring search.

Config: level, include_fields (optional list to add from the LogRecord,
e.g. ["pathname", "lineno"])
"""
import json
import logging
import traceback

from logger.base_logger import BaseLogger

log = logging.getLogger(__name__)


class _JSONFormatter(logging.Formatter):
    def __init__(self, include_fields=None):
        super().__init__()
        self.include_fields = include_fields or []

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = "".join(traceback.format_exception(*record.exc_info))
        for field in self.include_fields:
            if hasattr(record, field):
                payload[field] = getattr(record, field)
        return json.dumps(payload, default=str)


class JSONLogger(BaseLogger):
    def configure(self):
        root = logging.getLogger()
        root.handlers.clear()

        handler = logging.StreamHandler()
        handler.setFormatter(_JSONFormatter(self.config.get("include_fields")))
        root.addHandler(handler)
        root.setLevel(self.level_int)

        log.info("JSONLogger: root logger now emitting one JSON object per line (level=%s)", self.level)
