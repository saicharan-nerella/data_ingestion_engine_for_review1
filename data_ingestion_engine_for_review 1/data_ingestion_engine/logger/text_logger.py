"""
logger/text_logger.py
───────────────────────
Human-readable text logging — makes the format main/aimatic.py already
applies by default (via logging.basicConfig at import time) an explicit,
intentional, user-configurable backend instead of an implicit side effect.
Selecting --logger text with no --logger_format override changes nothing
visible; the point is making the choice real and overridable.

Config: level, format (default matches aimatic.py's existing default)
"""
import logging

from logger.base_logger import BaseLogger

log = logging.getLogger(__name__)

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


class TextLogger(BaseLogger):
    def configure(self):
        fmt = self.config.get("format", _DEFAULT_FORMAT)

        root = logging.getLogger()
        root.handlers.clear()

        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(fmt))
        root.addHandler(handler)
        root.setLevel(self.level_int)

        log.info("TextLogger: root logger configured (level=%s, format=%r)", self.level, fmt)
