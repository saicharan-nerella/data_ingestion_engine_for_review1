"""
logger/base_logger.py — Contract for all logger backends.
"""
import logging
from abc import ABC, abstractmethod


class BaseLogger(ABC):
    # False (default): configure() replaces the root logger's handlers
    # (json/text — a log line is formatted one way or another, not both).
    # True: configure() only adds a handler, leaving existing ones alone
    # (cloudwatch — a second destination alongside whatever's already
    # there). --logger accepts a comma-separated list (core/pipeline_builder.py)
    # of exactly one of the former plus any number of the latter — this
    # flag is how that gets validated and ordered correctly (every
    # non-additive backend must run before any additive one, or the
    # additive one's handler would just get wiped out by the next clear()).
    ADDITIVE = False

    def __init__(self, config: dict):
        self.config = config
        self.level = config.get("level", "INFO")

    @property
    def level_int(self) -> int:
        """self.level ('INFO', 'DEBUG', ...) as the int logging expects."""
        resolved = logging.getLevelName(str(self.level).upper())
        return resolved if isinstance(resolved, int) else logging.INFO

    @abstractmethod
    def configure(self) -> None:
        """Apply logging configuration to the Python logging system."""
