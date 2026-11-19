"""
structured_readers/base_reader.py — Abstract contract for all source connectors.
"""
from abc import ABC, abstractmethod
from typing import Generator
import logging

log = logging.getLogger(__name__)


class BaseReader(ABC):
    """
    All readers implement connect / read / close.
    Used as context manager: with reader: for batch in reader.read(): ...
    """
    def __init__(self, config: dict):
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def read(self) -> Generator: ...

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self):
        self.connect(); return self

    def __exit__(self, *_):
        self.close(); return False
