"""
unstructured_readers/base_unstructured_reader.py — Abstract base class for all ReaderPlugins.

Extends BaseReader from structured_readers/base_reader.py and adds the extract() method
that all format-specific ReaderPlugins must implement.
"""
from abc import abstractmethod
from datetime import datetime, timezone
import logging

from structured_readers.base_reader import BaseReader

log = logging.getLogger(__name__)


class BaseUnstructuredReader(BaseReader):
    """
    Abstract contract for all format-specific ReaderPlugins.

    Subclasses implement extract() only — connect/read/close are no-ops
    since unstructured readers operate on in-memory bytes, not streams.

    Each ReaderPlugin lives in its own file under unstructured_readers/ using the
    naming convention <format>_reader.py (e.g., pdf_reader.py, image_reader.py)
    and is registered in core/registry.py under the "unstructured_reader_plugin" layer.

    ReaderPlugins are instantiated and invoked exclusively by the UnifiedParser,
    never directly by the pipeline.
    """

    def connect(self) -> None:
        """No-op: unstructured readers operate on in-memory bytes."""
        pass

    def read(self):
        """No-op: unstructured readers operate on in-memory bytes."""
        pass

    def close(self) -> None:
        """No-op: unstructured readers operate on in-memory bytes."""
        pass

    @abstractmethod
    def extract(self, object_key: str, content: bytes) -> dict:
        """
        Extract content from raw bytes and return an ExtractionRecord.

        Args:
            object_key: The S3 object key (used as source_uri and for logging)
            content:    Raw bytes of the file

        Returns:
            ExtractionRecord dict with keys:
              source_uri   (str)   full S3 URI
              format       (str)   detected format string
              extracted_at (str)   ISO-8601 UTC timestamp
              content      (dict)  {"blocks": [ContentBlock, ...]}
              error        (str)   present only on failure; content is null

            Example success:
            {
                "source_uri": "s3://bucket/path/file.pdf",
                "format": "unstructured_pdf",
                "extracted_at": "2024-01-15T10:30:45.123456+00:00",
                "content": {
                    "blocks": [
                        {"type": "text", "content": "Hello world", "page": 1},
                        {"type": "heading", "content": "Section 1", "level": 1},
                        ...
                    ]
                }
            }

            Example error:
            {
                "source_uri": "s3://bucket/path/file.pdf",
                "format": "unstructured_pdf",
                "extracted_at": "2024-01-15T10:30:45.123456+00:00",
                "content": null,
                "error": "PDF parsing failed: invalid PDF header"
            }
        """
        ...

    def _make_error_record(
        self, object_key: str, format_str: str, error: Exception
    ) -> dict:
        """
        Build a standardized error ExtractionRecord.

        Args:
            object_key:  The S3 object key
            format_str:  The detected format string
            error:       The exception that was raised

        Returns:
            ExtractionRecord dict with content=null and error message
        """
        log.error(
            "[%s] extraction failed for %s: %s",
            self.__class__.__name__,
            object_key,
            error,
        )
        return {
            "source_uri": f"s3://{object_key}",
            "format": format_str,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "content": None,
            "error": str(error),
        }

    def _make_record(self, object_key: str, format_str: str, blocks: list) -> dict:
        """
        Build a successful ExtractionRecord.

        Args:
            object_key:  The S3 object key
            format_str:  The detected format string
            blocks:      List of ContentBlock dicts

        Returns:
            ExtractionRecord dict with content.blocks populated
        """
        return {
            "source_uri": f"s3://{object_key}",
            "format": format_str,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "content": {"blocks": blocks},
        }
