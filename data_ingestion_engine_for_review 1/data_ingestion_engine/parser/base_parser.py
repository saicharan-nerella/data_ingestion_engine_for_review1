"""
parser/base_parser.py — Abstract base class for all content parsers.

Defines the EXTRACTION_SCHEMA contract that all parsers must follow,
and provides helper methods for building ContentBlocks.
"""
from abc import ABC, abstractmethod

# Required fields per block type — shared schema contract
EXTRACTION_SCHEMA: dict[str, set] = {
    "text":     {"content"},
    "heading":  {"content", "level"},
    "table":    {"headers", "rows"},
    "image":    {"data_base64", "mime_type"},
    "list":     {"items", "ordered"},
    "code":     {"content"},
    "metadata": {"properties"},
}


class BaseParser(ABC):
    """
    Abstract base class for pipeline-facing parsers.
    Subclasses implement parse() to route content to format-specific readers.
    
    This class defines the contract for all parsers that extract content
    from raw bytes and return structured ContentBlocks.
    """

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def parse(self, object_key: str, content: bytes) -> list[dict]:
        """
        Parse raw bytes for the given S3 object key and return a list of
        ContentBlock dicts.

        Args:
            object_key: The S3 object key
            content:    Raw bytes of the file

        Returns:
            List of ContentBlock dicts, each containing:
              - type (str): One of the valid Block_Type values
                           (text, heading, table, image, list, code, metadata)
              - <type-specific fields>: Fields required for the block type

            Returns an empty list [] on parsing failure (error handling is
            delegated to the ReaderPlugin wrapper).

            Example:
            [
                {"type": "text", "content": "Hello world", "page": 1},
                {"type": "heading", "content": "Section 1", "level": 1},
                {"type": "metadata", "properties": {"title": "Document"}}
            ]
        """
        ...

    def _make_block(self, block_type: str, **fields) -> dict:
        """
        Build a ContentBlock dict with the given type and fields.

        Args:
            block_type: One of the valid Block_Type values:
                       text, heading, table, image, list, code, metadata
            **fields:   Type-specific payload fields

        Returns:
            ContentBlock dict with 'type' and all provided fields
            
        Example:
            block = self._make_block("text", content="Hello world", page=1)
            # Returns: {"type": "text", "content": "Hello world", "page": 1}
        """
        return {"type": block_type, **fields}
