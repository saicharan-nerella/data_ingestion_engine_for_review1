"""
detector — Format detection utilities.

The FormatDetector maps file extensions and magic bytes to ReaderPlugin identifiers,
enabling the UnifiedParser to route each S3 object to the correct format-specific reader.
"""
