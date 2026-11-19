"""
detector/format_detector.py — File format detection and validation.

Detects file formats and validates against supported formats.
Works with the generic unstructured reader to ensure only supported
files are processed by UnifiedDoclingParser.
"""

import logging
from typing import Dict, List, Tuple

log = logging.getLogger(__name__)


class FormatDetector:
    """
    Detects and validates file formats.
    
    Supported formats (20+):
    - Documents: PDF, DOCX, PPTX, XLSX, HTML, Markdown, CSV, JSON, Text, XML, LaTeX, AsciiDoc, WebVTT
    - Images: PNG, JPEG, TIFF, BMP, WEBP, GIF
    - Audio: MP3, WAV, FLAC, OGG, M4A
    - Video: MP4, AVI, MOV, MKV, WEBM
    """

    # Supported file extensions
    SUPPORTED_FORMATS = {
        # Documents
        "pdf": {"category": "document", "mime": "application/pdf"},
        "docx": {"category": "document", "mime": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
        "doc": {"category": "document", "mime": "application/msword"},
        "pptx": {"category": "document", "mime": "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
        "ppt": {"category": "document", "mime": "application/vnd.ms-powerpoint"},
        "xlsx": {"category": "document", "mime": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
        "xls": {"category": "document", "mime": "application/vnd.ms-excel"},
        "html": {"category": "document", "mime": "text/html"},
        "htm": {"category": "document", "mime": "text/html"},
        "md": {"category": "document", "mime": "text/markdown"},
        "markdown": {"category": "document", "mime": "text/markdown"},
        "csv": {"category": "document", "mime": "text/csv"},
        "json": {"category": "document", "mime": "application/json"},
        "txt": {"category": "document", "mime": "text/plain"},
        "text": {"category": "document", "mime": "text/plain"},
        "xml": {"category": "document", "mime": "application/xml"},
        "tex": {"category": "document", "mime": "application/x-latex"},
        "latex": {"category": "document", "mime": "application/x-latex"},
        "adoc": {"category": "document", "mime": "text/asciidoc"},
        "asciidoc": {"category": "document", "mime": "text/asciidoc"},
        "vtt": {"category": "document", "mime": "text/vtt"},
        
        # Images
        "png": {"category": "image", "mime": "image/png"},
        "jpg": {"category": "image", "mime": "image/jpeg"},
        "jpeg": {"category": "image", "mime": "image/jpeg"},
        "tiff": {"category": "image", "mime": "image/tiff"},
        "tif": {"category": "image", "mime": "image/tiff"},
        "bmp": {"category": "image", "mime": "image/bmp"},
        "webp": {"category": "image", "mime": "image/webp"},
        "gif": {"category": "image", "mime": "image/gif"},
        
        # Audio
        "mp3": {"category": "audio", "mime": "audio/mpeg"},
        "wav": {"category": "audio", "mime": "audio/wav"},
        "flac": {"category": "audio", "mime": "audio/flac"},
        "ogg": {"category": "audio", "mime": "audio/ogg"},
        "m4a": {"category": "audio", "mime": "audio/mp4"},
        
        # Video
        "mp4": {"category": "video", "mime": "video/mp4"},
        "avi": {"category": "video", "mime": "video/x-msvideo"},
        "mov": {"category": "video", "mime": "video/quicktime"},
        "mkv": {"category": "video", "mime": "video/x-matroska"},
        "webm": {"category": "video", "mime": "video/webm"},
    }

    def __init__(self, config: dict = None):
        """
        Initialize format detector.
        
        Args:
            config: Optional configuration dict
        """
        self.config = config or {}
        log.info("✓ FormatDetector initialized with %d supported formats", len(self.SUPPORTED_FORMATS))

    def detect_format(self, object_key: str) -> Tuple[str, bool]:
        """
        Detect file format from object key.
        
        Args:
            object_key: S3 object key (e.g., "documents/report.pdf")
        
        Returns:
            Tuple of (format_string, is_supported)
            Example: ("pdf", True) or ("unknown", False)
        """
        # Extract extension
        if "." not in object_key:
            return "unknown", False
        
        ext = object_key.rsplit(".", 1)[-1].lower()
        
        # Check if supported
        is_supported = ext in self.SUPPORTED_FORMATS
        
        return ext, is_supported

    def get_format_info(self, file_format: str) -> Dict:
        """
        Get detailed information about a format.
        
        Args:
            file_format: File format string (e.g., "pdf")
        
        Returns:
            Dict with format metadata or empty dict if not supported
        """
        return self.SUPPORTED_FORMATS.get(file_format, {})

    def is_supported(self, object_key: str) -> bool:
        """
        Check if file format is supported.
        
        Args:
            object_key: S3 object key
        
        Returns:
            True if format is supported, False otherwise
        """
        _, is_supported = self.detect_format(object_key)
        return is_supported

    def validate_file(self, object_key: str, file_bytes: bytes = None) -> Dict:
        """
        Validate a file for processing.
        
        Args:
            object_key: S3 object key
            file_bytes: Optional file bytes for additional validation
        
        Returns:
            Dict with validation results:
              - valid: bool
              - format: str
              - category: str
              - mime: str
              - size_bytes: int
              - errors: list of error messages
        """
        file_format, is_supported = self.detect_format(object_key)
        
        result = {
            "valid": is_supported,
            "format": file_format,
            "category": None,
            "mime": None,
            "size_bytes": len(file_bytes) if file_bytes else 0,
            "errors": []
        }
        
        if not is_supported:
            result["errors"].append(f"Unsupported format: {file_format}")
            return result
        
        # Get format info
        format_info = self.get_format_info(file_format)
        result["category"] = format_info.get("category")
        result["mime"] = format_info.get("mime")
        
        # Validate file size (if provided)
        if file_bytes:
            size_mb = len(file_bytes) / (1024 * 1024)
            
            # Warn if file is very large
            if size_mb > 500:
                result["errors"].append(f"File is very large: {size_mb:.1f} MB (may be slow to process)")
            
            # Reject if file is empty
            if len(file_bytes) == 0:
                result["valid"] = False
                result["errors"].append("File is empty")
        
        return result

    def get_supported_formats(self) -> Dict[str, List[str]]:
        """
        Get all supported formats grouped by category.
        
        Returns:
            Dict with categories as keys and format lists as values
        """
        formats_by_category = {}
        
        for fmt, info in self.SUPPORTED_FORMATS.items():
            category = info.get("category", "unknown")
            if category not in formats_by_category:
                formats_by_category[category] = []
            formats_by_category[category].append(fmt)
        
        return formats_by_category

    def get_format_statistics(self, file_list: List[str]) -> Dict:
        """
        Analyze a list of files and return format statistics.
        
        Args:
            file_list: List of S3 object keys
        
        Returns:
            Dict with statistics:
              - total_files: int
              - supported_files: int
              - unsupported_files: int
              - formats_found: dict of format -> count
              - categories_found: dict of category -> count
        """
        stats = {
            "total_files": len(file_list),
            "supported_files": 0,
            "unsupported_files": 0,
            "formats_found": {},
            "categories_found": {}
        }
        
        for object_key in file_list:
            file_format, is_supported = self.detect_format(object_key)
            
            # Count format
            if file_format not in stats["formats_found"]:
                stats["formats_found"][file_format] = 0
            stats["formats_found"][file_format] += 1
            
            if is_supported:
                stats["supported_files"] += 1
                
                # Count category
                format_info = self.get_format_info(file_format)
                category = format_info.get("category", "unknown")
                if category not in stats["categories_found"]:
                    stats["categories_found"][category] = 0
                stats["categories_found"][category] += 1
            else:
                stats["unsupported_files"] += 1
        
        return stats

    def filter_supported_files(self, file_list: List[str]) -> Tuple[List[str], List[str]]:
        """
        Filter a list of files into supported and unsupported.
        
        Args:
            file_list: List of S3 object keys
        
        Returns:
            Tuple of (supported_files, unsupported_files)
        """
        supported = []
        unsupported = []
        
        for object_key in file_list:
            if self.is_supported(object_key):
                supported.append(object_key)
            else:
                unsupported.append(object_key)
        
        return supported, unsupported
