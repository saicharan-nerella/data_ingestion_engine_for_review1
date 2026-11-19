"""
parser/ecs_http_parser.py
Single HTTP-only parser for AWS Glue jobs.

This is the ONLY parser file needed. It calls the ECS Fargate service
via HTTP to process all document formats.

Features:
- HTTP-only communication with ECS Docling service  
- No binary dependencies (tesserocr, docling, etc.)
- Works with existing pipeline architecture
- Supports all 20+ document formats via ECS
- Compatible with GenericUnstructuredReader
- Standardized ExtractionRecord output format
"""

import logging
import json
import time
import requests
from typing import Dict, Any
from datetime import datetime, timezone

log = logging.getLogger(__name__)


class ECSHttpParser:
    """
    HTTP-only parser that calls ECS Fargate service for document processing.
    
    This replaces all format-specific parsers with a single HTTP client
    that delegates processing to the ECS service.
    """
    
    def __init__(self, config: dict):
        """
        Initialize ECS HTTP parser.
        
        Args:
            config: Configuration dict with ecs_endpoint
        """
        self.config = config
        self.ecs_endpoint = config.get("ecs_endpoint", "").rstrip('/')
        # Default timeout: 900s (15 minutes) for large PDFs with OCR
        # Can be overridden via config or Glue parameter
        self.timeout = config.get("timeout", 900)
        # Retry on transient failures (connection errors, timeouts, 5xx) —
        # mirrors structured_readers/batch_api_read.py's retry pattern, using
        # the same settings.retries / settings.retry_delay_seconds config
        # keys (threaded in from run_unstructured_pipeline). Without this, a
        # single ECS cold-start or network blip permanently failed that file.
        self.max_retries = config.get("retries", 2)
        self.retry_delay = config.get("retry_delay_seconds", 60)

        if not self.ecs_endpoint:
            raise ValueError("ecs_endpoint is required in config")
        
        if not self.ecs_endpoint.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid ECS endpoint: {self.ecs_endpoint}")
        
        log.info("✓ ECSHttpParser initialized: %s (timeout: %ds)", 
                 self.ecs_endpoint, self.timeout)
    
    def extract(self, object_key: str, file_bytes: bytes) -> Dict[str, Any]:
        """
        Extract content from file via ECS HTTP service.
        
        This method matches the interface expected by the existing
        unstructured pipeline architecture.
        
        Args:
            object_key: S3 object key (e.g., "documents/report.pdf")
            file_bytes: Raw file bytes
        
        Returns:
            ExtractionRecord dict with standardized format:
            {
                "source_uri": "s3://bucket/key",
                "format": "pdf",
                "extracted_at": "2024-01-01T10:30:00Z",
                "content": {
                    "blocks": [
                        {"type": "text", "text": "...", "bbox": [...], ...},
                        {"type": "image", "path": "...", "bbox": [...], ...},
                        ...
                    ]
                }
            }
            
            On error:
            {
                "source_uri": "s3://bucket/key", 
                "format": "unknown",
                "extracted_at": "2024-01-01T10:30:00Z",
                "content": None,
                "error": "Error message"
            }
        """
        filename = object_key.split("/")[-1]  # Extract filename from key
        files = {"file": (filename, file_bytes)}

        last_error_msg = "Unknown error"

        for attempt in range(self.max_retries + 1):
            try:
                log.info("Parsing via ECS: %s (%d bytes)%s", object_key, len(file_bytes),
                         f" [attempt {attempt + 1}/{self.max_retries + 1}]" if attempt else "")

                response = requests.post(
                    f"{self.ecs_endpoint}/parse",
                    files=files,
                    timeout=self.timeout
                )

                if response.status_code == 200:
                    # Parse successful response. NOT response.json() — that
                    # decodes via response.encoding, which requests guesses
                    # from the Content-Type header's charset (absent here,
                    # since FastAPI's JSONResponse doesn't declare one) or
                    # falls back to chardet/charset_normalizer statistical
                    # detection. On a mostly-ASCII document with only a
                    # handful of multi-byte UTF-8 sequences (bullets, ≥/≤,
                    # accented names), that guesser frequently misfires to
                    # ISO-8859-1, mangling every non-ASCII character
                    # ("•" -> "â¢", "≥" -> "â¥") without raising any error.
                    # The response bytes are always UTF-8 (FastAPI's JSON
                    # encoder), so decode that directly instead of guessing.
                    data = json.loads(response.content.decode("utf-8"))
                    blocks = data.get("blocks", [])

                    content = {"blocks": blocks}
                    # extraction_stats (raw items seen vs. blocks kept, OCR pages
                    # dropped as header/footer, etc.) — carried through so it's
                    # visible in the final S3 output, not just docling_service's
                    # own logs. Answers "does the raw parser output contain the
                    # expected blocks" without a separate debug artifact.
                    if "extraction_stats" in data:
                        content["extraction_stats"] = data["extraction_stats"]

                    # Create standardized ExtractionRecord
                    result = {
                        "source_uri": f"s3://unknown/{object_key}",  # Will be updated by pipeline
                        "format": data.get("format", "unknown"),
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "content": content
                    }

                    log.info("✓ Parsed %s - %d blocks extracted", object_key, len(blocks))
                    return result

                elif response.status_code < 500:
                    # Client-side error (bad request, unsupported format the
                    # service itself rejected, etc.) — retrying won't change
                    # the outcome, so fail immediately.
                    error_msg = f"ECS error: HTTP {response.status_code}"
                    if response.text:
                        error_msg += f" - {response.text[:200]}"
                    log.error("ECS parsing failed for %s (not retrying): %s", object_key, error_msg)
                    return self._create_error_record(object_key, error_msg)

                else:
                    # Server-side error — transient, worth retrying.
                    last_error_msg = f"ECS error: HTTP {response.status_code}"
                    if response.text:
                        last_error_msg += f" - {response.text[:200]}"
                    log.warning("ECS parsing failed for %s, attempt %d/%d: %s",
                                object_key, attempt + 1, self.max_retries + 1, last_error_msg)

            except requests.exceptions.ConnectionError as e:
                last_error_msg = f"Cannot connect to ECS at {self.ecs_endpoint}: {e}"
                log.warning("Connection error for %s, attempt %d/%d: %s",
                            object_key, attempt + 1, self.max_retries + 1, last_error_msg)

            except requests.exceptions.Timeout:
                last_error_msg = f"ECS timeout ({self.timeout}s)"
                log.warning("Timeout for %s, attempt %d/%d",
                            object_key, attempt + 1, self.max_retries + 1)

            except Exception as e:
                # Unexpected errors aren't necessarily transient — but a
                # bounded retry still costs little and covers the common
                # case of a flaky connection surfacing as a generic error.
                last_error_msg = f"Unexpected error: {e}"
                log.warning("Parse error for %s, attempt %d/%d: %s",
                            object_key, attempt + 1, self.max_retries + 1, last_error_msg, exc_info=True)

            if attempt < self.max_retries:
                wait = self.retry_delay * (2 ** attempt)  # exponential backoff
                log.info("Retrying %s in %ds", object_key, wait)
                time.sleep(wait)

        log.error("ECS parsing failed for %s after %d attempts: %s",
                   object_key, self.max_retries + 1, last_error_msg)
        return self._create_error_record(object_key, last_error_msg)
    
    def parse(self, object_key: str, file_bytes: bytes) -> Dict[str, Any]:
        """
        Alias for extract() to support both method names.
        
        Args:
            object_key: S3 object key
            file_bytes: Raw file bytes
        
        Returns:
            ExtractionRecord dict (same as extract())
        """
        return self.extract(object_key, file_bytes)
    
    def _create_error_record(self, object_key: str, error_message: str) -> Dict[str, Any]:
        """
        Create standardized error record.
        
        Args:
            object_key: S3 object key
            error_message: Error description
        
        Returns:
            ExtractionRecord with error
        """
        return {
            "source_uri": f"s3://unknown/{object_key}",
            "format": "unknown", 
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "content": None,
            "error": error_message
        }
    
    def check_health(self) -> bool:
        """
        Check if ECS service is healthy and reachable.
        
        Returns:
            True if service is healthy, False otherwise
        """
        try:
            log.info("Checking ECS health: %s/health", self.ecs_endpoint)
            response = requests.get(
                f"{self.ecs_endpoint}/health",
                timeout=10
            )
            
            if response.status_code == 200:
                log.info("✓ ECS service is healthy")
                return True
            else:
                log.error("ECS health check failed: HTTP %d", response.status_code)
                return False
        
        except Exception as e:
            log.error("ECS health check error: %s", e)
            return False