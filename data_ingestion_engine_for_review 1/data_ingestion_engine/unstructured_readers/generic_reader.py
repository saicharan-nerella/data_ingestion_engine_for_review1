"""
unstructured_readers/generic_reader.py — Generic unstructured reader for all file formats.

This is the ONLY unstructured reader needed. It:
1. Lists files from S3 (any format)
2. Downloads file bytes
3. Delegates parsing to the ECS Docling service (parser/ecs_http_parser.py)
4. Returns standardized ExtractionRecord

Format handling: this reader does NOT gatekeep by a fixed extension
allowlist. docling_service already degrades gracefully on a genuinely
unparseable format (returns an error record, which the pipeline writes to
S3 with stats["failed"] incremented) — so the reader forwards everything
except a short blocklist of clearly non-document files (archives,
binaries, directory markers) and lets the parser be the actual authority
on what's parseable. An earlier version of this reader used a fixed
allowlist of ~33 extensions; anything outside it (a new format, an
uncommon one, or even a file explicitly named in source.files) was
silently dropped before ever reaching the parser — no error record, no
visibility, just gone. That directly contradicted this pipeline's own
design goal of supporting new formats with zero code changes.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Generator, Dict, Any

import boto3

log = logging.getLogger(__name__)


class GenericUnstructuredReader:
    """
    Generic reader for all unstructured file formats.
    
    Reads files from S3 and yields file metadata for parsing.
    Format detection and parsing is handled by UnifiedDoclingParser.
    """

    def __init__(self, config: dict):
        """
        Initialize generic unstructured reader.
        
        Args:
            config: Configuration dict with unstructured.source section
        """
        self.config = config
        self.source_config = config.get("unstructured", {}).get("source", {})
        
        # S3 configuration
        self.bucket = self.source_config.get("bucket")
        self.prefix = self.source_config.get("prefix", "")
        self.region = self.source_config.get("region", "us-east-1")
        
        # Explicit files list (optional)
        self.explicit_files = self.source_config.get("files", [])
        
        # Auth configuration
        auth_config = self.source_config.get("auth", {})
        self.auth_type = auth_config.get("type", "iam_role")
        
        # Load mode
        load_config = self.source_config.get("load", {})
        self.load_mode = load_config.get("mode", "full")  # full | incremental
        
        # Checkpoint configuration (for incremental loads)
        checkpoint_config = load_config.get("checkpoint", {})
        self.checkpoint_type = checkpoint_config.get("type")  # s3 | local
        self.checkpoint_path = checkpoint_config.get("path")
        
        # Initialize S3 client
        self.s3 = self._init_s3_client(auth_config)
        
        # Track last processed key for incremental loads
        self.last_processed_key = None
        if self.load_mode == "incremental":
            self.last_processed_key = self._load_checkpoint()
        
        log.info(
            "✓ GenericUnstructuredReader initialized: bucket=%s, prefix=%s, mode=%s, explicit_files=%d",
            self.bucket, self.prefix, self.load_mode, len(self.explicit_files)
        )

    def _init_s3_client(self, auth_config: dict) -> boto3.client:
        """Initialize S3 client based on auth configuration."""
        auth_type = auth_config.get("type", "iam_role")
        
        if auth_type == "iam_role":
            # Use IAM role (default for Glue)
            return boto3.client("s3", region_name=self.region)
        
        elif auth_type == "access_key":
            # Use access key credentials
            access_key = auth_config.get("access_key")
            secret_key = auth_config.get("secret_key")
            return boto3.client(
                "s3",
                region_name=self.region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
        
        elif auth_type == "secrets_manager":
            # Load credentials from Secrets Manager
            secret_name = auth_config.get("secret_name")
            secret_region = auth_config.get("region", self.region)
            
            secrets_client = boto3.client("secretsmanager", region_name=secret_region)
            secret = secrets_client.get_secret_value(SecretId=secret_name)
            
            import json
            creds = json.loads(secret["SecretString"])
            
            return boto3.client(
                "s3",
                region_name=self.region,
                aws_access_key_id=creds["access_key"],
                aws_secret_access_key=creds["secret_key"]
            )
        
        else:
            raise ValueError(f"Unsupported auth type: {auth_type}")

    def _load_checkpoint(self) -> str:
        """Load last processed key from checkpoint."""
        if not self.checkpoint_type or not self.checkpoint_path:
            return None
        
        try:
            if self.checkpoint_type == "s3":
                # Load from S3
                bucket, key = self.checkpoint_path.replace("s3://", "").split("/", 1)
                obj = self.s3.get_object(Bucket=bucket, Key=key)
                last_key = obj["Body"].read().decode("utf-8").strip()
                log.info("✓ Loaded checkpoint from S3: %s", last_key)
                return last_key
            
            elif self.checkpoint_type == "local":
                # Load from local file
                if os.path.exists(self.checkpoint_path):
                    with open(self.checkpoint_path, "r") as f:
                        last_key = f.read().strip()
                    log.info("✓ Loaded checkpoint from local: %s", last_key)
                    return last_key
        
        except Exception as e:
            log.warning("Failed to load checkpoint: %s", e)
        
        return None

    def _save_checkpoint(self, object_key: str) -> None:
        """Save last processed key to checkpoint."""
        if not self.checkpoint_type or not self.checkpoint_path:
            return
        
        try:
            if self.checkpoint_type == "s3":
                # Save to S3
                bucket, key = self.checkpoint_path.replace("s3://", "").split("/", 1)
                self.s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=object_key.encode("utf-8")
                )
                log.debug("✓ Saved checkpoint to S3: %s", object_key)
            
            elif self.checkpoint_type == "local":
                # Save to local file
                os.makedirs(os.path.dirname(self.checkpoint_path), exist_ok=True)
                with open(self.checkpoint_path, "w") as f:
                    f.write(object_key)
                log.debug("✓ Saved checkpoint to local: %s", object_key)
        
        except Exception as e:
            log.warning("Failed to save checkpoint: %s", e)

    # Extensions that are never documents, regardless of what docling_service
    # might theoretically attempt — excluded from a full-bucket scan so a
    # stray archive/binary/lockfile in the source bucket doesn't waste an
    # ECS call and a slot in the failure stats. This is a blocklist, not an
    # allowlist: anything NOT in this set is forwarded to the parser, which
    # is the actual authority on what it can parse.
    _NEVER_DOCUMENT_EXTENSIONS = {
        'zip', 'tar', 'gz', 'tgz', 'bz2', '7z', 'rar',
        'exe', 'dll', 'so', 'bin', 'dmg', 'app',
        'lock', 'pyc', 'class', 'o', 'a',
    }

    # Common sync/metadata artifacts that have NO extension by Python's own
    # os.path.splitext rules (a leading-dot-only name like ".gitkeep" or
    # ".DS_Store" splits to an empty extension, not "gitkeep"/"DS_Store") —
    # matched by exact basename instead.
    _NEVER_DOCUMENT_BASENAMES = {
        '.ds_store', '.gitkeep', '.gitignore', 'thumbs.db',
    }

    def _is_valid_document(self, filename: str) -> bool:
        """
        Check if a file (found via a full-bucket scan) is worth sending to
        the parser.

        Deliberately permissive: docling_service already returns a clear
        error record for a format it genuinely can't parse (see module
        docstring), so this only excludes a short blocklist of things that
        are never documents (archives, binaries, directory markers, common
        sync/metadata artifacts) rather than gatekeeping by a fixed
        allowlist. Only called for the full-bucket-scan path — explicitly
        named files in source.files always get processed, since naming a
        file is itself the user's intent to process it (see list_files()).

        Args:
            filename: Filename or S3 key

        Returns:
            True if the file should be sent to the parser
        """
        basename = filename.lower().rsplit('/', 1)[-1]
        if basename in self._NEVER_DOCUMENT_BASENAMES:
            return False

        _, ext = os.path.splitext(basename)
        ext = ext.lstrip('.')

        # No extension at all is still worth trying — docling_service falls
        # back to its generic Docling converter and fails gracefully if the
        # content genuinely isn't parseable.
        if not ext:
            return True

        return ext not in self._NEVER_DOCUMENT_EXTENSIONS
    
    def list_files(self) -> Generator[Dict[str, Any], None, None]:
        """
        List all files in S3 bucket with prefix.
        
        If explicit_files list is provided in config, only those files are listed.
        Otherwise, all files under prefix are listed.
        
        Explicitly named files (source.files in config.yaml) are always
        processed — naming a file is itself the user's intent, so this never
        second-guesses it with a format check. A full-bucket scan excludes
        only a short blocklist of things that are never documents (archives,
        binaries, directory markers); see _is_valid_document().

        Yields:
            Dict with keys:
              - key: S3 object key
              - size: File size in bytes
              - last_modified: ISO 8601 timestamp
              - source_uri: Full S3 URI
        """
        log.info("Listing files from s3://%s/%s", self.bucket, self.prefix)

        file_count = 0
        skipped_count = 0

        # If explicit files list provided, use that — always processed,
        # regardless of extension.
        if self.explicit_files:
            log.info("Using explicit file list: %d file(s)", len(self.explicit_files))
            for filename in self.explicit_files:
                # Construct full S3 key
                if self.prefix:
                    key = f"{self.prefix}{filename}" if not filename.startswith(self.prefix) else filename
                else:
                    key = filename
                
                # Get object metadata
                try:
                    response = self.s3.head_object(Bucket=self.bucket, Key=key)
                    file_count += 1
                    
                    yield {
                        "key": key,
                        "size": response["ContentLength"],
                        "last_modified": response["LastModified"].isoformat(),
                        "source_uri": f"s3://{self.bucket}/{key}"
                    }
                except Exception as e:
                    log.warning("Skipping file %s: %s", key, e)
                    skipped_count += 1
        else:
            # List all files under prefix
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)
            
            for page in pages:
                if "Contents" not in page:
                    continue
                
                for obj in page["Contents"]:
                    key = obj["Key"]
                    
                    # Skip directories
                    if key.endswith("/"):
                        continue
                    
                    # Skip non-document files
                    if not self._is_valid_document(key):
                        log.debug("Skipping non-document file: %s", key)
                        skipped_count += 1
                        continue
                    
                    # Skip if before checkpoint (for incremental loads)
                    if self.load_mode == "incremental" and self.last_processed_key:
                        if key <= self.last_processed_key:
                            skipped_count += 1
                            continue
                    
                    file_count += 1
                    
                    yield {
                        "key": key,
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                        "source_uri": f"s3://{self.bucket}/{key}"
                    }
        
        log.info(
            "✓ Listed %d files (skipped %d from checkpoint)",
            file_count, skipped_count
        )

    def download_file(self, object_key: str) -> bytes:
        """
        Download file from S3.
        
        Args:
            object_key: S3 object key
        
        Returns:
            File bytes
        """
        log.debug("Downloading file: %s", object_key)
        
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=object_key)
            file_bytes = obj["Body"].read()
            log.debug("✓ Downloaded %d bytes", len(file_bytes))
            return file_bytes
        
        except Exception as e:
            log.error("Failed to download file %s: %s", object_key, e)
            raise

    def get_file_format(self, object_key: str) -> str:
        """
        Detect file format from extension.
        
        Args:
            object_key: S3 object key
        
        Returns:
            File format string (e.g., "pdf", "docx", "png")
        """
        _, ext = os.path.splitext(object_key)
        file_format = ext.lstrip(".").lower()
        return file_format if file_format else "unknown"

    def process_file(self, object_key: str, file_bytes: bytes) -> Dict[str, Any]:
        """
        Process a single file.
        
        This method returns file metadata for the parser.
        Actual parsing is done by UnifiedDoclingParser.
        
        Args:
            object_key: S3 object key
            file_bytes: File bytes
        
        Returns:
            Dict with keys:
              - source_uri: Full S3 URI
              - object_key: S3 object key
              - file_format: Detected format
              - file_size: Size in bytes
              - file_bytes: Raw file bytes
              - discovered_at: ISO 8601 timestamp
        """
        file_format = self.get_file_format(object_key)
        
        return {
            "source_uri": f"s3://{self.bucket}/{object_key}",
            "object_key": object_key,
            "file_format": file_format,
            "file_size": len(file_bytes),
            "file_bytes": file_bytes,
            "discovered_at": datetime.now(timezone.utc).isoformat()
        }

    def read(self) -> Generator[Dict[str, Any], None, None]:
        """
        Main read method: list files, download, and yield for parsing.
        
        Yields:
            Dict with file metadata and bytes for UnifiedDoclingParser
        """
        log.info("Starting unstructured file reading from S3")
        
        total_files = 0
        total_bytes = 0
        
        try:
            for file_info in self.list_files():
                object_key = file_info["key"]
                
                try:
                    # Download file
                    file_bytes = self.download_file(object_key)
                    
                    # Process file metadata
                    file_record = self.process_file(object_key, file_bytes)
                    
                    total_files += 1
                    total_bytes += len(file_bytes)
                    
                    log.info(
                        "✓ Read file %d: %s (%s, %d bytes)",
                        total_files, object_key, file_record["file_format"], len(file_bytes)
                    )
                    
                    yield file_record
                    
                    # Save checkpoint after successful read
                    if self.load_mode == "incremental":
                        self._save_checkpoint(object_key)
                
                except Exception as e:
                    log.error("Failed to process file %s: %s", object_key, e)
                    # Continue with next file
                    continue
        
        except Exception as e:
            log.error("Error during file reading: %s", e)
            raise
        
        log.info(
            "✓ Completed file reading: %d files, %.2f MB",
            total_files, total_bytes / (1024 * 1024)
        )

    def close(self) -> None:
        """Close reader (no-op for S3)."""
        log.debug("Closing GenericUnstructuredReader")
