"""
structured_readers/batch_cloud_read.py
──────────────────────────────────────
Batch reader for cloud storage sources (S3, GCS, Azure Blob).

Reads files from cloud object storage defined under config["batch"]["cloud"],
handles multiple file formats, and returns raw data records.

Transformation happens at the architecture layer.
Writing is handled by the user-selected writer component.

Registered in core/registry.py as:
  "reader": { "batch_cloud": "structured_readers.batch_cloud_read.BatchCloudReader" }
"""

import logging
from typing import Generator, List, Dict, Any
import boto3

from structured_readers.base_reader import BaseReader

log = logging.getLogger(__name__)


class BatchCloudReader(BaseReader):
    """
    Reads data from cloud storage (S3, GCS, Azure Blob).

    Responsibility: READ ONLY
    - Lists objects from configured cloud storage bucket
    - Downloads and parses files (CSV, JSON, Parquet)
    - Returns raw records without transformation
    - Transformation is delegated to the architecture layer
    - Writing is delegated to the user-selected writer

    Supports:
      - Providers: AWS S3, GCS, Azure Blob
      - Formats: CSV, JSON, Parquet
      - Incremental loads via checkpoint
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self._client = None
        self._connected = False

    # ── BaseReader contract ───────────────────────────────────────────────────

    def connect(self) -> None:
        """Initialize cloud storage client."""
        cloud_cfg = self.config.get("structured", {}).get("batch", {}).get("cloud", [{}])[0]
        provider = cloud_cfg.get("provider", "aws").lower()
        
        if provider == "aws":
            region = cloud_cfg.get("source", {}).get("region", "us-east-1")
            self._client = boto3.client("s3", region_name=region)
            log.info("BatchCloudReader: AWS S3 client ready")
        
        elif provider == "gcs":
            try:
                from google.cloud import storage
                self._client = storage.Client()
                log.info("BatchCloudReader: GCS client ready")
            except ImportError:
                log.error("BatchCloudReader: google-cloud-storage not installed")
                raise
        
        elif provider == "azure":
            try:
                from azure.storage.blob import BlobServiceClient
                account_name = cloud_cfg.get("source", {}).get("account_name")
                account_key = cloud_cfg.get("source", {}).get("account_key")
                self._client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=account_key
                )
                log.info("BatchCloudReader: Azure Blob client ready")
            except ImportError:
                log.error("BatchCloudReader: azure-storage-blob not installed")
                raise
        
        else:
            raise ValueError(f"Unknown provider: {provider}")
        
        self._connected = True

    def read(self) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Yield raw record lists from each file in cloud storage.
        Each yield is a list of dictionaries (parsed file contents).
        No transformation is applied here.
        """
        if not self._connected:
            self.connect()
        
        cloud_cfg = self.config.get("structured", {}).get("batch", {}).get("cloud", [{}])[0]
        provider = cloud_cfg.get("provider", "aws").lower()
        bucket = cloud_cfg.get("source", {}).get("bucket")
        prefix = cloud_cfg.get("source", {}).get("prefix", "")
        file_format = cloud_cfg.get("source", {}).get("format", "csv").lower()
        
        if not bucket:
            raise ValueError("No bucket configured for cloud storage")
        
        log.info("BatchCloudReader: reading from %s://%s/%s (format: %s)",
                 provider, bucket, prefix, file_format)
        
        # List objects
        objects = self._list_objects(provider, bucket, prefix)
        
        if not objects:
            log.warning("BatchCloudReader: no objects found in %s/%s", bucket, prefix)
            return
        
        # Process each object
        for obj_key in objects:
            try:
                log.info("BatchCloudReader: processing %s", obj_key)
                records = self._read_object(provider, bucket, obj_key, file_format)
                
                if records:
                    log.info("BatchCloudReader: %s → %d records", obj_key, len(records))
                    yield records
            
            except Exception as exc:
                log.error("BatchCloudReader: failed to read %s: %s", obj_key, exc)
                continue

    def close(self) -> None:
        """Close cloud storage client."""
        self._connected = False
        log.info("BatchCloudReader: client closed")

    # ── Cloud storage helpers ─────────────────────────────────────────────────

    def _list_objects(self, provider: str, bucket: str, prefix: str) -> List[str]:
        """
        List objects in cloud storage.
        
        If config has 'files' list, uses those directly (no ListObjectsV2 needed).
        Otherwise falls back to listing by prefix.
        """
        # Check if specific files are listed in config (avoids ListObjectsV2)
        cloud_cfg = self.config.get("structured", {}).get("batch", {}).get("cloud", [{}])[0]
        files = cloud_cfg.get("source", {}).get("files", [])
        
        if files:
            log.info("BatchCloudReader: using explicit file list (%d files)", len(files))
            return files
        
        # Fallback: list objects by prefix
        objects = []
        
        try:
            if provider == "aws":
                paginator = self._client.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
                
                for page in pages:
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            objects.append(obj["Key"])
            
            elif provider == "gcs":
                blobs = self._client.list_blobs(bucket, prefix=prefix)
                objects = [blob.name for blob in blobs]
            
            elif provider == "azure":
                container_client = self._client.get_container_client(bucket)
                blobs = container_client.list_blobs(name_starts_with=prefix)
                objects = [blob.name for blob in blobs]
        
        except Exception as exc:
            log.error("BatchCloudReader: failed to list objects: %s", exc)
            raise
        
        log.info("BatchCloudReader: found %d objects", len(objects))
        return objects

    def _read_object(self, provider: str, bucket: str, obj_key: str, 
                     file_format: str) -> List[Dict[str, Any]]:
        """Download and parse a single object."""
        try:
            # Download object
            if provider == "aws":
                response = self._client.get_object(Bucket=bucket, Key=obj_key)
                content = response["Body"].read()
            
            elif provider == "gcs":
                blob = self._client.bucket(bucket).blob(obj_key)
                content = blob.download_as_bytes()
            
            elif provider == "azure":
                container_client = self._client.get_container_client(bucket)
                blob_client = container_client.get_blob_client(obj_key)
                content = blob_client.download_blob().readall()
            
            else:
                raise ValueError(f"Unknown provider: {provider}")
            
            # Parse based on format
            if file_format == "csv":
                import csv
                import io

                text = content.decode("utf-8")
                reader = csv.DictReader(io.StringIO(text))
                records = list(reader)
            
            elif file_format == "json":
                import json
                
                text = content.decode("utf-8")
                data = json.loads(text)
                
                # Handle both list and dict responses
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    # Try common envelope keys
                    for key in ("data", "results", "items", "records"):
                        if key in data and isinstance(data[key], list):
                            records = data[key]
                            break
                    else:
                        records = [data]
                else:
                    records = []
            
            elif file_format == "parquet":
                import pyarrow.parquet as pq
                import io
                
                parquet_file = pq.read_table(io.BytesIO(content))
                records = parquet_file.to_pylist()
            
            else:
                raise ValueError(f"Unsupported format: {file_format}")
            
            return records
        
        except Exception as exc:
            log.error("BatchCloudReader: failed to parse %s: %s", obj_key, exc)
            raise

