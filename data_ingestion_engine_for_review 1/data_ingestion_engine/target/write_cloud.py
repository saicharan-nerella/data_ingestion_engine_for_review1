"""
target/write_cloud.py
─────────────────────
Generic, format-agnostic cloud storage writer.

Responsibility:
- Accept ANY record from ANY source (PDF, HTML, CSV, database, API, etc)
- Convert to ANY output format (JSON, CSV, Parquet, XML, HTML, Markdown, Text)
- Write to ANY cloud provider (AWS S3, GCP Cloud Storage, Azure Blob Storage)
- NO data transformation or format-specific logic

Key Design Principles:
1. Format-Agnostic: Works with any record structure
2. Provider-Agnostic: Works with any cloud provider
3. No Transformation: Just serialization and storage
4. Pluggable: Easy to add new formats or providers
5. Reusable: Works with any reader/parser/source

Configuration — two supported shapes, picked automatically (see __init__):

    1. STRUCTURED PIPELINE — flat dict built from Glue job parameters.
       Nothing is read from config.yaml for this pipeline; core/pipeline_builder.py
       assembles this dict entirely from --writer_* args and passes it straight in:
         {
           "provider":    from --writer_provider (default: aws),
           "bucket":      from --writer_bucket   (required),
           "prefix":      from --writer_prefix   (default: ""),
           "region":      from --writer_region   (default: us-east-1),
           "file_format": from --writer_format   (default: json)  # json | csv | parquet | xml | html | md | txt
           "batch_mode":  from --writer_batch_mode (default: false),
           "auth": {"type": from --writer_auth_type (default: iam_role)},
         }
       See main/aimatic.py's module docstring for the full --writer_* parameter list.

    2. UNSTRUCTURED PIPELINE / direct or test usage — nested config.yaml-style dict:
        target_config:
          write_cloud:
            provider: aws | gcp | azure
            bucket: bucket-name
            prefix: path/prefix/
            region: region-name
            file_format: json | csv | parquet | xml | html | md | txt
            batch_mode: true | false
            auth:
              type: iam_role | access_key | service_account | connection_string
              ... (provider-specific auth fields)

Usage Examples:
    # PDF Parser → JSON
    writer = CloudWriter(config)
    record = {"source_uri": "...", "format": "pdf", "content": {...}}
    writer.write(record)
    writer.flush()
    
    # HTML Reader → CSV
    record = {"url": "...", "title": "...", "body": "..."}
    writer.write(record)
    writer.flush()
    
    # CSV Reader → Parquet
    record = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    writer.write(record)
    writer.flush()
"""

import logging
import json
import csv
import io
from typing import List, Dict, Any
from datetime import datetime

log = logging.getLogger(__name__)


class CloudWriter:
    """
    Generic, format-agnostic cloud storage writer.
    
    Accepts ANY record from ANY source and writes to ANY cloud provider
    in ANY output format. No data transformation or format-specific logic.
    """
    
    def __init__(self, config: dict):
        """
        Initialize CloudWriter with cloud provider configuration.
        
        Args:
            config: Either a flat writer-config dict built from Glue --writer_*
                parameters (structured pipeline — see core/pipeline_builder.py),
                or a full config.yaml-shaped dict with target_config.write_cloud
                nested inside (unstructured pipeline / direct or test usage).

        Raises:
            ValueError: If provider is unsupported
        """
        self.config = config
        # Falls back to `config` itself when target_config.write_cloud isn't present,
        # which is exactly the flat-dict case above (structured pipeline via Glue params).
        self.target_config = config.get("target_config", {}).get("write_cloud", config)

        # Cloud provider configuration — structured pipeline: from --writer_provider /
        # --writer_bucket / --writer_prefix / --writer_region Glue parameters.
        self.provider = self.target_config.get("provider", "aws").lower()
        self.bucket = self.target_config.get("bucket", "")
        self.prefix = self.target_config.get("prefix", "")
        self.region = self.target_config.get("region", "us-east-1")

        # Output format — structured pipeline: from the --writer_format Glue parameter
        # (default: json). Must be one of: json | csv | parquet | xml | html | md | txt.
        # parquet requires pyarrow to be installed in the Glue Python environment.
        self.file_format = self.target_config.get("file_format", "json").lower()

        # Buffering configuration
        self._buffer = []
        self._batch_mode = self.target_config.get("batch_mode", False)

        # Write mode — "append" (default) always writes a new file, never
        # touching what's already there. "overwrite" deletes every existing
        # object under the target prefix (or target partition's sub-prefix,
        # if partition_cols is set) immediately before writing — real
        # behavior, not just a parameter that's accepted and ignored.
        # "merge" is deliberately NOT supported here: a true merge/upsert
        # needs a primary key and a table format (Delta/Iceberg/Hudi) or a
        # real database/warehouse — neither of which a flat object-storage
        # writer has. Ask for it anyway and you get a clear error now
        # instead of silently getting "append" behavior, which is what
        # happened before this check existed.
        self.mode = self.target_config.get("mode", "append").lower()
        if self.mode not in ("append", "overwrite"):
            raise ValueError(
                f"Unsupported mode for CloudWriter: '{self.mode}'. Supported: append, overwrite. "
                f"'merge' needs a primary key and a real table/warehouse target — "
                f"use target.write_data_warehouse.DataWarehouseWriter's upsert support instead."
            )

        # Partition columns — Hive-style sub-paths (col1=val1/col2=val2/...)
        # under the configured prefix, one file per distinct combination of
        # values instead of one file for the whole buffer. Empty list (the
        # default) means exactly today's single-file-per-flush behavior.
        self.partition_cols = self.target_config.get("partition_cols") or []

        log.info(
            "CloudWriter: initialized — provider=%s, bucket=%s, prefix=%s, format=%s, "
            "mode=%s, partition_cols=%s",
            self.provider, self.bucket, self.prefix, self.file_format,
            self.mode, self.partition_cols,
        )

        # Validate provider
        if self.provider not in ("aws", "gcp", "azure"):
            raise ValueError(
                f"Unsupported provider: {self.provider}. "
                f"Supported: aws, gcp, azure"
            )

        # Validate format
        supported_formats = ("json", "csv", "parquet", "xml", "html", "md", "txt", "text")
        if self.file_format not in supported_formats:
            raise ValueError(
                f"Unsupported format: {self.file_format}. "
                f"Supported: {', '.join(supported_formats)}"
            )
    
    def write(self, record: Dict[str, Any]) -> None:
        """
        Write a single record to cloud storage.
        
        Accepts ANY record structure from ANY source:
        - PDF parser: ExtractionRecord with nested content.blocks
        - HTML reader: {url, title, body, links, ...}
        - CSV reader: {col1, col2, col3, ...}
        - Database: {id, name, email, ...}
        - API: {user_id, timestamp, data, ...}
        - Any other source: {field1, field2, ...}
        
        Args:
            record: Dictionary to write (any structure)
        
        Raises:
            Exception: If write operation fails
        """
        if not record:
            log.warning("CloudWriter: empty record, skipping")
            return
        
        try:
            # Buffer the record
            self._buffer.append(record)
            
            # Flush immediately if not in batch mode
            if not self._batch_mode:
                self._flush_buffer()
        
        except Exception as exc:
            log.error("CloudWriter: write failed — %s", exc, exc_info=True)
            raise
    
    def flush(self) -> None:
        """
        Flush any buffered records to cloud storage.
        
        Called by the engine after all records have been written.
        """
        self._flush_buffer()
    
    def _flush_buffer(self) -> None:
        """
        Flush buffered records to cloud storage.

        Without partition_cols: one group (the whole buffer), written exactly
        as before. With partition_cols: the buffer is split into one group
        per distinct combination of those columns' values, each written to
        its own Hive-style sub-path — this is the only part that changes
        behavior; a writer with no partition_cols configured behaves
        identically to before this existed.
        """
        if not self._buffer:
            log.debug("CloudWriter: buffer is empty, nothing to flush")
            return

        try:
            if self.partition_cols:
                groups = self._partition_buffer()
                log.info("CloudWriter: flushing %d record(s) across %d partition(s)",
                          len(self._buffer), len(groups))
                for group_prefix, group_records in groups.items():
                    self._write_group(group_records, group_prefix)
            else:
                self._write_group(self._buffer, self.prefix)

            # Clear buffer
            self._buffer = []

        except Exception as exc:
            log.error("CloudWriter: flush failed — %s", exc, exc_info=True)
            raise

    def _partition_buffer(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group the buffer by partition_cols' values into Hive-style
        sub-prefixes, e.g. partition_cols=["date","region"] turns
        prefix="gold/" into "gold/date=2026-01-01/region=us-east-1/" per
        distinct combination actually present in the buffer.

        A record missing one of the partition columns goes under
        "<col>=__missing__" rather than being silently dropped — a record
        with no partition value still needs to land somewhere findable.
        """
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for record in self._buffer:
            parts = []
            for col in self.partition_cols:
                value = record.get(col, "__missing__") if isinstance(record, dict) else "__missing__"
                # Sanitize: a partition value becomes a path segment, so
                # "/" in it would silently create extra sub-directories.
                safe_value = str(value).replace("/", "_") or "__missing__"
                parts.append(f"{col}={safe_value}")
            group_prefix = f"{self.prefix}{'/'.join(parts)}/"
            groups.setdefault(group_prefix, []).append(record)
        return groups

    def _write_group(self, records: List[Dict[str, Any]], prefix: str) -> None:
        """
        Format and write one group of records to one target prefix —
        everything _flush_buffer used to do directly, now reusable per
        partition. In "overwrite" mode, every existing object under this
        exact prefix is deleted first — scoped to just this group's prefix,
        so overwriting one partition (or one Medallion layer, which swaps
        `self.prefix` before each flush) never touches any other.
        """
        log.info("CloudWriter: flushing %d record(s) to %s (prefix=%s)",
                  len(records), self.provider, prefix)

        if self.mode == "overwrite":
            self._delete_existing_under_prefix(prefix)

        # Format records (no transformation, just serialization)
        formatted_data = self._format_records(records, self.file_format)

        # Generate filename based on source file or timestamp
        file_extension = self._get_file_extension(self.file_format)

        # Try to extract source filename from first record
        source_filename = None
        if records and isinstance(records[0], dict):
            # Priority 1: _bronze_source field (set by reader, e.g., "users" from API name)
            bronze_source = records[0].get("_bronze_source", "")
            if bronze_source and bronze_source != "reader":
                source_filename = bronze_source

            # Priority 2: source_uri (e.g., "s3://bucket/path/file.pdf" → "path_file_pdf").
            # Uses the full relative key (path + name + original extension,
            # sanitized), not just the basename with its extension
            # stripped — two source files that share a base name in
            # different subfolders, or the same name with different
            # extensions (report.pdf vs report.docx in the same folder),
            # previously both derived the identical output key and
            # silently overwrote each other on the same day.
            if not source_filename:
                source_uri = records[0].get("source_uri", "")
                if source_uri:
                    without_scheme = source_uri.split("://", 1)[-1]  # drop "s3://"
                    _, _, key = without_scheme.partition("/")  # drop "bucket/"
                    if key:
                        source_filename = key.strip("/").replace("/", "_").replace(".", "_")

            # Priority 3: name field from API config
            if not source_filename:
                name_field = records[0].get("_api_name", "")
                if name_field:
                    source_filename = name_field

        # Generate timestamp (date only, YYYYMMDD format)
        date_str = datetime.utcnow().strftime("%Y%m%d")

        # Build filename
        if source_filename:
            filename = f"{source_filename}_{date_str}.{file_extension}"
        else:
            # Fallback to timestamp if no source filename found
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"output_{timestamp}.{file_extension}"

        path = f"{prefix}{filename}"

        # Write to cloud provider
        if self.provider == "aws":
            self._write_to_s3(formatted_data, path)
        elif self.provider == "gcp":
            self._write_to_gcs(formatted_data, path)
        elif self.provider == "azure":
            self._write_to_azure_blob(formatted_data, path)

        log.info("CloudWriter: successfully wrote %d record(s) to %s/%s",
                 len(records), self.bucket, path)

    def _delete_existing_under_prefix(self, prefix: str) -> None:
        """
        Delete every existing object under `prefix` — the real
        implementation behind mode="overwrite". Scoped to exactly the
        prefix being written to right now (a single partition, or a single
        Medallion layer), never the whole bucket or the writer's base prefix.
        """
        if self.provider == "aws":
            self._delete_existing_s3(prefix)
        elif self.provider == "gcp":
            self._delete_existing_gcs(prefix)
        elif self.provider == "azure":
            self._delete_existing_azure(prefix)

    def _delete_existing_s3(self, prefix: str) -> None:
        s3 = self._get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        to_delete = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})

        if not to_delete:
            log.debug("CloudWriter: overwrite mode — nothing existing under s3://%s/%s",
                      self.bucket, prefix)
            return

        # S3's delete_objects caps at 1000 keys per call
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i:i + 1000]
            s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch})
        log.info("CloudWriter: overwrite mode — deleted %d existing object(s) under s3://%s/%s",
                  len(to_delete), self.bucket, prefix)

    def _delete_existing_gcs(self, prefix: str) -> None:
        from google.cloud import storage
        gcs_client = self._get_gcs_client()
        bucket = gcs_client.bucket(self.bucket)
        blobs = list(bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            blob.delete()
        log.info("CloudWriter: overwrite mode — deleted %d existing object(s) under gs://%s/%s",
                  len(blobs), self.bucket, prefix)

    def _delete_existing_azure(self, prefix: str) -> None:
        from azure.storage.blob import BlobServiceClient

        auth_cfg = self.target_config.get("auth", {})
        auth_type = auth_cfg.get("type", "connection_string").lower()

        # Mirrors _write_to_azure_blob's own auth handling exactly, so
        # overwrite mode works under whichever auth type the writer is
        # actually configured with, not just one of the two.
        if auth_type == "connection_string":
            connection_string = auth_cfg.get("connection_string")
            service_client = BlobServiceClient.from_connection_string(connection_string)
        elif auth_type == "managed_identity":
            from azure.identity import DefaultAzureCredential
            account_name = self.target_config.get("account_name")
            account_url = f"https://{account_name}.blob.core.windows.net"
            service_client = BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())
        else:
            raise ValueError(f"Unsupported Azure auth type: {auth_type}")

        container_client = service_client.get_container_client(self.bucket)
        blobs = list(container_client.list_blobs(name_starts_with=prefix))
        for blob in blobs:
            container_client.delete_blob(blob.name)
        log.info("CloudWriter: overwrite mode — deleted %d existing object(s) under azure://%s/%s",
                  len(blobs), self.bucket, prefix)
    
    # ── Format Conversion ─────────────────────────────────────────────────────
    
    def _format_records(self, records: List[Dict[str, Any]], 
                       file_format: str) -> bytes:
        """
        Format records for output.
        
        Takes records AS-IS and serializes them to the specified format.
        No transformation, no flattening, no special handling.
        Works with ANY record structure.
        
        Args:
            records: List of dictionaries (any structure)
            file_format: Output format (json, csv, parquet, xml, html, md, txt)
        
        Returns:
            Formatted data as bytes
        
        Raises:
            ValueError: If format is unsupported
        """
        file_format = file_format.lower()
        
        if file_format == "json":
            return self._format_json(records)
        elif file_format == "csv":
            return self._format_csv(records)
        elif file_format == "parquet":
            return self._format_parquet(records)
        elif file_format == "xml":
            return self._format_xml(records)
        elif file_format == "html":
            return self._format_html(records)
        elif file_format == "md":
            return self._format_markdown(records)
        elif file_format in ("txt", "text"):
            return self._format_text(records)
        else:
            raise ValueError(
                f"Unsupported format: {file_format}. "
                f"Supported: json, csv, parquet, xml, html, md, txt"
            )
    
    def _format_json(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as line-delimited JSON (NDJSON) when writing more than
        one record per file, since NDJSON's one-record-per-line contract
        requires each record to stay on a single line.

        A single-record file (e.g. one parsed document from the unstructured
        pipeline, which flushes after every write()) has no such constraint,
        so it's pretty-printed instead — the difference between a readable,
        indented document and one 150KB line is entirely for whoever opens
        the file afterward, not for anything downstream that parses it.
        """
        if len(records) == 1:
            serializable = self._make_serializable(records[0])
            json_str = json.dumps(serializable, indent=2, ensure_ascii=False)
            return json_str.encode("utf-8")

        lines = []
        for record in records:
            serializable = self._make_serializable(record)
            lines.append(json.dumps(serializable))

        json_str = "\n".join(lines)
        return json_str.encode("utf-8")
    
    def _format_csv(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as CSV.
        
        Works with ANY record structure.
        Uses first record's keys as headers.
        Handles nested structures by converting to JSON strings.
        """
        if not records:
            return b""
        
        # Get field names from first record
        fieldnames = list(records[0].keys())
        
        # Write to bytes buffer
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        
        # Write header
        writer.writeheader()
        
        # Write records
        for record in records:
            serializable = self._make_serializable(record)
            writer.writerow(serializable)
        
        return buffer.getvalue().encode("utf-8")
    
    def _format_parquet(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as Parquet.
        
        Works with ANY record structure.
        Requires pyarrow library.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError(
                "pyarrow is required for Parquet format. "
                "Install with: pip install pyarrow"
            )
        
        # Convert records to PyArrow table
        table = pa.Table.from_pylist(records)
        
        # Write to bytes buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        
        return buffer.getvalue()
    
    def _format_xml(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as XML.
        
        Works with ANY record structure.
        Each record becomes a <record> element.
        """
        xml_lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            "<records>",
        ]
        
        for i, record in enumerate(records):
            serializable = self._make_serializable(record)
            xml_lines.append(f"  <record id='{i}'>")
            
            for key, value in serializable.items():
                # Sanitize key for XML tag name
                tag_name = self._sanitize_xml_tag(key)
                escaped_value = self._escape_xml(str(value))
                xml_lines.append(f"    <{tag_name}>{escaped_value}</{tag_name}>")
            
            xml_lines.append("  </record>")
        
        xml_lines.append("</records>")
        
        xml_str = "\n".join(xml_lines)
        return xml_str.encode("utf-8")
    
    def _format_html(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as HTML table.
        
        Works with ANY record structure.
        Creates a table with one row per record.
        """
        if not records:
            return b"<table></table>"
        
        html_lines = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "<meta charset='UTF-8'>",
            "<style>",
            "table { border-collapse: collapse; width: 100%; }",
            "th, td { border: 1px solid black; padding: 8px; text-align: left; }",
            "th { background-color: #f2f2f2; }",
            "</style>",
            "</head>",
            "<body>",
            "<table>",
        ]
        
        # Get field names from first record
        fieldnames = list(records[0].keys())
        
        # Write header
        html_lines.append("<tr>")
        for field in fieldnames:
            html_lines.append(f"<th>{self._escape_html(str(field))}</th>")
        html_lines.append("</tr>")
        
        # Write rows
        for record in records:
            serializable = self._make_serializable(record)
            html_lines.append("<tr>")
            for field in fieldnames:
                value = serializable.get(field, "")
                html_lines.append(f"<td>{self._escape_html(str(value))}</td>")
            html_lines.append("</tr>")
        
        html_lines.extend([
            "</table>",
            "</body>",
            "</html>",
        ])
        
        html_str = "\n".join(html_lines)
        return html_str.encode("utf-8")
    
    def _format_markdown(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as Markdown table.
        
        Works with ANY record structure.
        Creates a markdown table with one row per record.
        """
        if not records:
            return b"| |\n|---|\n"
        
        md_lines = []
        
        # Get field names from first record
        fieldnames = list(records[0].keys())
        
        # Write header
        md_lines.append("| " + " | ".join(fieldnames) + " |")
        md_lines.append("|" + "|".join(["---"] * len(fieldnames)) + "|")
        
        # Write rows
        for record in records:
            serializable = self._make_serializable(record)
            row_values = []
            for field in fieldnames:
                value = serializable.get(field, "")
                # Escape pipe characters in values
                value_str = str(value).replace("|", "\\|")
                row_values.append(value_str)
            md_lines.append("| " + " | ".join(row_values) + " |")
        
        md_str = "\n".join(md_lines)
        return md_str.encode("utf-8")
    
    def _format_text(self, records: List[Dict[str, Any]]) -> bytes:
        """
        Format records as plain text.
        
        Works with ANY record structure.
        One record per section, key-value pairs per line.
        """
        text_lines = []
        
        for i, record in enumerate(records):
            serializable = self._make_serializable(record)
            text_lines.append(f"--- Record {i + 1} ---")
            
            for key, value in serializable.items():
                text_lines.append(f"{key}: {value}")
            
            text_lines.append("")  # Blank line between records
        
        text_str = "\n".join(text_lines)
        return text_str.encode("utf-8")
    
    # ── Utilities ─────────────────────────────────────────────────────────────
    
    def _make_serializable(self, obj: Any) -> Any:
        """
        Convert non-serializable objects to serializable format.
        
        Handles:
        - Dictionaries (recursive)
        - Lists and tuples (recursive)
        - Datetime objects (ISO format)
        - Other types (as-is)
        """
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
    
    def _get_file_extension(self, file_format: str) -> str:
        """Get file extension for format."""
        extensions = {
            "json": "json",
            "csv": "csv",
            "parquet": "parquet",
            "xml": "xml",
            "html": "html",
            "md": "md",
            "txt": "txt",
            "text": "txt",
        }
        return extensions.get(file_format.lower(), "json")
    
    def _get_content_type(self, file_format: str) -> str:
        """
        Get MIME content type for format.

        For "json", this must match _format_json()'s own branching: a
        single-record file (e.g. one parsed document from the unstructured
        pipeline) is pretty-printed, not line-delimited, so advertising it
        as application/x-ndjson is actively misleading — a consumer that
        trusts the content type and tries to parse it one line at a time
        will fail on the first line. Only genuine multi-record output
        (records joined one-JSON-per-line) is real NDJSON.
        """
        if file_format.lower() == "json":
            is_ndjson = len(self._buffer) > 1
            return "application/x-ndjson" if is_ndjson else "application/json"

        content_types = {
            "csv": "text/csv",
            "parquet": "application/octet-stream",
            "xml": "application/xml",
            "html": "text/html",
            "md": "text/markdown",
            "txt": "text/plain",
            "text": "text/plain",
        }
        return content_types.get(file_format.lower(), "application/octet-stream")
    
    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&#39;"))
    
    def _escape_xml(self, text: str) -> str:
        """Escape XML special characters."""
        return (text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&apos;"))
    
    def _sanitize_xml_tag(self, tag: str) -> str:
        """
        Sanitize string to be a valid XML tag name.
        XML tag names must start with letter or underscore.
        """
        # Replace invalid characters with underscore
        tag = tag.replace(" ", "_").replace("-", "_")
        # Remove any remaining invalid characters
        tag = "".join(c if c.isalnum() or c == "_" else "" for c in tag)
        # Ensure it starts with letter or underscore
        if tag and not (tag[0].isalpha() or tag[0] == "_"):
            tag = "_" + tag
        return tag or "field"
    
    # ── AWS S3 ────────────────────────────────────────────────────────────────
    
    def _write_to_s3(self, data: bytes, path: str) -> None:
        """
        Write data to AWS S3.
        
        Args:
            data: Data to write (bytes)
            path: S3 key (without s3:// prefix)
        """
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "boto3 is required for AWS S3. "
                "Install with: pip install boto3"
            )
        
        try:
            s3_client = self._get_s3_client()
            
            s3_client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=data,
                ContentType=self._get_content_type(self.file_format)
            )
            
            log.info("CloudWriter: wrote to S3 — s3://%s/%s", self.bucket, path)
        
        except Exception as exc:
            log.error("CloudWriter: S3 write failed — %s", exc)
            raise
    
    def _get_s3_client(self):
        """
        Get boto3 S3 client with configured authentication.
        
        Returns:
            boto3 S3 client
        """
        import boto3
        
        auth_cfg = self.target_config.get("auth", {})
        auth_type = auth_cfg.get("type", "iam_role").lower()
        
        if auth_type == "iam_role":
            # Use IAM role (default for AWS Glue)
            return boto3.client("s3", region_name=self.region)
        
        elif auth_type == "access_key":
            # Use access key credentials
            return boto3.client(
                "s3",
                region_name=self.region,
                aws_access_key_id=auth_cfg.get("access_key"),
                aws_secret_access_key=auth_cfg.get("secret_key")
            )
        
        elif auth_type == "secrets_manager":
            # Fetch credentials from AWS Secrets Manager
            import json
            secrets_client = boto3.client("secretsmanager", region_name=self.region)
            secret_name = auth_cfg.get("secret_name")
            
            try:
                response = secrets_client.get_secret_value(SecretId=secret_name)
                secret = json.loads(response["SecretString"])
                return boto3.client(
                    "s3",
                    region_name=self.region,
                    aws_access_key_id=secret.get("access_key"),
                    aws_secret_access_key=secret.get("secret_key")
                )
            except Exception as exc:
                log.error("CloudWriter: failed to fetch S3 credentials — %s", exc)
                raise
        
        else:
            raise ValueError(f"Unsupported S3 auth type: {auth_type}")
    
    # ── GCP Cloud Storage ─────────────────────────────────────────────────────
    
    def _write_to_gcs(self, data: bytes, path: str) -> None:
        """
        Write data to GCP Cloud Storage.
        
        Args:
            data: Data to write (bytes)
            path: GCS blob name
        """
        try:
            from google.cloud import storage
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required for GCP. "
                "Install with: pip install google-cloud-storage"
            )
        
        try:
            gcs_client = self._get_gcs_client()
            bucket = gcs_client.bucket(self.bucket)
            blob = bucket.blob(path)
            
            blob.upload_from_string(
                data,
                content_type=self._get_content_type(self.file_format)
            )
            
            log.info("CloudWriter: wrote to GCS — gs://%s/%s", self.bucket, path)
        
        except Exception as exc:
            log.error("CloudWriter: GCS write failed — %s", exc)
            raise
    
    def _get_gcs_client(self):
        """
        Get GCP Cloud Storage client with configured authentication.
        
        Returns:
            google.cloud.storage.Client
        """
        from google.cloud import storage
        
        auth_cfg = self.target_config.get("auth", {})
        auth_type = auth_cfg.get("type", "application_default").lower()
        project_id = auth_cfg.get("project_id")
        
        if auth_type == "application_default":
            # Use Application Default Credentials
            return storage.Client(project=project_id)
        
        elif auth_type == "service_account":
            # Use service account credentials
            from google.oauth2 import service_account
            
            credentials_path = auth_cfg.get("credentials_path")
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            return storage.Client(project=project_id, credentials=credentials)
        
        else:
            raise ValueError(f"Unsupported GCS auth type: {auth_type}")
    
    # ── Azure Blob Storage ────────────────────────────────────────────────────
    
    def _write_to_azure_blob(self, data: bytes, path: str) -> None:
        """
        Write data to Azure Blob Storage.
        
        Args:
            data: Data to write (bytes)
            path: Blob name
        """
        try:
            from azure.storage.blob import BlobClient
        except ImportError:
            raise ImportError(
                "azure-storage-blob is required for Azure. "
                "Install with: pip install azure-storage-blob"
            )
        
        try:
            auth_cfg = self.target_config.get("auth", {})
            auth_type = auth_cfg.get("type", "connection_string").lower()
            
            if auth_type == "connection_string":
                connection_string = auth_cfg.get("connection_string")
                blob_client = BlobClient.from_connection_string(
                    connection_string,
                    container_name=self.bucket,
                    blob_name=path
                )
            
            elif auth_type == "managed_identity":
                from azure.identity import DefaultAzureCredential
                
                account_name = self.target_config.get("account_name")
                account_url = f"https://{account_name}.blob.core.windows.net"
                blob_client = BlobClient(
                    account_url=account_url,
                    container_name=self.bucket,
                    blob_name=path,
                    credential=DefaultAzureCredential()
                )
            
            else:
                raise ValueError(f"Unsupported Azure auth type: {auth_type}")
            
            blob_client.upload_blob(
                data,
                overwrite=True,
                content_settings={"content_type": self._get_content_type(self.file_format)}
            )
            
            log.info("CloudWriter: wrote to Azure Blob — %s/%s", self.bucket, path)
        
        except Exception as exc:
            log.error("CloudWriter: Azure Blob write failed — %s", exc)
            raise


# ── Factory function ──────────────────────────────────────────────────────────

def create_cloud_writer(config: dict) -> CloudWriter:
    """
    Factory function to create a CloudWriter instance.
    
    Args:
        config: Full configuration from config.yaml
    
    Returns:
        Configured CloudWriter instance
    """
    return CloudWriter(config)
