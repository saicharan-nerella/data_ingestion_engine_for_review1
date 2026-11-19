"""
tests/test_end_to_end_batch_api.py
───────────────────────────────────
End-to-end test for the structured pipeline via the REST API reader:
  batch_api_read (BatchAPIReader) → medallion architecture (PII masking
  enabled in the Silver layer) → write_cloud (CloudWriter)

Runs once per CloudWriter-supported file_format (json, csv, parquet, xml,
html, md, txt) to confirm the pipeline is format-agnostic end-to-end, as
documented in target/write_cloud.py.

The API call itself is mocked (no real HTTP request); S3 is mocked via moto.
"""

import csv as csv_module
import json
import logging
from io import StringIO
from unittest.mock import Mock, patch

import boto3
import pytest
from moto import mock_aws

log = logging.getLogger(__name__)

FILE_FORMATS = ["json", "csv", "parquet", "xml", "html", "md", "txt"]

EXPECTED_EXTENSION = {
    "json": "json", "csv": "csv", "parquet": "parquet",
    "xml": "xml", "html": "html", "md": "md", "txt": "txt",
}


def _api_records():
    """Fresh copy each call — BatchAPIReader tags these dicts in place (_api_name)."""
    return [
        {"id": "1", "firstName": "Alice",   "email": "alice@example.com",   "ssn": "123-45-6789", "age": 30},
        {"id": "2", "firstName": "Bob",     "email": "bob@example.com",     "ssn": "987-65-4321", "age": 25},
        {"id": "3", "firstName": "Charlie", "email": "charlie@example.com", "ssn": "456-78-9012", "age": 35},
    ]


def _mock_api_response(records):
    response = Mock()
    response.raise_for_status = Mock()
    response.json = Mock(return_value=records)
    return response


def _build_test_config(target_bucket, file_format):
    return {
        "structured": {
            "batch": {
                "apis": [
                    {
                        "name": "test-users-api",
                        "enabled": True,
                        "method": "GET",
                        "source": {
                            "url": "https://api.example.com/v1/users",
                            "headers": {},
                            "auth": {"type": "none"},
                            "pagination": {"type": "none"},
                        },
                        "load": {"mode": "full"},
                    }
                ]
            }
        },
        "target_config": {
            "write_cloud": {
                "provider": "aws",
                "bucket": target_bucket,
                "prefix": "output/",
                "region": "us-east-1",
                "file_format": file_format,
                "batch_mode": True,
                "auth": {"type": "iam_role"},
            }
        },
        "pii_masking": {
            "enabled": True,
            "strategy": "sha256",
            "sensitive_columns": ["email", "ssn"],
        },
        "settings": {"retries": 0, "retry_delay_seconds": 0},
    }


@mock_aws
@pytest.mark.parametrize("file_format", FILE_FORMATS)
def test_end_to_end_batch_api_pipeline(file_format):
    """
    batch_api reader -> medallion architecture (PII masking in Silver) -> write_cloud,
    exercised once per supported file_format.
    """
    from architecture.medallion_architecture.pipeline import MedallionArchitecture
    from structured_readers.batch_api_read import BatchAPIReader
    from target.write_cloud import CloudWriter

    target_bucket = "test-target-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=target_bucket)

    test_config = _build_test_config(target_bucket, file_format)

    # ── STEP 1: Read from the (mocked) API ────────────────────────────────
    reader = BatchAPIReader(test_config)

    with patch("structured_readers.batch_api_read.requests.get",
               return_value=_mock_api_response(_api_records())) as mock_get:
        raw_records = []
        with reader:
            for batch in reader.read():
                raw_records.extend(batch)

    assert mock_get.called, "BatchAPIReader did not call requests.get"
    assert len(raw_records) == 3, f"Expected 3 raw records, got {len(raw_records)}"
    assert raw_records[0]["_api_name"] == "test-users-api"
    log.info("✓ [%s] Read %d raw records from mocked API", file_format, len(raw_records))

    # ── STEP 2: Process through medallion architecture + write via CloudWriter ──
    writer = CloudWriter(test_config)
    architecture = MedallionArchitecture(test_config, writers=[writer])

    layer_outputs = architecture.execute(raw_records)
    assert len(layer_outputs["bronze"]) == 3
    assert len(layer_outputs["silver"]) == 3
    assert len(layer_outputs["gold"]) == 3
    log.info("✓ [%s] Processed through bronze/silver/gold", file_format)

    # ── STEP 3: Validate the gold-layer output written to (mocked) S3 ─────
    response = s3.list_objects_v2(Bucket=target_bucket, Prefix="output/gold/")
    objects = response.get("Contents", [])
    assert len(objects) == 1, f"Expected exactly 1 gold output file, got {len(objects)}"

    obj_key = objects[0]["Key"]
    assert obj_key.endswith(f".{EXPECTED_EXTENSION[file_format]}"), (
        f"Unexpected file extension for format={file_format}: {obj_key}"
    )

    content = s3.get_object(Bucket=target_bucket, Key=obj_key)["Body"].read()
    assert len(content) > 0, f"Empty output file written for format={file_format}"
    log.info("✓ [%s] Gold output written to s3://%s/%s (%d bytes)",
              file_format, target_bucket, obj_key, len(content))

    # ── STEP 4: Where the format is parseable, verify data + PII masking ──
    if file_format == "json":
        lines = content.decode("utf-8").strip().split("\n")
        records = [json.loads(line) for line in lines if line]
        assert len(records) == 3
        first = records[0]
        assert first["_bronze_layer"] == "bronze"
        assert first["_silver_layer"] == "silver"
        assert first["_gold_layer"] == "gold"
        assert first["_silver_pii_masked"] is True
        assert "first_name" in first, "camelCase -> snake_case normalization failed"
        assert first["email"].startswith("HASH_"), "email should be PII-masked"
        assert first["ssn"].startswith("HASH_"), "ssn should be PII-masked"

    elif file_format == "csv":
        rows = list(csv_module.DictReader(StringIO(content.decode("utf-8"))))
        assert len(rows) == 3
        assert rows[0]["email"].startswith("HASH_"), "email should be PII-masked"
        assert rows[0]["ssn"].startswith("HASH_"), "ssn should be PII-masked"

    log.info("✓ [%s] End-to-end batch_api pipeline test passed", file_format)
