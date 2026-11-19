"""
tests/test_end_to_end_structured.py
───────────────────────────────────
End-to-end test for structured pipeline:
  batch_s3_reader → medallion architecture → write_cloud

Test Flow:
1. Create test data in S3
2. Configure pipeline (batch_s3_reader → medallion → write_cloud)
3. Run pipeline
4. Validate output in S3
5. Verify data transformations (bronze → silver → gold)
"""

import pytest
import json
import logging
import boto3
from moto import mock_aws
from io import StringIO
import csv

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@mock_aws
def test_end_to_end_structured_pipeline():
    """
    Test complete structured pipeline:
    batch_s3_reader → medallion architecture → write_cloud
    """
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1: Setup mock S3 and create test data
    # ─────────────────────────────────────────────────────────────────────────
    
    # Create mock S3 buckets
    s3 = boto3.client("s3", region_name="us-east-1")
    source_bucket = "test-source-bucket"
    target_bucket = "test-target-bucket"
    
    s3.create_bucket(Bucket=source_bucket)
    s3.create_bucket(Bucket=target_bucket)
    
    log.info("✓ Created mock S3 buckets: %s, %s", source_bucket, target_bucket)
    
    # Create test CSV data
    test_data = [
        {"id": "1", "firstName": "Alice", "email": "alice@example.com", "age": "30"},
        {"id": "2", "firstName": "Bob", "email": "bob@example.com", "age": "25"},
        {"id": "3", "firstName": "Charlie", "email": "charlie@example.com", "age": "35"},
    ]
    
    # Write test CSV to source bucket
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["id", "firstName", "email", "age"])
    writer.writeheader()
    writer.writerows(test_data)
    
    s3.put_object(
        Bucket=source_bucket,
        Key="test_data.csv",
        Body=csv_buffer.getvalue().encode("utf-8")
    )
    
    log.info("✓ Created test CSV with %d records", len(test_data))
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2: Create test configuration
    # ─────────────────────────────────────────────────────────────────────────
    
    test_config = {
        "structured": {
            "batch": {
                "cloud": [
                    {
                        "name": "test-s3-ingestion",
                        "enabled": True,
                        "provider": "aws",
                        "source": {
                            "bucket": source_bucket,
                            "prefix": "",
                            "format": "csv",
                            "region": "us-east-1"
                        }
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
                "file_format": "json",
                "batch_mode": False,
                "auth": {
                    "type": "iam_role"
                }
            }
        },
        "pii_masking": {
            "enabled": False
        }
    }
    
    log.info("✓ Created test configuration")
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3: Import and instantiate pipeline components
    # ─────────────────────────────────────────────────────────────────────────
    
    from structured_readers.batch_s3_read import BatchCloudReader
    from architecture.medallion_architecture.pipeline import MedallionArchitecture
    from target.write_cloud import CloudWriter

    # Create reader
    reader = BatchCloudReader(test_config)
    log.info("✓ Created BatchCloudReader")

    # Create writer (must exist before architecture, which writes each layer through it)
    writer = CloudWriter(test_config)
    log.info("✓ Created CloudWriter")

    # Create architecture — pass the writer so each layer (bronze/silver/gold)
    # writes its own output, matching how PipelineBuilder wires this in production
    architecture = MedallionArchitecture(test_config, writers=[writer])
    log.info("✓ Created MedallionArchitecture")
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4: Execute pipeline
    # ─────────────────────────────────────────────────────────────────────────
    
    log.info("=" * 80)
    log.info("EXECUTING PIPELINE")
    log.info("=" * 80)
    
    # Read raw records
    raw_records = []
    with reader:
        for batch in reader.read():
            raw_records.extend(batch)
    
    log.info("✓ Read %d raw records from S3", len(raw_records))
    assert len(raw_records) == 3, f"Expected 3 records, got {len(raw_records)}"
    
    # Process through medallion architecture.
    # execute() writes bronze/silver/gold output itself (via the writer passed above)
    # and returns {"bronze": [...], "silver": [...], "gold": [...]} — NOT a flat list.
    layer_outputs = architecture.execute(raw_records)

    log.info("✓ Processed records through medallion architecture — bronze=%d silver=%d gold=%d",
              len(layer_outputs["bronze"]), len(layer_outputs["silver"]), len(layer_outputs["gold"]))
    assert len(layer_outputs["gold"]) == 3, f"Expected 3 gold records, got {len(layer_outputs['gold'])}"

    log.info("✓ Wrote records to target S3 (bronze/, silver/, gold/ prefixes)")
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 5: Validate output
    # ─────────────────────────────────────────────────────────────────────────
    
    log.info("=" * 80)
    log.info("VALIDATING OUTPUT")
    log.info("=" * 80)
    
    # Gold layer output carries the full history (bronze + silver + gold fields),
    # so validate against output/gold/ specifically rather than the shared "output/"
    # prefix (which also contains the separate bronze/ and silver/ layer files).
    response = s3.list_objects_v2(Bucket=target_bucket, Prefix="output/gold/")
    objects = response.get("Contents", [])

    log.info("✓ Found %d gold output files in target bucket", len(objects))
    assert len(objects) > 0, "No gold output files found in target bucket"

    # Read and validate output
    output_records = []
    for obj in objects:
        response = s3.get_object(Bucket=target_bucket, Key=obj["Key"])
        content = response["Body"].read().decode("utf-8")

        # Parse NDJSON (line-delimited JSON)
        for line in content.strip().split("\n"):
            if line:
                record = json.loads(line)
                output_records.append(record)

    log.info("✓ Read %d output records from target", len(output_records))
    assert len(output_records) == 3, f"Expected 3 output records, got {len(output_records)}"
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 6: Verify data transformations
    # ─────────────────────────────────────────────────────────────────────────
    
    log.info("=" * 80)
    log.info("VERIFYING DATA TRANSFORMATIONS")
    log.info("=" * 80)
    
    # Check first record (or any record if only one)
    assert len(output_records) > 0, "No output records found"
    first_record = output_records[0]
    
    # Verify bronze layer metadata
    assert "_bronze_ingestion_time" in first_record, "Missing bronze ingestion time"
    assert first_record["_bronze_layer"] == "bronze", "Missing bronze layer marker"
    log.info("✓ Bronze layer metadata present")
    
    # Verify silver layer metadata
    assert "_silver_normalization_time" in first_record, "Missing silver normalization time"
    assert first_record["_silver_layer"] == "silver", "Missing silver layer marker"
    log.info("✓ Silver layer metadata present")
    
    # Verify gold layer metadata
    assert "_gold_enrichment_time" in first_record, "Missing gold enrichment time"
    assert first_record["_gold_layer"] == "gold", "Missing gold layer marker"
    assert "_quality_score" in first_record, "Missing quality score"
    log.info("✓ Gold layer metadata present")
    
    # Verify field name normalization (camelCase → snake_case)
    assert "first_name" in first_record, "Field name not normalized (firstName → first_name)"
    log.info("✓ Field names normalized (camelCase → snake_case)")
    
    # Verify data preservation (check that data exists, not specific values since order may vary)
    assert "id" in first_record, "ID field missing"
    assert "first_name" in first_record, "first_name field missing"
    assert "email" in first_record, "email field missing"
    log.info("✓ Data preserved correctly through all layers")
    
    # ─────────────────────────────────────────────────────────────────────────
    # STEP 7: Print summary
    # ─────────────────────────────────────────────────────────────────────────
    
    log.info("=" * 80)
    log.info("TEST SUMMARY")
    log.info("=" * 80)
    log.info("✓ Source: %s/%s", source_bucket, "test_data.csv")
    log.info("✓ Records read: %d", len(raw_records))
    log.info("✓ Records processed: %d", len(layer_outputs["gold"]))
    log.info("✓ Records written: %d", len(output_records))
    log.info("✓ Target: %s/output/", target_bucket)
    log.info("✓ Transformations: Bronze → Silver → Gold")
    log.info("✓ All validations passed!")
    
    return True


@mock_aws
def test_pipeline_with_pii_masking():
    """
    Test structured pipeline with PII masking enabled.
    """
    
    log.info("=" * 80)
    log.info("TEST: Pipeline with PII Masking")
    log.info("=" * 80)
    
    # Setup
    s3 = boto3.client("s3", region_name="us-east-1")
    source_bucket = "test-source-pii"
    target_bucket = "test-target-pii"
    
    s3.create_bucket(Bucket=source_bucket)
    s3.create_bucket(Bucket=target_bucket)
    
    # Create test data with PII
    test_data = [
        {"id": "1", "name": "Alice", "email": "alice@example.com", "ssn": "123-45-6789"},
        {"id": "2", "name": "Bob", "email": "bob@example.com", "ssn": "987-65-4321"},
    ]
    
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["id", "name", "email", "ssn"])
    writer.writeheader()
    writer.writerows(test_data)
    
    s3.put_object(
        Bucket=source_bucket,
        Key="pii_data.csv",
        Body=csv_buffer.getvalue().encode("utf-8")
    )
    
    # Configuration with PII masking enabled
    test_config = {
        "structured": {
            "batch": {
                "cloud": [
                    {
                        "name": "test-s3-pii",
                        "enabled": True,
                        "provider": "aws",
                        "source": {
                            "bucket": source_bucket,
                            "prefix": "",
                            "format": "csv",
                            "region": "us-east-1"
                        }
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
                "file_format": "json",
                "batch_mode": False,
                "auth": {"type": "iam_role"}
            }
        },
        "pii_masking": {
            "enabled": True,
            "strategy": "sha256",
            "sensitive_columns": ["email", "ssn"]
        }
    }
    
    # Execute pipeline
    from structured_readers.batch_s3_read import BatchCloudReader
    from architecture.medallion_architecture.pipeline import MedallionArchitecture
    from target.write_cloud import CloudWriter
    
    reader = BatchCloudReader(test_config)
    writer = CloudWriter(test_config)
    architecture = MedallionArchitecture(test_config, writers=[writer])

    raw_records = []
    with reader:
        for batch in reader.read():
            raw_records.extend(batch)

    architecture.execute(raw_records)

    # Validate PII masking — read from gold/ where the fully-processed records land
    response = s3.list_objects_v2(Bucket=target_bucket, Prefix="output/gold/")
    objects = response.get("Contents", [])

    output_records = []
    for obj in objects:
        response = s3.get_object(Bucket=target_bucket, Key=obj["Key"])
        content = response["Body"].read().decode("utf-8")
        for line in content.strip().split("\n"):
            if line:
                output_records.append(json.loads(line))
    
    # Check that PII is masked
    assert len(output_records) > 0, "No output records found"
    first_record = output_records[0]
    assert first_record["_silver_pii_masked"] == True, "PII masking not applied"
    assert "HASH_" in first_record["email"], "Email not masked"
    assert "HASH_" in first_record["ssn"], "SSN not masked"
    # Check that non-sensitive data is preserved (name should be present and not masked)
    assert "name" in first_record, "Name field missing"
    assert first_record["name"] in ["Alice", "Bob"], "Name should be preserved"
    
    log.info("✓ PII masking test passed!")
    log.info("  - Email masked: %s", first_record["email"])
    log.info("  - SSN masked: %s", first_record["ssn"])
    log.info("  - Name preserved: %s", first_record["name"])
    
    return True


if __name__ == "__main__":
    log.info("Running end-to-end structured pipeline tests...")
    
    try:
        test_end_to_end_structured_pipeline()
        log.info("\n✓ Test 1 PASSED: End-to-end structured pipeline\n")
    except Exception as e:
        log.error("\n✗ Test 1 FAILED: %s\n", e, exc_info=True)
        raise
    
    try:
        test_pipeline_with_pii_masking()
        log.info("\n✓ Test 2 PASSED: Pipeline with PII masking\n")
    except Exception as e:
        log.error("\n✗ Test 2 FAILED: %s\n", e, exc_info=True)
        raise
    
    log.info("=" * 80)
    log.info("ALL TESTS PASSED!")
    log.info("=" * 80)
