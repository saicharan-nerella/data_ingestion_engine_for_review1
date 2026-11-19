"""
tests/test_end_to_end_batch_database.py
─────────────────────────────────────────
End-to-end test for the structured pipeline via the database reader:
  batch_database_read (BatchDatabaseReader) → medallion architecture (PII
  masking enabled in the Silver layer) → write_cloud (CloudWriter)

The database connection itself is mocked out to a real in-memory SQLite
engine (create_engine is patched) so the test exercises the reader's actual
query/row-mapping logic without requiring a live Postgres/MySQL instance;
S3 is mocked via moto.

Covers:
  - Every CloudWriter file_format (json, csv, parquet, xml, html, md, txt),
    matching the coverage already done for batch_api.
  - PII masking on vs off (config-gated).
  - Reading multiple tables in one config (as the real config.yaml pagila
    setup does with 10 tables), including what happens when two different
    tables both use small auto-increment ids that collide across tables.
"""

import csv as csv_module
import json
import logging
from io import StringIO
from unittest.mock import patch

import boto3
import pytest
import sqlalchemy as sa
from moto import mock_aws
from sqlalchemy import text

log = logging.getLogger(__name__)

FILE_FORMATS = ["json", "csv", "parquet", "xml", "html", "md", "txt"]

EXPECTED_EXTENSION = {
    "json": "json", "csv": "csv", "parquet": "parquet",
    "xml": "xml", "html": "html", "md": "md", "txt": "txt",
}


def _build_test_config(target_bucket, file_format="json", tables=None):
    return {
        "structured": {
            "batch": {
                "databases": [
                    {
                        "name": "test-database",
                        "enabled": True,
                        "type": "postgres",
                        "source": {
                            "host": "test-db-host",
                            "port": 5432,
                            "database": "testdb",
                            "username": "testuser",
                            "password": "testpass",
                            "tables": tables or ["customer"],
                        },
                        "load": {
                            "mode": "full",
                            "batch_size": 10000,
                        },
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
            "sensitive_columns": ["email", "ssn", "last_name"],
        },
    }


def _seeded_sqlite_engine(with_actor_table=False):
    """A real in-memory SQLite engine, seeded like the pagila 'customer' (+ optional 'actor') table."""
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE customer (id INTEGER, firstName TEXT, email TEXT, ssn TEXT)"
        ))
        conn.execute(text(
            "INSERT INTO customer VALUES (1, 'Alice', 'alice@example.com', '123-45-6789')"
        ))
        conn.execute(text(
            "INSERT INTO customer VALUES (2, 'Bob', 'bob@example.com', '987-65-4321')"
        ))
        conn.execute(text(
            "INSERT INTO customer VALUES (3, 'Charlie', 'charlie@example.com', '456-78-9012')"
        ))

        if with_actor_table:
            # Deliberately reuses ids 1 and 2 — like pagila, every table has its
            # own auto-increment PK starting at 1, so id collisions ACROSS
            # tables are the realistic case, not an edge case.
            conn.execute(text(
                "CREATE TABLE actor (id INTEGER, firstName TEXT, lastName TEXT)"
            ))
            conn.execute(text(
                "INSERT INTO actor VALUES (1, 'Tom', 'Hanks')"
            ))
            conn.execute(text(
                "INSERT INTO actor VALUES (2, 'Meryl', 'Streep')"
            ))
    return engine


def _run_reader(test_config, fake_engine):
    from structured_readers.batch_database_read import BatchDatabaseReader

    reader = BatchDatabaseReader(test_config)
    with patch("structured_readers.batch_database_read.create_engine", return_value=fake_engine):
        raw_records = []
        with reader:
            for batch in reader.read():
                raw_records.extend(batch)
    return raw_records


@mock_aws
@pytest.mark.parametrize("file_format", FILE_FORMATS)
def test_end_to_end_batch_database_pipeline(file_format):
    """
    batch_database reader -> medallion architecture (PII masking in Silver) -> write_cloud,
    exercised once per supported file_format.
    """
    from architecture.medallion_architecture.pipeline import MedallionArchitecture
    from target.write_cloud import CloudWriter

    target_bucket = "test-target-db-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=target_bucket)

    test_config = _build_test_config(target_bucket, file_format=file_format)

    # ── STEP 1: Read from the (mocked) database ────────────────────────────
    raw_records = _run_reader(test_config, _seeded_sqlite_engine())
    assert len(raw_records) == 3, f"Expected 3 raw records, got {len(raw_records)}"
    log.info("✓ [%s] Read %d raw records from mocked database", file_format, len(raw_records))

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

    if file_format == "json":
        records = [json.loads(line) for line in content.decode("utf-8").strip().split("\n") if line]
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
        assert rows[0]["email"].startswith("HASH_")
        assert rows[0]["ssn"].startswith("HASH_")

    log.info("✓ [%s] End-to-end batch_database pipeline test passed", file_format)


@mock_aws
def test_batch_database_pii_masking_disabled():
    """
    Same pipeline, but with pii_masking.enabled=False — email/ssn must pass through untouched.
    """
    from architecture.medallion_architecture.pipeline import MedallionArchitecture
    from target.write_cloud import CloudWriter

    target_bucket = "test-target-db-nomask-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=target_bucket)

    test_config = _build_test_config(target_bucket)
    test_config["pii_masking"]["enabled"] = False

    raw_records = _run_reader(test_config, _seeded_sqlite_engine())

    writer = CloudWriter(test_config)
    architecture = MedallionArchitecture(test_config, writers=[writer])
    architecture.execute(raw_records)

    response = s3.list_objects_v2(Bucket=target_bucket, Prefix="output/gold/")
    objects = response.get("Contents", [])
    content = s3.get_object(Bucket=target_bucket, Key=objects[0]["Key"])["Body"].read()
    records = [json.loads(line) for line in content.decode("utf-8").strip().split("\n") if line]

    first = records[0]
    assert first["_silver_pii_masked"] is False
    assert first["email"] == "alice@example.com", "email must NOT be masked when pii_masking.enabled=False"
    assert first["ssn"] == "123-45-6789", "ssn must NOT be masked when pii_masking.enabled=False"

    log.info("✓ Confirmed PII masking is skipped when pii_masking.enabled=False")


@mock_aws
def test_batch_database_multi_table_read():
    """
    Reads TWO tables in one config (customer + actor), as the real config.yaml
    pagila setup does across 10 tables. customer has a genuine intra-table
    duplicate (id=1 appears twice), and actor reuses ids 1-2 — colliding with
    customer's ids — matching how every independently auto-incremented table
    in a real schema behaves.

    This exercises the fix in silver_layer.py: dedup is now namespaced by
    _bronze_source (the table name, tagged via _source_table by
    BatchDatabaseReader), so:
      - customer's genuine duplicate (same table, same id) is still dropped.
      - actor's id=1/id=2 are NOT dropped just because they collide with
        customer's id=1/id=2 — different table, different record.
    """
    from architecture.medallion_architecture.pipeline import MedallionArchitecture
    from target.write_cloud import CloudWriter

    target_bucket = "test-target-db-multitable-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=target_bucket)

    test_config = _build_test_config(target_bucket, tables=["customer", "actor"])
    engine = _seeded_sqlite_engine(with_actor_table=True)
    with engine.begin() as conn:
        # Genuine intra-table duplicate — same table, same id, should still dedup.
        conn.execute(text(
            "INSERT INTO customer VALUES (1, 'Alice', 'alice@example.com', '123-45-6789')"
        ))

    raw_records = _run_reader(test_config, engine)

    # The reader itself has no dedup logic — it must yield every row from both tables.
    assert len(raw_records) == 6, (
        f"Reader should yield all rows from both tables (4 customer incl. dup + 2 actor), "
        f"got {len(raw_records)}"
    )

    writer = CloudWriter(test_config)
    architecture = MedallionArchitecture(test_config, writers=[writer])
    layer_outputs = architecture.execute(raw_records)

    # Bronze does no dedup — should still be 6.
    assert len(layer_outputs["bronze"]) == 6

    # Silver: the genuine customer id=1 duplicate is dropped (6 - 1 = 5), but
    # actor id=1/id=2 survive despite colliding with customer's id=1/id=2,
    # because dedup is now namespaced by source table.
    assert len(layer_outputs["silver"]) == 5, (
        f"Expected 5 (6 raw - 1 genuine intra-table duplicate), got "
        f"{len(layer_outputs['silver'])}. If this is 4, cross-table records "
        f"are still colliding — the source-namespaced dedup fix regressed."
    )

    gold_sources = sorted(r.get("_bronze_source") for r in layer_outputs["gold"])
    assert gold_sources == ["actor", "actor", "customer", "customer", "customer"], (
        f"Expected 2 actor + 3 distinct customer records, got sources: {gold_sources}"
    )

    log.info("✓ Multi-table read: genuine duplicate deduped, cross-table id collision preserved")
