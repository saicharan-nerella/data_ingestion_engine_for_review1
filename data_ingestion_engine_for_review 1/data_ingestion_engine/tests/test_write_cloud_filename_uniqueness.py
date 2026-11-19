"""
tests/test_write_cloud_filename_uniqueness.py
───────────────────────────────────────────────
Regression test for CloudWriter's output filename derivation
(target/write_cloud.py, _flush_buffer's source_uri-based naming).

Two source files that share a base name — in different subfolders, or the
same name with different extensions — must never derive the same output
key, since batch_mode=False writers (e.g. the unstructured pipeline, one
file per source document) flush each write immediately: a collision means
silent data loss (the second write overwrites the first).
"""

import logging

import boto3
from moto import mock_aws

log = logging.getLogger(__name__)


def _write_one(config, source_uri, text):
    from target.write_cloud import CloudWriter

    writer = CloudWriter(config)
    writer.write({"source_uri": source_uri, "text": text})
    writer.flush()


@mock_aws
def test_same_basename_different_extension_no_collision():
    """report.pdf and report.docx in the same folder must not collide."""
    bucket = "test-collision-ext-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)

    config = {
        "provider": "aws", "bucket": bucket, "prefix": "output/",
        "region": "us-east-1", "file_format": "json", "batch_mode": False,
    }

    _write_one(config, "s3://src/docs/report.pdf", "from the pdf")
    _write_one(config, "s3://src/docs/report.docx", "from the docx")

    objects = s3.list_objects_v2(Bucket=bucket, Prefix="output/").get("Contents", [])
    keys = [o["Key"] for o in objects]
    assert len(keys) == 2, f"Expected 2 distinct output files, got {len(keys)}: {keys}"
    assert len(set(keys)) == 2, f"Collision — duplicate output key: {keys}"


@mock_aws
def test_same_name_different_subfolder_no_collision():
    """notes.txt in folder1 and notes.txt in folder2 must not collide."""
    bucket = "test-collision-folder-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)

    config = {
        "provider": "aws", "bucket": bucket, "prefix": "output/",
        "region": "us-east-1", "file_format": "json", "batch_mode": False,
    }

    _write_one(config, "s3://src/folder1/notes.txt", "folder1 notes")
    _write_one(config, "s3://src/folder2/notes.txt", "folder2 notes")

    objects = s3.list_objects_v2(Bucket=bucket, Prefix="output/").get("Contents", [])
    keys = [o["Key"] for o in objects]
    assert len(keys) == 2, f"Expected 2 distinct output files, got {len(keys)}: {keys}"
    assert len(set(keys)) == 2, f"Collision — duplicate output key: {keys}"


@mock_aws
def test_flat_no_subfolder_still_works():
    """A simple flat source_uri (no subfolder) still produces a sensible filename."""
    bucket = "test-flat-bucket"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket)

    config = {
        "provider": "aws", "bucket": bucket, "prefix": "output/",
        "region": "us-east-1", "file_format": "json", "batch_mode": False,
    }

    _write_one(config, "s3://src/report.pdf", "flat file")

    objects = s3.list_objects_v2(Bucket=bucket, Prefix="output/").get("Contents", [])
    assert len(objects) == 1
    assert "report" in objects[0]["Key"]
