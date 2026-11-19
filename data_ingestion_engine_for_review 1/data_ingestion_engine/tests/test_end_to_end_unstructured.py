"""
tests/test_end_to_end_unstructured.py
───────────────────────────────────────
End-to-end test for the unstructured pipeline:
  config (source/target) → GenericUnstructuredReader → ECSHttpParser
  → run_unstructured_pipeline's noise filtering → CloudWriter (S3 JSON)

The ECS /parse HTTP call is mocked (no real docling_service call); S3 is
mocked via moto.

Covers:
  - Format-agnostic reading: a file with an extension NOT on the old fixed
    allowlist (e.g. .rtf) must still be sent to the parser and processed.
  - A known non-document artifact (.gitkeep) must be skipped without an
    ECS call.
  - Retry: a transient ECS failure followed by success must still result
    in the file being processed, not permanently failed.
  - Output lands in S3 as JSON (the unstructured pipeline's hardcoded
    default format).
"""

import json
import logging
from unittest.mock import Mock, patch

import boto3
import pytest
from moto import mock_aws

log = logging.getLogger(__name__)


def _mock_parse_response(blocks, file_format="rtf"):
    # ecs_http_parser.py reads response.content.decode("utf-8") directly
    # (not response.json() — see its own comment on why), so the mock must
    # provide real UTF-8 JSON bytes on .content, not just a .json() lambda.
    payload = {
        "format": file_format,
        "blocks": blocks,
        "extraction_stats": {"raw_items_seen": len(blocks), "blocks_kept": len(blocks)},
    }
    return Mock(
        status_code=200,
        content=json.dumps(payload).encode("utf-8"),
        json=lambda: payload,
    )


def _build_config(source_bucket, target_bucket):
    return {
        "unstructured": {
            "source": {
                "bucket": source_bucket,
                "prefix": "",
                "region": "us-east-1",
            },
            "target": {
                "bucket": target_bucket,
                "output_prefix": "unstructured/",
                "region": "us-east-1",
                "batch_mode": False,
            },
        },
        "settings": {"retries": 2, "retry_delay_seconds": 0},
    }


@mock_aws
def test_end_to_end_unstructured_pipeline_is_format_agnostic():
    """
    A .rtf file (not on the old fixed allowlist) must be read and processed,
    and a .gitkeep artifact must be skipped without ever reaching the parser.
    """
    from pipelines.unstructured_pipeline import run_unstructured_pipeline

    source_bucket = "test-unstructured-source"
    target_bucket = "test-unstructured-target"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=source_bucket)
    s3.create_bucket(Bucket=target_bucket)

    # A format NOT on the old hardcoded allowlist, plus a common sync artifact
    # that should never be sent to the parser at all.
    s3.put_object(Bucket=source_bucket, Key="notes.rtf", Body=b"{\\rtf1 fake rtf body}")
    s3.put_object(Bucket=source_bucket, Key=".gitkeep", Body=b"")

    config = _build_config(source_bucket, target_bucket)

    blocks = [{"type": "text", "layout_type": "paragraph", "text": "Extracted from the rtf file."}]

    with patch("parser.ecs_http_parser.requests.post",
               return_value=_mock_parse_response(blocks)) as mock_post:
        run_unstructured_pipeline(config, ecs_endpoint="http://fake-ecs")

    # Exactly one call — for notes.rtf. The .gitkeep file must never reach
    # the parser at all (it's blocked at the reader, before any HTTP call).
    assert mock_post.call_count == 1, f"Expected exactly 1 ECS call, got {mock_post.call_count}"
    called_files = [c.kwargs["files"]["file"][0] for c in mock_post.call_args_list]
    assert called_files == ["notes.rtf"], f"Unexpected files sent to parser: {called_files}"

    # Validate the output actually landed in S3 as JSON.
    response = s3.list_objects_v2(Bucket=target_bucket, Prefix="unstructured/")
    objects = response.get("Contents", [])
    assert len(objects) == 1, f"Expected exactly 1 output file, got {len(objects)}"
    assert objects[0]["Key"].endswith(".json")

    content = s3.get_object(Bucket=target_bucket, Key=objects[0]["Key"])["Body"].read()
    record = json.loads(content.decode("utf-8").strip())
    assert record["content"]["blocks"][0]["text"] == "Extracted from the rtf file."
    assert record["content"]["extraction_stats"] == {"raw_items_seen": 1, "blocks_kept": 1}

    log.info("✓ .rtf file processed despite not being on the old allowlist; .gitkeep correctly skipped")


@mock_aws
def test_unstructured_pipeline_retries_transient_ecs_failure():
    """
    A transient ECS failure (503) followed by success must still result in
    the file being processed — not permanently failed on the first blip.
    """
    from pipelines.unstructured_pipeline import run_unstructured_pipeline

    source_bucket = "test-unstructured-retry-source"
    target_bucket = "test-unstructured-retry-target"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=source_bucket)
    s3.create_bucket(Bucket=target_bucket)

    s3.put_object(Bucket=source_bucket, Key="report.pdf", Body=b"%PDF-fake-bytes")

    config = _build_config(source_bucket, target_bucket)

    blocks = [{"type": "text", "layout_type": "paragraph", "text": "Recovered after retry."}]
    responses = [
        Mock(status_code=503, text="temporarily unavailable"),
        _mock_parse_response(blocks, file_format="pdf"),
    ]

    with patch("parser.ecs_http_parser.requests.post", side_effect=responses) as mock_post:
        run_unstructured_pipeline(config, ecs_endpoint="http://fake-ecs")

    assert mock_post.call_count == 2, f"Expected a retry (2 calls), got {mock_post.call_count}"

    response = s3.list_objects_v2(Bucket=target_bucket, Prefix="unstructured/")
    objects = response.get("Contents", [])
    assert len(objects) == 1
    content = s3.get_object(Bucket=target_bucket, Key=objects[0]["Key"])["Body"].read()
    record = json.loads(content.decode("utf-8").strip())
    assert record["content"]["blocks"][0]["text"] == "Recovered after retry."
    assert record.get("error") is None

    log.info("✓ Transient ECS failure retried and recovered, file processed successfully")
