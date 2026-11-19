"""
architecture/lambda_architecture/record_keys.py
──────────────────────────────────────────────────
Shared record-identity helpers for the Lambda architecture.

The Batch and Speed layers tag records independently and on different
threads, but the Serving layer still needs to join their outputs on the
same key. Centralizing "what is this record's identity" here keeps that
join honest — if Batch and Speed each computed keys their own way, records
that are really the same entity could silently end up under different
keys and never merge.

The actual id-field detection lives in core/record_identity.py, shared
with Medallion's Silver layer — this module only adds the source
namespacing on top (two different tables/APIs whose auto-increment ids
both start at 1 shouldn't be treated as the same entity).
"""

from typing import Any, Dict, Optional, Tuple

from core.record_identity import get_record_id


def extract_source(record: Dict[str, Any]) -> str:
    """
    Pop the source label a reader tagged this record with (API name for
    batch_api_read, table name for batch_database_read), defaulting to
    "reader" for sources that don't tag themselves (streaming readers,
    cloud file readers).

    Mutates `record` by removing the raw tag — callers merge the returned
    source into their own metadata field instead.
    """
    return record.pop("_api_name", None) or record.pop("_source_table", None) or "reader"


def get_record_key(record: Dict[str, Any], source: str) -> Optional[Tuple[str, Any]]:
    """
    Find a natural/primary key value on a record, namespaced by `source` so
    that two different tables/APIs whose auto-increment ids both start at 1
    aren't treated as the same entity.

    Returns:
        (source, id_value), or None if no id-like field was found
    """
    record_id = get_record_id(record)
    if record_id is None:
        return None
    return (source, record_id)