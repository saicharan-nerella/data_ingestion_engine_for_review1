"""
architecture/lambda_architecture/batch_layer.py
──────────────────────────────────────────────────
Batch Layer: Full Recompute Over All Historical Raw Data

Responsibility:
- Treat every record read from the batch readers as ground truth
- Tag each record with source/ingestion metadata
- Normalize blanks to real None (core/null_handling.py) — a reader's ""
  is never left indistinguishable from a genuine empty string
- Mask PII (optional, config-driven — same pii_masking config and shared
  PIIMasker Medallion's Silver layer uses, so the setting behaves
  identically regardless of --architecture)
- Collapse duplicate keys — the last record for a given key wins, so a
  full recompute always converges to one authoritative row per key no
  matter how many times that key reappeared across sources/batches. Key
  detection (core/record_identity.py) recognizes both "employee_id" and
  "EmployeeID" style columns, not just underscore-delimited ones.
- Records with no id-like field at all can't be deduplicated — they pass
  through untouched rather than being silently dropped

This output is the "batch view": slow to produce, always correct. The
Speed layer's incremental view gets reconciled against it every time this
runs.

Data Flow:
    raw_records (all history) → Batch Layer (tag + normalize + mask + dedupe) → batch_view
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from architecture.lambda_architecture.record_keys import extract_source, get_record_key
from core.null_handling import normalize_nulls
from core.pii_masking import PIIMasker

log = logging.getLogger(__name__)


class BatchLayer:
    """
    Batch layer: full recompute from all raw records available to the
    batch readers. Produces the authoritative "batch view" that the
    Serving layer falls back to for any key the Speed layer hasn't seen
    recently.
    """

    def __init__(self, config: dict):
        self.config = config
        self.pii_masker = PIIMasker(config)
        log.info("BatchLayer: initialized")

    def process(self, raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Recompute the batch view from scratch.

        Args:
            raw_records: All records read from the batch readers this run

        Returns:
            One tagged record per key — later records in the input win, so
            duplicate keys collapse to their most recent value. Records
            with no id-like field are appended as-is.
        """
        log.info("BatchLayer: recomputing from %d raw record(s)", len(raw_records))

        batch_time = datetime.utcnow().isoformat()
        by_key: Dict[Any, Dict[str, Any]] = {}
        unkeyed: List[Dict[str, Any]] = []

        for idx, record in enumerate(raw_records):
            source = extract_source(record)
            tagged = {
                "_lambda_layer": "batch",
                "_batch_view_time": batch_time,
                "_batch_source": source,
                **record,
            }

            # Normalize blanks to real None, then mask, then compute the
            # dedup key — same order Silver uses. mask() no-ops when
            # pii_masking.enabled is false, so no branch is needed for it.
            tagged = normalize_nulls(tagged)
            tagged = self.pii_masker.mask(tagged)
            tagged["_batch_pii_masked"] = self.pii_masker.enabled

            key = get_record_key(tagged, source)
            if key is None:
                tagged["_batch_record_index"] = idx
                unkeyed.append(tagged)
            else:
                by_key[key] = tagged

        batch_view = list(by_key.values()) + unkeyed
        log.info(
            "BatchLayer: batch view has %d record(s) (%d keyed, %d unkeyed, %d duplicate(s) collapsed)",
            len(batch_view), len(by_key), len(unkeyed),
            len(raw_records) - len(unkeyed) - len(by_key),
        )
        return batch_view
