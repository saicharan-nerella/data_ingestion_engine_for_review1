"""
architecture/lambda_architecture/serving_layer.py
──────────────────────────────────────────────────
Serving Layer: Merge Batch View + Speed View

Responsibility:
- Answer queries by combining the accurate-but-stale batch view with the
  fast-but-approximate speed view
- For any key present in both, the Speed layer wins — it reflects events
  that arrived after the last Batch recompute
- Keys the Speed layer hasn't seen fall back to the Batch layer untouched
- Unkeyed records from either layer pass through as-is — there's no key
  to merge on, so dropping them would silently lose data

No PII masking happens here, deliberately: every record arriving at merge()
already passed through Batch's or Speed's own PIIMasker, so masking here
too would double-process already-masked values (re-hashing a hash,
re-redacting a redaction marker). Masking is each source layer's own
responsibility, not the merge step's.

Data Flow:
    batch_view + speed_view → Serving Layer (merge, speed wins on overlap) → serving_view
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

from architecture.lambda_architecture.record_keys import get_record_key

log = logging.getLogger(__name__)


class ServingLayer:
    """
    Serving layer: merges the Batch and Speed views into one queryable
    result, preferring the Speed layer's fresher data on overlapping keys.
    """

    def __init__(self, config: dict):
        self.config = config
        log.info("ServingLayer: initialized")

    def merge(self, batch_records: List[Dict[str, Any]],
              speed_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge the batch and speed views.

        Args:
            batch_records: Output of BatchLayer.process()
            speed_records: Output of SpeedLayer.snapshot()

        Returns:
            One record per key — the Speed layer's version if it has one,
            else the Batch layer's version. Unkeyed records from both
            layers are appended untouched.
        """
        merge_time = datetime.utcnow().isoformat()
        merged: Dict[Any, Dict[str, Any]] = {}
        unkeyed: List[Dict[str, Any]] = []

        for record in batch_records:
            key = get_record_key(record, record.get("_batch_source", "reader"))
            tagged = {**record, "_serving_source_layer": "batch"}
            if key is None:
                unkeyed.append(tagged)
            else:
                merged[key] = tagged

        speed_overrides = 0
        for record in speed_records:
            key = get_record_key(record, record.get("_speed_source", "reader"))
            tagged = {**record, "_serving_source_layer": "speed"}
            if key is None:
                unkeyed.append(tagged)
            else:
                if key in merged:
                    speed_overrides += 1
                merged[key] = tagged

        serving_view = list(merged.values()) + unkeyed
        for record in serving_view:
            record["_serving_merged_time"] = merge_time

        log.info(
            "ServingLayer: merged %d batch + %d speed record(s) -> %d served "
            "(%d key(s) overridden by speed layer, %d unkeyed)",
            len(batch_records), len(speed_records), len(serving_view),
            speed_overrides, len(unkeyed),
        )
        return serving_view
