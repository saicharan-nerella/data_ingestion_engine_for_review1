"""
architecture/medallion_architecture/silver_layer.py
────────────────────────────────────────────────────
Silver Layer: Data Cleaning, Normalization, Validation, and PII Masking

Responsibility:
- Flatten nested objects
- Normalize field names (camelCase → snake_case)
- Handle null/missing values
- Validate and coerce data types
- Deduplicate records
- Mask PII (optional, config-driven)
- Add quality metrics

Data Flow:
    Bronze Records → Silver Layer (clean + normalize + validate + mask) → Gold Layer

Key Points:
- All data cleaning happens in this layer
- PII masking is optional and controlled by config.yaml
- Applies consistent transformations across all data sources
- Tracks validation errors and metadata
- Adds quality metrics for tracking
"""

import logging
import re
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from core.pii_masking import PIIMasker
from core.record_identity import get_record_id

log = logging.getLogger(__name__)


class SilverLayer:
    """
    Silver layer: Comprehensive data cleaning, normalization, validation, and PII masking.
    
    Process:
    1. Flatten nested objects
    2. Normalize field names (camelCase → snake_case)
    3. Handle null/missing values
    4. Validate and coerce data types
    5. Deduplicate records
    6. Mask PII (optional, config-driven)
    7. Add quality metrics and metadata
    
    All transformations are controlled by config.yaml — no code changes needed.
    """
    
    def __init__(self, config: dict):
        """
        Initialize the Silver layer.

        Args:
            config: Full configuration from config.yaml
        """
        self.config = config

        # Shared masking implementation — see core/pii_masking.py. Using the
        # same PIIMasker Lambda's Batch/Speed layers use is what keeps
        # pii_masking.enabled/strategy/sensitive_columns behaving identically
        # regardless of --architecture, instead of this layer maintaining its
        # own independent copy of the column-matching and masking logic.
        self.pii_masker = PIIMasker(config)

        log.info("SilverLayer: initialized")
        log.info("  - PII Masking Enabled: %s", self.pii_masker.enabled)
        if self.pii_masker.enabled:
            log.info("  - PII Masking Strategy: %s", self.pii_masker.strategy)
            log.info("  - Sensitive Columns: %d", len(self.pii_masker.sensitive_columns))

    def process(self, bronze_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Execute the Silver layer: clean, normalize, validate, and mask records.
        
        Args:
            bronze_records: Raw records from Bronze layer
        
        Returns:
            List of cleaned and validated records
        """
        try:
            log.info("SilverLayer: processing %d records", len(bronze_records))
            
            silver_records = []
            validation_errors = []
            seen_ids = set()  # For deduplication
            
            for idx, record in enumerate(bronze_records):
                try:
                    # Step 1: Flatten nested objects
                    flattened = self._flatten_nested_objects(record)
                    
                    # Step 2: Normalize field names (camelCase → snake_case)
                    normalized = self._normalize_field_names(flattened)
                    
                    # Step 3: Handle null values — preserve as real nulls rather
                    # than dropping the key or blanking it to "" (a blank string
                    # is not the same as "no value", and downstream steps like
                    # PII masking/type coercion need to be able to tell the
                    # difference).
                    cleaned = self._handle_nulls(normalized, strategy="preserve")
                    
                    # Step 4: Validate and coerce data types
                    validated = self._validate_and_coerce_types(cleaned)
                    
                    # Step 5: Mask PII (optional, config-driven — mask() is a
                    # no-op passthrough when pii_masking.enabled is false, so
                    # no branch is needed here).
                    masked = self.pii_masker.mask(validated)
                    if self.pii_masker.enabled:
                        log.debug(
                            "SilverLayer: PII masked for record %d using strategy '%s'",
                            idx, self.pii_masker.strategy
                        )
                    else:
                        log.debug("SilverLayer: PII masking disabled for record %d", idx)
                    
                    # Step 6: Deduplication check — namespaced by source (table/API
                    # name, tagged by Bronze as _bronze_source) plus id. Different
                    # tables/endpoints each have their own auto-increment id
                    # starting at 1, so a bare id value is not unique across
                    # sources when multiple readers/tables feed one pipeline run.
                    # Not every source uses a literal "id"/"_id" column (e.g. this
                    # employee dataset's key is "employee_id"), so fall back to
                    # any "*_id" field rather than silently skipping dedup.
                    record_id = self._get_record_id(masked)
                    if record_id is not None:
                        dedup_key = (masked.get("_bronze_source"), record_id)
                        if dedup_key in seen_ids:
                            log.debug(
                                "SilverLayer: skipping duplicate record with source=%s id=%s",
                                dedup_key[0], record_id
                            )
                            continue
                        seen_ids.add(dedup_key)
                    else:
                        log.debug(
                            "SilverLayer: no id-like field found on record %d — "
                            "deduplication could not be checked",
                            idx
                        )

                    # Step 7: Add silver layer metadata
                    masked["_silver_normalization_time"] = datetime.utcnow().isoformat()
                    masked["_silver_layer"] = "silver"
                    masked["_silver_record_index"] = idx
                    # True means dedup was actually checked for this record (and
                    # it passed, since duplicates are filtered out above via
                    # `continue`) — not "this record was a duplicate". False
                    # means no id-like field could be found, so dedup could not
                    # run at all for this record.
                    masked["_silver_deduplicated"] = record_id is not None
                    masked["_silver_pii_masked"] = self.pii_masker.enabled
                    if self.pii_masker.enabled:
                        masked["_silver_masking_strategy"] = self.pii_masker.strategy
                    
                    silver_records.append(masked)
                
                except Exception as exc:
                    log.warning(
                        "SilverLayer: validation error for record %d — %s",
                        idx, exc
                    )
                    validation_errors.append({"record_index": idx, "error": str(exc)})
                    # Still include the record but mark it as having errors
                    normalized = {}
                    for key, value in record.items():
                        snake_key = self._camel_to_snake(key)
                        normalized[snake_key] = value
                    normalized["_silver_normalization_time"] = datetime.utcnow().isoformat()
                    normalized["_silver_layer"] = "silver"
                    normalized["_silver_validation_error"] = str(exc)
                    silver_records.append(normalized)
            
            log.info(
                "SilverLayer: processed %d records (cleaned, validated, deduplicated)",
                len(silver_records)
            )
            if self.pii_masker.enabled:
                log.info(
                    "SilverLayer: PII masking applied using strategy '%s'",
                    self.pii_masker.strategy
                )
            else:
                log.info("SilverLayer: PII masking disabled (no masking applied)")
            if validation_errors:
                log.warning("SilverLayer: %d validation errors encountered", len(validation_errors))
            
            return silver_records
        
        except Exception as exc:
            log.error("SilverLayer: execution failed — %s", exc, exc_info=True)
            raise
    
    # ── Helper Methods ───────────────────────────────────────────────────────────

    def _get_record_id(self, record: Dict[str, Any]) -> Optional[Any]:
        """
        Find a natural/primary key value on a record for deduplication.

        Delegates to the shared implementation in core/record_identity.py,
        also used by Lambda's Batch/Speed layers — recognizes "employee_id"
        and "EmployeeID" style columns alike, not just underscore-delimited
        ones. This layer's own field-name normalization (Step 2, above)
        already converts most camelCase columns to snake_case before this
        ever runs, so in practice this rarely needs the camelCase branch —
        but relying on that as the only reason dedup works would leave any
        record that skips normalization (e.g. the per-record exception
        fallback further down, which normalizes but never re-validates)
        one rename away from silently losing dedup again.

        Args:
            record: Record to inspect

        Returns:
            The id value if one was found (and is non-blank), else None
        """
        return get_record_id(record)

    def _flatten_nested_objects(self, obj: Any, prefix: str = "", max_depth: int = 5,
                                current_depth: int = 0) -> Dict[str, Any]:
        """
        Flatten nested dictionaries and lists into a single-level dictionary.
        
        Args:
            obj: Object to flatten (dict, list, or scalar)
            prefix: Current key prefix for nested keys
            max_depth: Maximum nesting depth to flatten
            current_depth: Current recursion depth
        
        Returns:
            Flattened dictionary
        """
        if current_depth >= max_depth:
            return {prefix: obj} if prefix else {"_nested_value": obj}
        
        result = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}_{key}" if prefix else key
                
                if isinstance(value, dict):
                    # Recursively flatten nested dict
                    flattened = self._flatten_nested_objects(
                        value, prefix=new_key, max_depth=max_depth, 
                        current_depth=current_depth + 1
                    )
                    result.update(flattened)
                elif isinstance(value, list):
                    # Convert list to JSON string (don't flatten lists)
                    result[new_key] = json.dumps(value)
                else:
                    # Scalar value
                    result[new_key] = value
        
        elif isinstance(obj, list):
            # Convert list to JSON string
            return {prefix: json.dumps(obj)} if prefix else {"_list_value": json.dumps(obj)}
        
        else:
            # Scalar value
            return {prefix: obj} if prefix else {"_scalar_value": obj}
        
        return result
    
    def _normalize_field_names(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize field names: camelCase → snake_case.
        
        Args:
            record: Record with potentially mixed naming conventions
        
        Returns:
            Record with standardized snake_case field names
        """
        normalized = {}
        for key, value in record.items():
            snake_key = self._camel_to_snake(key)
            normalized[snake_key] = value
        
        log.debug("SilverLayer: normalized field names for record")
        return normalized
    
    def _camel_to_snake(self, name: str) -> str:
        """
        Convert camelCase to snake_case.
        
        Examples:
            userId → user_id
            firstName → first_name
            HTTPResponse → http_response
        
        Args:
            name: camelCase string
        
        Returns:
            snake_case string
        """
        # Insert underscore before uppercase letters followed by lowercase
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        # Insert underscore before uppercase letters that follow lowercase or digits
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    
    def _handle_nulls(self, record: Dict[str, Any], strategy: str = "preserve") -> Dict[str, Any]:
        """
        Handle null/missing values in a record.

        A "missing" value doesn't always arrive as a real `None`. CSV has no
        native null, so batch_s3_read's csv.DictReader represents a blank
        cell as `""`; other sources have their own quirks. Readers are
        intentionally kept dumb (read the source as-is, no transformation) —
        this normalization is centralized here in Silver so every source
        (S3/CSV, database, API, ...) gets treated the same way regardless of
        how that particular reader happens to represent "no value".

        Args:
            record: Dictionary to process
            strategy: "preserve" (keep as real null — default), "drop" (remove
                the key entirely), "replace_empty" (replace with ""),
                "flag" (keep as null and add a companion "<key>_is_null" flag)

        Returns:
            Processed dictionary
        """
        result = {}

        for key, value in record.items():
            # Treat None and blank/whitespace-only strings as equally "missing"
            is_missing = value is None or (isinstance(value, str) and value.strip() == "")

            if is_missing:
                if strategy == "drop":
                    # Skip missing values
                    continue
                elif strategy == "replace_empty":
                    # Replace with empty string
                    result[key] = ""
                elif strategy == "flag":
                    # Keep the field present and genuinely null, but flag it
                    result[key] = None
                    result[f"{key}_is_null"] = True
                else:
                    # "preserve" (default): keep the field present and null,
                    # so a missing value is never confused with real data
                    result[key] = None
            else:
                result[key] = value

        return result
    
    def _validate_and_coerce_types(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and coerce data types in a record.
        
        Args:
            record: Dictionary to validate
        
        Returns:
            Validated dictionary with type coercion applied
        """
        result = {}
        
        for key, value in record.items():
            if value is None:
                result[key] = None
                continue
            
            # Try to coerce to appropriate type
            if isinstance(value, bool):
                # Keep booleans as-is
                result[key] = value
            elif isinstance(value, (int, float)):
                # Keep numbers as-is
                result[key] = value
            elif isinstance(value, str):
                # Blank strings were already normalized to None by
                # _handle_nulls (Step 3, runs before this), so any string
                # reaching here is real, non-missing data.
                stripped = value.strip()
                if re.fullmatch(r"-?\d+", stripped):
                    # Plain integer string (e.g. "29") — value.isdigit() alone
                    # would miss the leading "-" on negatives and would reject
                    # "29.0", pushing it into float() below.
                    result[key] = int(stripped)
                else:
                    try:
                        parsed = float(stripped)
                    except ValueError:
                        # Keep as string
                        result[key] = value
                    else:
                        # Collapse whole-number floats (e.g. "29.0" from a
                        # source column that had missing values) down to int
                        # so the column doesn't end up mixing int and float
                        # types across rows. Genuine decimals (e.g. "75000.5")
                        # stay float.
                        result[key] = int(parsed) if parsed.is_integer() else parsed
            else:
                # Keep other types as-is (lists, dicts, etc.)
                result[key] = value
        
        return result
    

# ── Factory function ──────────────────────────────────────────────────────────

def create_silver_layer(config: dict) -> SilverLayer:
    """
    Factory function to create a Silver layer instance.
    
    Args:
        config: Full configuration from config.yaml
    
    Returns:
        Configured SilverLayer instance
    """
    return SilverLayer(config)
