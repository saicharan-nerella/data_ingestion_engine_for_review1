"""
core/pii_masking.py
─────────────────────
Shared PII masking — the single implementation every architecture uses, so
config.yaml's `pii_masking` section (enabled / strategy / sensitive_columns)
means exactly the same thing regardless of --architecture. Originally this
logic lived only inside Medallion's SilverLayer; Lambda's Batch/Speed layers
had no masking at all, so choosing --architecture lambda silently bypassed
pii_masking.enabled entirely. Extracting it here is what makes the config
"common" across architectures — each architecture wires this in wherever its
own records are finalized, instead of reimplementing (and risking drifting
from) the same column-matching and masking-strategy logic independently.

Usage:
    masker = PIIMasker(config)   # config is the full config.yaml dict
    record = masker.mask(record) # no-op passthrough if pii_masking.enabled is false
"""

import hashlib
import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

DEFAULT_SENSITIVE_COLUMNS = [
    "email", "phone", "phone_number", "ssn", "social_security_number",
    "credit_card", "card_number", "password", "api_key", "token",
    "address", "street", "zip_code", "zipcode", "postal_code",
    "date_of_birth", "dob", "passport", "driver_license",
]

VALID_STRATEGIES = ("sha256", "redact", "partial")


class PIIMasker:
    """
    Reads config["pii_masking"] once and applies it consistently wherever
    it's plugged in:
        enabled: true | false
        strategy: sha256 | redact | partial
        sensitive_columns: [...]            # optional, falls back to DEFAULT_SENSITIVE_COLUMNS

    mask() always no-ops when disabled — callers never need to check
    `.enabled` themselves before calling it, which is what lets every
    architecture apply the same on/off switch without duplicating the check.
    """

    def __init__(self, config: dict):
        pii_config = (config or {}).get("pii_masking", {}) or {}

        self.enabled = bool(pii_config.get("enabled", False))
        self.strategy = pii_config.get("strategy", "sha256")

        custom_columns = pii_config.get("sensitive_columns") or []
        self.sensitive_columns: List[str] = (
            custom_columns if custom_columns else list(DEFAULT_SENSITIVE_COLUMNS)
        )

        # Fail once at construction time rather than on the first record —
        # a misconfigured strategy should never let a whole batch process
        # before surfacing the error.
        if self.enabled and self.strategy not in VALID_STRATEGIES:
            raise ValueError(
                f"Invalid pii_masking.strategy '{self.strategy}'. "
                f"Must be one of: {', '.join(VALID_STRATEGIES)}"
            )

        log.info(
            "PIIMasker: enabled=%s strategy=%s sensitive_columns=%d",
            self.enabled, self.strategy, len(self.sensitive_columns),
        )

    def mask(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask sensitive fields in `record`. Returns `record` unchanged
        (same object, not a copy) when masking is disabled.

        Args:
            record: Dictionary to process

        Returns:
            New dict with sensitive fields masked, or the original dict
            untouched if pii_masking.enabled is false.
        """
        if not self.enabled:
            return record

        result = {}
        for key, value in record.items():
            if self._is_sensitive(key) and value not in (None, ""):
                result[key] = self._apply_strategy(value)
            else:
                result[key] = value
        return result

    # ── Internals ────────────────────────────────────────────────────────────

    def _is_sensitive(self, key: str) -> bool:
        """
        A column is sensitive on an exact (case-insensitive) match against
        sensitive_columns, or if any configured column name appears as a
        substring of it (e.g. "email" matches "email_address").
        """
        key_lower = key.lower()
        lowered_columns = [col.lower() for col in self.sensitive_columns]
        if key_lower in lowered_columns:
            return True
        return any(col in key_lower for col in lowered_columns)

    def _apply_strategy(self, value: Any) -> Any:
        if self.strategy == "sha256":
            hashed = hashlib.sha256(str(value).encode()).hexdigest()[:16]
            return f"HASH_{hashed}"

        if self.strategy == "redact":
            return "***REDACTED***"

        # "partial" — show first/last few characters
        str_value = str(value)
        if len(str_value) <= 4:
            return "***"
        if "@" in str_value:  # email
            parts = str_value.split("@")
            return f"{parts[0][:2]}***@{parts[1]}"
        if "-" in str_value:  # phone or SSN
            return f"{str_value[:3]}***{str_value[-4:]}"
        return f"{str_value[:2]}***{str_value[-2:]}"
