"""
core/null_handling.py
────────────────────────
Shared blank/null normalization. Medallion's Silver layer has always done
this (see its own _handle_nulls); Lambda's Batch/Speed layers had no
equivalent step at all, so a blank source value stayed a literal "" forever
instead of becoming a real null — indistinguishable from a genuine empty
string to anything reading the output. This is that same normalization,
factored out so Lambda can apply it too without reimplementing it.
"""

from typing import Any, Dict


def normalize_nulls(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Treat None and blank/whitespace-only strings as equally "missing",
    converting both to a real None.

    A missing value doesn't always arrive as a real None — CSV has no
    native null, so a blank cell reads as "" (e.g. via csv.DictReader);
    other sources have their own quirks. Normalizing here means a
    downstream consumer never has to guess whether "" in the output means
    "no value" or "the source genuinely contained an empty string".

    Args:
        record: Dictionary to process

    Returns:
        A new dict with blank/whitespace-only string values converted to
        None; does not mutate the input.
    """
    result = {}
    for key, value in record.items():
        is_missing = value is None or (isinstance(value, str) and value.strip() == "")
        result[key] = None if is_missing else value
    return result
