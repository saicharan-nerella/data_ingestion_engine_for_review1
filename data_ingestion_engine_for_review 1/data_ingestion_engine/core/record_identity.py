"""
core/record_identity.py
──────────────────────────
Shared "does this record have a natural key, and what is it" logic — used
by Lambda's Batch/Speed layers to key their dedup/merge, and by Medallion's
Silver layer to key its own dedup. Single implementation so both
architectures recognize the same id-like fields, instead of each keeping
its own copy that can silently drift (which is exactly what happened
before this existed: Lambda's own copy only recognized "employee_id", not
"EmployeeID" — Silver appeared to "work" on the same data only because its
unrelated camelCase-to-snake_case step happened to rewrite the field name
first).
"""

from typing import Any, Dict, Optional


def looks_like_id_field(key: str) -> bool:
    """
    True if `key` is very likely an identifier column:
      - snake_case: ends in "_id" ("employee_id", "customer_id")
      - camelCase/PascalCase: "Id"/"ID" starts a new word at the end
        ("EmployeeID", "customerId") — detected by a lowercase letter
        immediately before the suffix, i.e. a real case-boundary, not just
        the last two characters happening to spell "id"

    Deliberately NOT a bare case-insensitive `endswith("id")` — that would
    also match ordinary words that merely happen to end in those letters:
    "grid", "valid", "paid", "void", "solid", "acid", "rapid", "avoid".
    The boundary check is what tells "EmployeeID" (boundary: ...e|ID) apart
    from "Grid" (no boundary — every letter after the leading capital is
    lowercase, so there's no camelCase transition to detect).

    A field with NO case signal at all — a genuinely all-lowercase,
    unseparated name like "employeeid" — can't be told apart from a
    coincidental word this way. That's a real, accepted limitation: a
    heuristic that guessed anyway would risk silently keying the wrong
    field, which is worse than correctly declining to guess.
    """
    if key.lower().endswith("_id"):
        return True
    if len(key) > 2 and key[-2:] in ("ID", "Id") and key[-3].islower():
        return True
    return False


def get_record_id(record: Dict[str, Any]) -> Optional[Any]:
    """
    Find a natural/primary key value on a record.

    Checks the generic "id"/"_id" fields first, then falls back to any
    field recognized by looks_like_id_field() (e.g. "employee_id",
    "EmployeeID", "customerId") — many sources key their rows by a
    domain-specific id rather than a literal "id" column.

    Args:
        record: Record to inspect

    Returns:
        The id value if one was found (and is non-blank), else None
    """
    for key in ("id", "_id"):
        value = record.get(key)
        if value not in (None, ""):
            return value

    for key, value in record.items():
        if looks_like_id_field(key) and value not in (None, ""):
            return value

    return None
