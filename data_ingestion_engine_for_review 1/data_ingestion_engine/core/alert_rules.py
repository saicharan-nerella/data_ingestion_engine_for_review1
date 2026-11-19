"""
core/alert_rules.py
──────────────────────
Real implementation behind config.yaml's alerting.rules section. Until this
existed, `rules:` and `severity:` were pure description — nothing in the
codebase ever read them, so a customer configuring a threshold there got no
actual monitoring. This module computes error rate, run duration, and
throughput for one completed run and fires the matching notify() event if a
configured threshold was crossed.

This is a single post-run check, not a continuously-monitored sliding
window: both Medallion and Lambda run as one-shot Glue batch jobs, not a
long-running service — a job either runs or it doesn't, there's no "keep
checking every 5 minutes" concept to hook into. So high_error_rate's
`window: 300` key is accepted (so an existing config doesn't have to drop
it) but not actually used as a sliding window; the check is simply "this
run's error rate," end to end.
"""

import logging
import time
from typing import Any, Optional

log = logging.getLogger(__name__)


def check_alert_rules(
    config: dict,
    notifier: Any,
    *,
    source: str,
    start_time: float,
    total_records: int,
    failed_records: int = 0,
) -> None:
    """
    Evaluate alerting.rules against one completed run and fire whichever
    notify() events crossed their configured threshold. The three rules are
    independent — one crossing its threshold doesn't stop the others from
    being checked.

    Args:
        config: full config.yaml dict
        notifier: a NotificationService (or anything with a matching
            .notify(event_type, **context) method)
        source: human-readable label for what this run processed (table,
            file, API name, ...) — same label used in the success/failure
            notifications
        start_time: time.time() captured when the run began
        total_records: records seen this run — the denominator for error
            rate and throughput
        failed_records: records this architecture marked as failed this run
            (e.g. Medallion Silver layer's _silver_validation_error count).
            Default 0 for architectures that don't track per-record
            failures yet (e.g. Lambda's Batch/Speed layers today) — that's
            an honest 0%, not a fabricated number.
    """
    rules = (config.get("alerting", {}) or {}).get("rules", {}) or {}
    if not rules:
        return

    duration_seconds = time.time() - start_time

    _check_pipeline_timeout(rules, notifier, source, duration_seconds)
    _check_high_error_rate(rules, notifier, source, total_records, failed_records)
    _check_low_throughput(rules, notifier, source, total_records, duration_seconds)


def _check_pipeline_timeout(rules: dict, notifier: Any, source: str,
                             duration_seconds: float) -> None:
    rule = rules.get("pipeline_timeout")
    if not rule:
        return
    threshold: Optional[float] = rule.get("threshold_seconds")
    if threshold is None or duration_seconds <= threshold:
        return

    # Reuses the existing "performance" event/formatter (job_name,
    # duration_seconds, threshold) rather than inventing a new event type —
    # "the pipeline ran too long" IS a performance alert.
    notifier.notify(
        "performance",
        job_name=source,
        duration_seconds=round(duration_seconds, 2),
        threshold=threshold,
    )


def _check_high_error_rate(rules: dict, notifier: Any, source: str,
                            total_records: int, failed_records: int) -> None:
    rule = rules.get("high_error_rate")
    if not rule or total_records == 0:
        return
    threshold: Optional[float] = rule.get("threshold")
    if threshold is None:
        return

    error_rate = failed_records / total_records
    if error_rate <= threshold:
        return

    notifier.notify(
        "high_error_rate",
        source=source,
        error_rate=f"{error_rate:.1%}",
        threshold=f"{threshold:.1%}",
        failed_records=failed_records,
        total_records=total_records,
    )


def _check_low_throughput(rules: dict, notifier: Any, source: str,
                           total_records: int, duration_seconds: float) -> None:
    rule = rules.get("low_throughput")
    # Unlike the other two rules, low_throughput carries its own `enabled`
    # flag in config.yaml (default false) — throughput naturally varies far
    # more by source size than an error rate or a timeout does, so it's
    # opt-in rather than on whenever the rule block is present.
    if not rule or not rule.get("enabled", False):
        return
    if duration_seconds <= 0:
        return  # too fast to compute a meaningful rate — nothing to flag

    threshold: Optional[float] = rule.get("threshold_rows_per_min")
    if threshold is None:
        return

    rows_per_min = total_records / (duration_seconds / 60)
    if rows_per_min >= threshold:
        return

    notifier.notify(
        "low_throughput",
        source=source,
        rows_per_min=round(rows_per_min, 1),
        threshold=threshold,
    )
