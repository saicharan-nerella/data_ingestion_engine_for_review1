# Formats a pipeline event and hands it to a caller-supplied sender callable.
# Whether that sender talks to SNS, Slack, SMTP, or just logs is deliberately
# invisible here — this file must never import a channel-specific library.

import json
import logging
from datetime import datetime
from typing import Callable, Optional

log = logging.getLogger(__name__)

# Fallback severities, used only when config.yaml's alerting.severity
# section doesn't name a given event_type explicitly. Every event type
# _format() below knows how to render has an entry here so severity is
# never missing from a payload.
_DEFAULT_SEVERITY = {
    "ingestion_failure": "critical",
    "ingestion_success": "low",
    "data_volume":       "medium",
    "performance":       "medium",
    "retry":             "medium",
    "sla_breach":        "high",
    "high_error_rate":   "high",
    "low_throughput":    "low",
}


class NotificationService:

    def __init__(self, config: dict, sender: Optional[Callable] = None):
        self.config  = config or {}
        self.enabled = self.config.get("enabled", False)
        self.on      = self.config.get("notify_on", {})  # not "on" — YAML 1.1 parses on: as boolean True, not the string
        self._sender = sender

        if self.enabled and sender is None:
            # enabled with nothing to deliver through would otherwise fail silently at runtime
            log.warning(
                "NotificationService: enabled=true but no sender provided — "
                "events will be logged only"
            )

    # ── Public API ────────────────────────────────────────────────────────────

    def notify(self, event_type: str, **context) -> None:
        if not self.enabled:
            return  # global kill switch — skips formatting entirely, not just delivery

        if not self.on.get(event_type, False):
            log.debug("NotificationService: '%s' disabled — skipping", event_type)
            return  # per-event gate, independent of whichever channel is wired up

        subject, message_body = self._format(event_type, context)

        payload = {
            "event_type": event_type,
            "subject":    subject,
            "message":    message_body,
            "timestamp":  datetime.utcnow().isoformat() + "Z",
            "context":    {k: str(v) for k, v in context.items()},  # stringify so payload is always JSON-safe
            # config.yaml's alerting.severity section, keyed by event_type —
            # e.g. {"ingestion_failure": "critical"}. Falls back to a
            # sensible built-in per event type so severity is always present
            # even if the customer never configured that section at all.
            "severity":   self.config.get("severity", {}).get(event_type, _DEFAULT_SEVERITY.get(event_type, "medium")),
        }

        if self._sender is None:
            # no channel wired up — log so the event isn't silently lost
            log.warning("NOTIFICATION [%s] %s\n%s", event_type, subject, message_body)
            return

        try:
            self._sender(subject=subject, message=message_body, payload=payload)
            log.info("NotificationService: sent '%s'", event_type)
        except Exception as exc:
            # a broken channel (bad webhook, expired ARN, network blip) must
            # never crash the pipeline that's reporting on itself
            log.error("NotificationService: sender failed for '%s': %s", event_type, exc)

    # ── Message formatting ────────────────────────────────────────────────────

    def _format(self, event_type: str, context: dict) -> tuple[str, str]:
        ts  = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        sep = "─" * 40

        if event_type == "ingestion_failure":
            source       = context.get("source", "unknown")
            architecture = context.get("architecture", "unknown")
            writer       = context.get("writer", "unknown")
            error        = context.get("error", "unknown error")
            count        = context.get("record_count", "N/A")
            return (
                f"[FAILURE] Ingestion Failed — {source} ({architecture} → {writer})",
                f"Ingestion FAILURE\n{sep}\n"
                f"Reader        : {source}\n"
                f"Architecture  : {architecture}\n"
                f"Writer        : {writer}\n"
                f"Error         : {error}\n"
                f"Records       : {count}\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "ingestion_success":
            source       = context.get("source", "unknown")
            architecture = context.get("architecture", "unknown")
            writer       = context.get("writer", "unknown")
            count        = context.get("record_count", "N/A")
            duration     = context.get("duration_seconds", "N/A")
            return (
                f"[SUCCESS] Ingestion Complete — {source} ({architecture} → {writer})",
                f"Ingestion SUCCESS\n{sep}\n"
                f"Reader        : {source}\n"
                f"Architecture  : {architecture}\n"
                f"Writer        : {writer}\n"
                f"Records       : {count}\n"
                f"Duration      : {duration}s\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "data_volume":
            source    = context.get("source", "unknown")
            count     = int(context.get("record_count", 0))
            threshold = context.get("threshold", "N/A")
            kind      = "NO DATA" if count == 0 else "HIGH VOLUME"
            return (
                f"[ALERT] Data Volume {kind} — {source}",
                f"Data Volume Alert: {kind}\n{sep}\n"
                f"Source        : {source}\n"
                f"Record Count  : {count}\n"
                f"Threshold     : {threshold}\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "performance":
            job      = context.get("job_name", "unknown")
            duration = context.get("duration_seconds", 0)
            limit    = context.get("threshold", "N/A")
            return (
                f"[ALERT] Performance — Job Too Slow — {job}",
                f"Performance Alert\n{sep}\n"
                f"Job Name      : {job}\n"
                f"Duration      : {duration}s\n"
                f"Threshold     : {limit}s\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "retry":
            job     = context.get("job_name", "unknown")
            retries = context.get("retry_count", 0)
            max_r   = context.get("max_retries", "N/A")
            return (
                f"[ALERT] Retry Limit Exceeded — {job}",
                f"Retry Alert\n{sep}\n"
                f"Job Name      : {job}\n"
                f"Retry Count   : {retries}\n"
                f"Max Retries   : {max_r}\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "sla_breach":
            job      = context.get("job_name", "unknown")
            expected = context.get("expected_time", "N/A")
            status   = context.get("status", "NOT COMPLETED")
            return (
                f"[ALERT] SLA Breach — {job}",
                f"SLA Breach Alert\n{sep}\n"
                f"Job Name      : {job}\n"
                f"Expected By   : {expected}\n"
                f"Current Status: {status}\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "high_error_rate":
            # Fired by core/alert_rules.py when alerting.rules.high_error_rate's
            # threshold is crossed for one completed run.
            source  = context.get("source", "unknown")
            rate    = context.get("error_rate", "N/A")
            thresh  = context.get("threshold", "N/A")
            failed  = context.get("failed_records", "N/A")
            total   = context.get("total_records", "N/A")
            return (
                f"[ALERT] High Error Rate — {source}",
                f"High Error Rate Alert\n{sep}\n"
                f"Source        : {source}\n"
                f"Error Rate    : {rate} (threshold: {thresh})\n"
                f"Failed Records: {failed} / {total}\n"
                f"Timestamp     : {ts}\n",
            )

        elif event_type == "low_throughput":
            # Fired by core/alert_rules.py when alerting.rules.low_throughput
            # is enabled and its threshold is crossed for one completed run.
            source = context.get("source", "unknown")
            rate   = context.get("rows_per_min", "N/A")
            thresh = context.get("threshold", "N/A")
            return (
                f"[ALERT] Low Throughput — {source}",
                f"Low Throughput Alert\n{sep}\n"
                f"Source        : {source}\n"
                f"Rows/Minute   : {rate} (threshold: {thresh})\n"
                f"Timestamp     : {ts}\n",
            )

        else:
            # unrecognized event_type — dump context generically rather than dropping it
            return (
                f"[ALERT] {event_type}",
                f"Pipeline Event: {event_type}\n{sep}\n"
                + "\n".join(f"{k:14}: {v}" for k, v in context.items())
                + f"\nTimestamp     : {ts}\n",
            )
