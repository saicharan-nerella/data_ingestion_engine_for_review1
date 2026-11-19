# The only file allowed to import channel SDKs (boto3, requests, smtplib).
# build_sender_from_config() dispatches on alerting.channel to the matching
# build_<x>_sender() below, so notification_service.py never needs that import.

import logging
from typing import Callable, Optional

from alerts.notification_service import NotificationService

log = logging.getLogger(__name__)


# ── Channel builders ────────────────────────────────────────────────────────
# Each takes that channel's own config sub-block and returns a sender matching
# NotificationService's contract: sender(subject, message, payload). Each
# validates its required setting immediately (raises ValueError here) rather
# than inside the closure — a misconfigured channel should surface at startup
# wiring time, not on the first real alert.

def build_sns_sender(cfg: dict) -> Callable:
    topic_arn = cfg.get("topic_arn")
    region = cfg.get("region", "us-east-1")
    if not topic_arn:
        raise ValueError("alerting.sns.topic_arn is required for channel: sns")

    def sns_sender(subject: str, message: str, payload: dict) -> None:
        import boto3
        boto3.client("sns", region_name=region).publish(
            TopicArn=topic_arn,
            Subject=subject[:100],  # SNS enforces a 100-char subject limit
            Message=message,
        )

    return sns_sender


def build_slack_sender(cfg: dict) -> Callable:
    webhook_url = cfg.get("webhook_url")
    if not webhook_url:
        raise ValueError("alerting.slack.webhook_url is required for channel: slack")

    def slack_sender(subject: str, message: str, payload: dict) -> None:
        import requests
        requests.post(
            webhook_url,
            json={"text": f"*{subject}*\n```{message}```"},
            timeout=10,
        )

    return slack_sender


def build_teams_sender(cfg: dict) -> Callable:
    webhook_url = cfg.get("webhook_url")
    if not webhook_url:
        raise ValueError("alerting.teams.webhook_url is required for channel: teams")

    def teams_sender(subject: str, message: str, payload: dict) -> None:
        import requests
        requests.post(
            webhook_url,
            json={
                "@type": "MessageCard",
                "summary": subject,
                "sections": [{
                    "activityTitle": subject,
                    "text": message.replace("\n", "<br>"),  # Teams renders HTML, not literal \n
                }],
            },
            timeout=10,
        )

    return teams_sender


def build_email_sender(cfg: dict) -> Callable:
    smtp_host = cfg.get("smtp_host")
    smtp_port = cfg.get("smtp_port", 587)
    username  = cfg.get("username")
    password  = cfg.get("password")
    from_addr = cfg.get("from_addr") or username  # fall back to the auth identity if no explicit From
    to_addrs  = cfg.get("to_addrs") or []
    use_tls   = cfg.get("use_tls", True)

    if not smtp_host or not to_addrs:
        raise ValueError(
            "alerting.email.smtp_host and alerting.email.to_addrs are "
            "required for channel: email"
        )

    def email_sender(subject: str, message: str, payload: dict) -> None:
        import smtplib
        from email.mime.text import MIMEText

        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = ", ".join(to_addrs)

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as smtp:
            if use_tls:
                smtp.starttls()
            if username and password:
                smtp.login(username, password)
            smtp.sendmail(from_addr, to_addrs, msg.as_string())

    return email_sender


def build_ses_sender(cfg: dict) -> Callable:
    # Separate from build_email_sender: this goes through the AWS SES API
    # directly (uses whatever AWS credentials the pipeline already has), no
    # SMTP server/credentials required. SES sandbox mode requires both
    # from_addr and every to_addrs entry to be verified identities.
    from_addr = cfg.get("from_addr")
    to_addrs  = cfg.get("to_addrs") or []
    region    = cfg.get("region", "us-east-1")

    if not from_addr or not to_addrs:
        raise ValueError(
            "alerting.ses.from_addr and alerting.ses.to_addrs are "
            "required for channel: ses"
        )

    def ses_sender(subject: str, message: str, payload: dict) -> None:
        import boto3
        boto3.client("ses", region_name=region).send_email(
            Source=from_addr,
            Destination={"ToAddresses": to_addrs},
            Message={
                "Subject": {"Data": subject[:998]},  # SES header length limit
                "Body": {"Text": {"Data": message}},
            },
        )

    return ses_sender


def build_sqs_sender(cfg: dict) -> Callable:
    # Unlike every other channel here, SQS's consumer is code, not a human —
    # there's nothing to "read" in a queue. So instead of the subject/message
    # text, this delivers the full structured payload as JSON, and tags the
    # event_type as a message attribute so a consumer can filter without
    # having to parse the body first (e.g. an SQS-triggered Lambda that only
    # cares about ingestion_failure).
    queue_url = cfg.get("queue_url")
    region    = cfg.get("region", "us-east-1")

    if not queue_url:
        raise ValueError("alerting.sqs.queue_url is required for channel: sqs")

    def sqs_sender(subject: str, message: str, payload: dict) -> None:
        import boto3
        import json as _json
        boto3.client("sqs", region_name=region).send_message(
            QueueUrl=queue_url,
            MessageBody=_json.dumps(payload),
            MessageAttributes={
                "event_type": {
                    "DataType": "String",
                    "StringValue": payload.get("event_type", "unknown"),
                }
            },
        )

    return sqs_sender


# PagerDuty's Events API v2 only accepts these 4 exact severity values.
# config.yaml's alerting.severity section uses critical/high/medium/low
# (matching the vocabulary the rest of the alerting config already uses),
# so this maps one to the other rather than asking customers to know
# PagerDuty's specific enum.
_PAGERDUTY_SEVERITY_MAP = {
    "critical": "critical",
    "high":     "error",
    "medium":   "warning",
    "low":      "info",
}


def _render_template(template, subject: str, message: str):
    """
    Walk a dict/list/string template, replacing the literal placeholder
    values "$subject"/"$message" with the real text. Works on native
    Python values (from YAML), never on a JSON string — so there's no
    escaping to get wrong no matter what the alert message contains
    (quotes, newlines, braces).
    """
    if isinstance(template, dict):
        return {k: _render_template(v, subject, message) for k, v in template.items()}
    if isinstance(template, list):
        return [_render_template(v, subject, message) for v in template]
    if template == "$subject":
        return subject
    if template == "$message":
        return message
    if isinstance(template, str):
        return template.replace("$subject", subject).replace("$message", message)
    return template


def build_webhook_sender(cfg: dict) -> Callable:
    """
    Generic HTTP webhook sender — works with any platform that accepts an
    incoming webhook (Discord, Opsgenie, Webex, Mattermost, Google Chat, a
    custom internal endpoint, ...) without this file needing to know that
    platform's exact request shape ahead of time.

    This is also the automatic fallback build_sender_from_config() reaches
    for when a configured channel name isn't one of the named builders
    above (see CHANNEL_BUILDERS) — that's what makes "any channel, we
    can't predict which" work: a channel this codebase has never heard of
    still gets delivered, as long as its config gives it a url, because
    nearly every notification platform in existence accepts a webhook even
    when it also offers a nicer native API.

    Config:
        url            REQUIRED — the webhook endpoint
        method         HTTP method (default: POST)
        headers        optional dict of extra HTTP headers (many webhook
                       receivers require an Authorization bearer token here)
        body_template  optional dict/string. "$subject" and "$message"
                       anywhere inside it are replaced with the real
                       values (see _render_template) — e.g.
                       {"content": "$message"} for a Discord-shaped
                       webhook. If omitted, the full structured payload
                       (event_type, subject, message, timestamp, context,
                       severity) is sent as-is — the safest default, since
                       any receiver can pull whatever field it needs out
                       of arbitrary JSON without this file having to guess
                       its shape.
    """
    url = cfg.get("url")
    if not url:
        raise ValueError(
            "alerting.<channel>.url is required for a webhook/generic channel"
        )

    method = cfg.get("method", "POST").upper()
    headers = cfg.get("headers") or {}
    body_template = cfg.get("body_template")

    def webhook_sender(subject: str, message: str, payload: dict) -> None:
        import requests
        body = _render_template(body_template, subject, message) if body_template else payload
        requests.request(method, url, json=body, headers=headers, timeout=10)

    return webhook_sender


def build_pagerduty_sender(cfg: dict) -> Callable:
    routing_key = cfg.get("routing_key")
    if not routing_key:
        raise ValueError("alerting.pagerduty.routing_key is required for channel: pagerduty")

    def pagerduty_sender(subject: str, message: str, payload: dict) -> None:
        import requests
        # payload["severity"] comes from config.yaml's alerting.severity
        # section (NotificationService fills in a sensible default per
        # event_type even if that section doesn't mention it) — no more
        # guessing severity by sniffing "FAILURE" out of the subject text.
        severity = _PAGERDUTY_SEVERITY_MAP.get(payload.get("severity", "medium"), "warning")
        requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json={
                "routing_key": routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": subject,
                    "source": payload.get("context", {}).get("source", "aimatic"),
                    "severity": severity,
                    "custom_details": payload,
                },
            },
            timeout=10,
        )

    return pagerduty_sender


# Plugin registry — add a channel by writing one build_<x>_sender() above and
# adding one line here. Nothing in NotificationService or the pipeline changes.
# "webhook" is the explicit generic entry; build_sender_from_config() below
# also falls back to build_webhook_sender automatically for any channel name
# NOT in this dict, as long as that channel's config block has a url — so a
# channel this dict has never heard of still works, it just isn't named here.
CHANNEL_BUILDERS = {
    "sns":       build_sns_sender,
    "slack":     build_slack_sender,
    "teams":     build_teams_sender,
    "email":     build_email_sender,
    "mail":      build_email_sender,   # alias — matches common phrasing
    "ses":       build_ses_sender,
    "sqs":       build_sqs_sender,
    "pagerduty": build_pagerduty_sender,
    "webhook":   build_webhook_sender,
}


# ── Fan-out ──────────────────────────────────────────────────────────────────

def _fan_out(senders_by_channel: dict) -> Callable:
    # Each channel is delivered independently and failures are caught per
    # channel — one dead webhook must not swallow a working one. Only raise
    # (so NotificationService's own log.error fires) if every channel failed;
    # if at least one got through, the event was genuinely delivered.
    def fan_out_sender(subject: str, message: str, payload: dict) -> None:
        errors = {}
        for name, sender in senders_by_channel.items():
            try:
                sender(subject=subject, message=message, payload=payload)
                log.info("channel_senders: delivered via '%s'", name)
            except Exception as exc:
                log.error("channel_senders: '%s' failed: %s", name, exc)
                errors[name] = exc
        if errors and len(errors) == len(senders_by_channel):
            raise RuntimeError(f"all channels failed: {errors}")

    return fan_out_sender


# ── Factory ──────────────────────────────────────────────────────────────────

def build_sender_from_config(alerting_config: dict) -> Optional[Callable]:
    alerting_config = alerting_config or {}

    # `channels` (list) is the fan-out form; `channel` (single string) is
    # still accepted and normalized to a one-item list, so existing configs
    # that only ever set one channel don't need to change.
    channels = alerting_config.get("channels")
    if channels is None:
        single = alerting_config.get("channel")
        channels = [single] if single else []

    if not channels:
        log.info(
            "channel_senders: no channel(s) configured under alerting — "
            "events will be logged only, not delivered"
        )
        return None  # NotificationService treats a None sender as "log only"

    senders = {}
    for name in channels:
        key = str(name).lower()
        channel_cfg = alerting_config.get(key, {})
        builder = CHANNEL_BUILDERS.get(key)

        if builder is None:
            # Not a name this file knows — but "channels could be anything"
            # by design, so fall back to the generic webhook sender instead
            # of just giving up, as long as this channel's own config block
            # gives it a url. This is what lets a user configure a channel
            # (Discord, Opsgenie, a custom internal system, anything) that
            # this codebase has never been taught about by name.
            if isinstance(channel_cfg, dict) and channel_cfg.get("url"):
                builder = build_webhook_sender
                log.info(
                    "channel_senders: '%s' isn't a named channel — delivering "
                    "via generic webhook since alerting.%s.url is set",
                    name, key,
                )
            else:
                log.warning(
                    "channel_senders: unknown channel '%s' (supported: %s — or "
                    "any channel name at all, as long as alerting.%s.url is set, "
                    "which delivers it as a generic webhook) — skipping",
                    name, ", ".join(sorted(CHANNEL_BUILDERS)), key,
                )
                continue

        try:
            senders[key] = builder(channel_cfg)
            log.info("channel_senders: wired channel '%s'", key)
        except ValueError as exc:
            # builder raised because a required setting (e.g. topic_arn) is missing
            log.warning("channel_senders: %s — skipping this channel", exc)

    if not senders:
        return None
    if len(senders) == 1:
        return next(iter(senders.values()))  # single channel — no fan-out wrapper needed
    return _fan_out(senders)


def create_notification_service(alerting_config: dict) -> NotificationService:
    # the one function that imports both this module and NotificationService —
    # keeps NotificationService itself from ever needing to know about channels
    sender = build_sender_from_config(alerting_config)
    return NotificationService(alerting_config, sender=sender)
