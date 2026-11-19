"""
tests/test_alerts_channel_senders.py
──────────────────────────────────────
Tests for alerts/channel_senders.py + alerts/notification_service.py.

Covers:
  - build_sender_from_config(): no channel / unknown channel / missing
    required settings all fail safe (return None, no exception).
  - The sns channel sender actually calls boto3 sns.publish() with the
    right TopicArn/Subject/Message (verified via an SNS -> SQS
    subscription, the standard moto pattern for asserting on published
    SNS messages).
  - create_notification_service() end-to-end: config in, real delivery
    out, for the two events currently enabled in config.yaml
    (ingestion_failure / ingestion_success — "job failure" and "job
    success"), and confirms a disabled event type is *not* delivered.

This is deliberately channel-agnostic in spirit: it proves the sns path
works, but NotificationService itself is exercised the same way regardless
of channel — swapping `channel: sns` for `channel: slack` in config only
changes which builder in CHANNEL_BUILDERS gets used.
"""

import json
import time
from unittest.mock import patch, MagicMock

import boto3
import pytest
from moto import mock_aws
from moto.ses import ses_backends

from alerts.channel_senders import (
    build_sender_from_config,
    build_sns_sender,
    build_ses_sender,
    build_sqs_sender,
    build_slack_sender,
    build_teams_sender,
    build_email_sender,
    build_pagerduty_sender,
    build_webhook_sender,
    create_notification_service,
    CHANNEL_BUILDERS,
)

SES_TEST_ACCOUNT_ID = "123456789012"  # moto's default mock account id


# ── build_sender_from_config: fail-safe behavior ─────────────────────────────

def test_no_channel_configured_returns_none():
    assert build_sender_from_config({}) is None
    assert build_sender_from_config({"enabled": True}) is None


def test_unknown_channel_returns_none():
    assert build_sender_from_config({"channel": "carrier_pigeon"}) is None


def test_sns_missing_topic_arn_returns_none():
    # channel selected, but its required setting is missing -> fail safe,
    # not an exception
    assert build_sender_from_config({"channel": "sns", "sns": {}}) is None


def test_channel_builders_registry_has_expected_channels():
    assert set(CHANNEL_BUILDERS) >= {"sns", "slack", "teams", "email", "mail", "pagerduty"}


# ── SNS sender: verify actual delivery via SNS -> SQS subscription ──────────

def _make_topic_with_sqs_subscription(region="us-east-1"):
    """
    Standard moto pattern for asserting on SNS publishes: subscribe an SQS
    queue to the topic, then read the queue afterwards.
    """
    sns = boto3.client("sns", region_name=region)
    sqs = boto3.client("sqs", region_name=region)

    topic_arn = sns.create_topic(Name="goml-aimatic-alert")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="alert-test-queue")["QueueUrl"]
    queue_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    queue_arn = queue_attrs["Attributes"]["QueueArn"]

    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    return topic_arn, sqs, queue_url


def _drain_queue(sqs, queue_url, expected=1, timeout=5):
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        messages.extend(resp.get("Messages", []))
    return messages


@mock_aws
def test_sns_sender_publishes_expected_subject_and_message():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()

    sender = build_sns_sender({"topic_arn": topic_arn, "region": "us-east-1"})
    sender(subject="[FAILURE] test", message="body text", payload={"event_type": "x"})

    received = _drain_queue(sqs, queue_url, expected=1)
    assert len(received) == 1

    body = json.loads(received[0]["Body"])
    assert body["Subject"] == "[FAILURE] test"
    assert body["Message"] == "body text"


@mock_aws
def test_sns_sender_truncates_subject_to_100_chars():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    sender = build_sns_sender({"topic_arn": topic_arn, "region": "us-east-1"})

    long_subject = "x" * 150
    sender(subject=long_subject, message="body", payload={})

    received = _drain_queue(sqs, queue_url, expected=1)
    body = json.loads(received[0]["Body"])
    assert len(body["Subject"]) == 100


# ── create_notification_service(): end-to-end from config.yaml's shape ──────

def _sns_alerting_config(topic_arn, on=None):
    return {
        "enabled": True,
        "channel": "sns",
        "notify_on": on if on is not None else {
            "ingestion_failure": True,
            "ingestion_success": True,
        },
        "sns": {"topic_arn": topic_arn, "region": "us-east-1"},
    }


@mock_aws
def test_notify_ingestion_failure_delivers_via_sns():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    notifier = create_notification_service(_sns_alerting_config(topic_arn))

    notifier.notify("ingestion_failure", source="employee_details.csv", error="boom")

    received = _drain_queue(sqs, queue_url, expected=1)
    assert len(received) == 1
    body = json.loads(received[0]["Body"])
    assert "FAILURE" in body["Subject"]
    assert "boom" in body["Message"]
    assert "employee_details.csv" in body["Message"]


@mock_aws
def test_notify_ingestion_success_delivers_via_sns():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    notifier = create_notification_service(_sns_alerting_config(topic_arn))

    notifier.notify("ingestion_success", source="employee_details.csv", record_count=999, duration_seconds=1.23)

    received = _drain_queue(sqs, queue_url, expected=1)
    assert len(received) == 1
    body = json.loads(received[0]["Body"])
    assert "SUCCESS" in body["Subject"]
    assert "999" in body["Message"]


@mock_aws
def test_disabled_event_type_is_not_delivered():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    # Only ingestion_failure enabled — matches "on: {x: true, y: false}"
    notifier = create_notification_service(
        _sns_alerting_config(topic_arn, on={"ingestion_failure": True, "ingestion_success": False})
    )

    notifier.notify("ingestion_success", source="employee_details.csv", record_count=1)

    received = _drain_queue(sqs, queue_url, expected=1, timeout=2)
    assert received == [], "ingestion_success is disabled — nothing should have been delivered"


@mock_aws
def test_alerting_disabled_entirely_short_circuits_before_any_send():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    cfg = _sns_alerting_config(topic_arn)
    cfg["enabled"] = False
    notifier = create_notification_service(cfg)

    notifier.notify("ingestion_failure", source="x", error="y")

    received = _drain_queue(sqs, queue_url, expected=1, timeout=2)
    assert received == []


# ── SES sender: verify actual delivery via moto's SES backend ───────────────

def _last_ses_message(region="us-east-1"):
    return ses_backends[SES_TEST_ACCOUNT_ID][region].sent_messages[-1]


@mock_aws
def test_ses_sender_sends_expected_subject_and_message():
    ses = boto3.client("ses", region_name="us-east-1")
    ses.verify_email_identity(EmailAddress="alerts@example.com")

    sender = build_ses_sender({
        "from_addr": "alerts@example.com",
        "to_addrs": ["nadhiya.c@goml.io"],
        "region": "us-east-1",
    })
    sender(subject="[SUCCESS] test", message="body text", payload={})

    sent = _last_ses_message()
    assert sent.source == "alerts@example.com"
    assert sent.subject == "[SUCCESS] test"
    assert sent.body == "body text"
    assert sent.destinations["ToAddresses"] == ["nadhiya.c@goml.io"]


def test_ses_missing_config_returns_none():
    assert build_sender_from_config({"channel": "ses", "ses": {}}) is None
    assert build_sender_from_config({"channel": "ses", "ses": {"from_addr": "a@b.com"}}) is None


# ── Fan-out: multiple channels for one event ─────────────────────────────────

def _fan_out_alerting_config(topic_arn, from_addr, to_addrs):
    return {
        "enabled": True,
        "channels": ["sns", "ses"],  # list form -> fan-out
        "notify_on": {"ingestion_failure": True, "ingestion_success": True},
        "sns": {"topic_arn": topic_arn, "region": "us-east-1"},
        "ses": {"from_addr": from_addr, "to_addrs": to_addrs, "region": "us-east-1"},
    }


@mock_aws
def test_fan_out_delivers_same_event_via_both_sns_and_ses():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    boto3.client("ses", region_name="us-east-1").verify_email_identity(
        EmailAddress="alerts@example.com"
    )

    notifier = create_notification_service(
        _fan_out_alerting_config(topic_arn, "alerts@example.com", ["nadhiya.c@goml.io"])
    )
    notifier.notify("ingestion_success", source="employee_details.csv", record_count=1000)

    # SNS side
    received = _drain_queue(sqs, queue_url, expected=1)
    assert len(received) == 1
    assert "SUCCESS" in json.loads(received[0]["Body"])["Subject"]

    # SES side
    sent = _last_ses_message()
    assert sent.destinations["ToAddresses"] == ["nadhiya.c@goml.io"]
    assert "SUCCESS" in sent.subject


@mock_aws
def test_fan_out_one_channel_failing_does_not_block_the_other():
    topic_arn, sqs, queue_url = _make_topic_with_sqs_subscription()
    # deliberately NOT verifying the SES identity -> moto rejects that send

    notifier = create_notification_service(
        _fan_out_alerting_config(topic_arn, "unverified@example.com", ["nadhiya.c@goml.io"])
    )
    notifier.notify("ingestion_failure", source="employee_details.csv", error="boom")

    # SNS still got it even though SES failed
    received = _drain_queue(sqs, queue_url, expected=1)
    assert len(received) == 1


@mock_aws
def test_fan_out_all_channels_failing_raises_and_is_logged(caplog):
    cfg = {
        "enabled": True,
        "channels": ["sns", "ses"],
        "notify_on": {"ingestion_failure": True},
        "sns": {"topic_arn": "arn:aws:sns:us-east-1:123456789012:does-not-exist", "region": "us-east-1"},
        "ses": {"from_addr": "unverified@example.com", "to_addrs": ["x@example.com"], "region": "us-east-1"},
    }
    notifier = create_notification_service(cfg)

    # Both channels fail. notify() itself must not raise — a reporting
    # failure can't be allowed to crash the pipeline it's reporting on —
    # but both the per-channel and the overall failure must be logged so
    # a totally-broken alert config isn't silently invisible.
    with caplog.at_level("ERROR"):
        notifier.notify("ingestion_failure", source="x", error="y")

    assert "'sns' failed" in caplog.text
    assert "'ses' failed" in caplog.text
    assert "sender failed for 'ingestion_failure'" in caplog.text


# ── Slack ────────────────────────────────────────────────────────────────────

def test_slack_sender_requires_webhook_url():
    with pytest.raises(ValueError, match="webhook_url"):
        build_slack_sender({})


def test_slack_sender_posts_expected_shape():
    sender = build_slack_sender({"webhook_url": "https://hooks.slack.com/services/x/y/z"})
    with patch("requests.post") as mock_post:
        sender(subject="[FAILURE] job", message="line one\nline two", payload={})

    args, kwargs = mock_post.call_args
    assert args[0] == "https://hooks.slack.com/services/x/y/z"
    assert "*[FAILURE] job*" in kwargs["json"]["text"]
    assert "line one\nline two" in kwargs["json"]["text"]
    assert kwargs["timeout"] == 10


# ── Teams ────────────────────────────────────────────────────────────────────

def test_teams_sender_requires_webhook_url():
    with pytest.raises(ValueError, match="webhook_url"):
        build_teams_sender({})


def test_teams_sender_posts_expected_shape():
    sender = build_teams_sender({"webhook_url": "https://outlook.office.com/webhook/x"})
    with patch("requests.post") as mock_post:
        sender(subject="[SUCCESS] job", message="line one\nline two", payload={})

    args, kwargs = mock_post.call_args
    assert args[0] == "https://outlook.office.com/webhook/x"
    body = kwargs["json"]
    assert body["@type"] == "MessageCard"
    assert body["summary"] == "[SUCCESS] job"
    assert body["sections"][0]["activityTitle"] == "[SUCCESS] job"
    assert "line one<br>line two" in body["sections"][0]["text"]  # \n -> <br> for Teams


# ── Email (SMTP) ─────────────────────────────────────────────────────────────

def test_email_sender_requires_smtp_host_and_to_addrs():
    with pytest.raises(ValueError, match="smtp_host"):
        build_email_sender({})
    with pytest.raises(ValueError, match="smtp_host"):
        build_email_sender({"smtp_host": "smtp.example.com"})  # no to_addrs


def test_email_sender_sends_via_smtp_with_tls_and_login():
    sender = build_email_sender({
        "smtp_host": "smtp.example.com", "smtp_port": 587,
        "username": "alerts@example.com", "password": "secret",
        "to_addrs": ["oncall@example.com"],
    })
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp
        sender(subject="[FAILURE] job", message="body text", payload={})

    mock_smtp_cls.assert_called_once_with("smtp.example.com", 587, timeout=10)
    mock_smtp.starttls.assert_called_once()
    mock_smtp.login.assert_called_once_with("alerts@example.com", "secret")
    send_args = mock_smtp.sendmail.call_args[0]
    assert send_args[0] == "alerts@example.com"
    assert send_args[1] == ["oncall@example.com"]
    assert "body text" in send_args[2]


def test_email_sender_skips_login_without_credentials():
    sender = build_email_sender({"smtp_host": "smtp.example.com", "to_addrs": ["x@example.com"]})
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp
        sender(subject="s", message="m", payload={})
    mock_smtp.login.assert_not_called()


# ── SQS ──────────────────────────────────────────────────────────────────────

def test_sqs_sender_requires_queue_url():
    with pytest.raises(ValueError, match="queue_url"):
        build_sqs_sender({})


@mock_aws
def test_sqs_sender_delivers_full_structured_payload():
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = sqs.create_queue(QueueName="aimatic-alerts")["QueueUrl"]

    sender = build_sqs_sender({"queue_url": queue_url, "region": "us-east-1"})
    sender(subject="[FAILURE] job", message="body",
           payload={"event_type": "ingestion_failure", "subject": "[FAILURE] job"})

    received = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1,
                                    WaitTimeSeconds=0, MessageAttributeNames=["All"])
    messages = received.get("Messages", [])
    assert len(messages) == 1
    body = json.loads(messages[0]["Body"])
    assert body["event_type"] == "ingestion_failure"
    attrs = messages[0]["MessageAttributes"]
    assert attrs["event_type"]["StringValue"] == "ingestion_failure"


# ── PagerDuty ────────────────────────────────────────────────────────────────

def test_pagerduty_sender_requires_routing_key():
    with pytest.raises(ValueError, match="routing_key"):
        build_pagerduty_sender({})


def test_pagerduty_sender_posts_events_api_shape_and_maps_severity():
    sender = build_pagerduty_sender({"routing_key": "R123"})
    with patch("requests.post") as mock_post:
        sender(subject="[FAILURE] job", message="body",
               payload={"severity": "critical", "context": {"source": "employee_details.csv"}})

    args, kwargs = mock_post.call_args
    assert args[0] == "https://events.pagerduty.com/v2/enqueue"
    body = kwargs["json"]
    assert body["routing_key"] == "R123"
    assert body["event_action"] == "trigger"
    assert body["payload"]["summary"] == "[FAILURE] job"
    assert body["payload"]["severity"] == "critical"  # our "critical" maps straight through
    assert body["payload"]["source"] == "employee_details.csv"


def test_pagerduty_sender_maps_high_to_error():
    sender = build_pagerduty_sender({"routing_key": "R123"})
    with patch("requests.post") as mock_post:
        sender(subject="s", message="m", payload={"severity": "high"})
    assert mock_post.call_args[1]["json"]["payload"]["severity"] == "error"


def test_pagerduty_sender_defaults_unknown_severity_to_warning():
    sender = build_pagerduty_sender({"routing_key": "R123"})
    with patch("requests.post") as mock_post:
        sender(subject="s", message="m", payload={})  # no severity key at all
    assert mock_post.call_args[1]["json"]["payload"]["severity"] == "warning"


# ── Generic webhook sender: "channels could be anything" ────────────────────

def test_webhook_sender_requires_url():
    with pytest.raises(ValueError, match="url"):
        build_webhook_sender({})


def test_webhook_sender_defaults_to_posting_full_payload():
    sender = build_webhook_sender({"url": "https://example.com/hook"})
    with patch("requests.request") as mock_request:
        sender(subject="[FAILURE] x", message="body text",
               payload={"event_type": "ingestion_failure", "subject": "[FAILURE] x"})

    args, kwargs = mock_request.call_args
    assert args[0] == "POST"
    assert args[1] == "https://example.com/hook"
    assert kwargs["json"] == {"event_type": "ingestion_failure", "subject": "[FAILURE] x"}
    assert kwargs["timeout"] == 10


def test_webhook_sender_respects_method_and_headers():
    sender = build_webhook_sender({
        "url": "https://example.com/hook",
        "method": "put",
        "headers": {"Authorization": "Bearer secret-token"},
    })
    with patch("requests.request") as mock_request:
        sender(subject="s", message="m", payload={})

    args, kwargs = mock_request.call_args
    assert args[0] == "PUT"
    assert kwargs["headers"] == {"Authorization": "Bearer secret-token"}


def test_webhook_sender_body_template_substitutes_placeholders():
    # Discord-shaped webhook — a platform this codebase has no named
    # builder for, handled purely through a configurable template.
    sender = build_webhook_sender({
        "url": "https://discord.com/api/webhooks/x/y",
        "body_template": {"content": "$message", "username": "AIMATIC Alerts"},
    })
    with patch("requests.request") as mock_request:
        sender(subject="[FAILURE] job", message="Ingestion FAILURE\nSource: x", payload={})

    _, kwargs = mock_request.call_args
    assert kwargs["json"] == {
        "content": "Ingestion FAILURE\nSource: x",
        "username": "AIMATIC Alerts",
    }


def test_webhook_sender_body_template_handles_special_characters_safely():
    # Message text containing quotes/braces must never break template
    # rendering — _render_template works on native dicts, not JSON strings,
    # so there's nothing to escape.
    sender = build_webhook_sender({
        "url": "https://example.com/hook",
        "body_template": {"text": "$message"},
    })
    tricky_message = 'Error: {"code": 500, "reason": "bad \\"quotes\\""}'
    with patch("requests.request") as mock_request:
        sender(subject="s", message=tricky_message, payload={})

    _, kwargs = mock_request.call_args
    assert kwargs["json"] == {"text": tricky_message}


# ── Automatic fallback: an unnamed channel still works if it has a url ──────

def test_unknown_channel_without_url_still_returns_none():
    # No url given — still fails safe, doesn't guess
    assert build_sender_from_config({
        "channel": "carrier_pigeon", "carrier_pigeon": {"nest": "oak-tree"}
    }) is None


def test_unknown_channel_with_url_falls_back_to_webhook():
    cfg = {
        "channel": "discord",
        "discord": {"url": "https://discord.com/api/webhooks/x/y",
                    "body_template": {"content": "$message"}},
    }
    sender = build_sender_from_config(cfg)
    assert sender is not None

    with patch("requests.request") as mock_request:
        sender(subject="s", message="hello", payload={})

    args, kwargs = mock_request.call_args
    assert args[1] == "https://discord.com/api/webhooks/x/y"
    assert kwargs["json"] == {"content": "hello"}


def test_explicit_webhook_channel_name_also_works():
    cfg = {"channel": "webhook", "webhook": {"url": "https://example.com/generic"}}
    sender = build_sender_from_config(cfg)
    assert sender is not None

    with patch("requests.request") as mock_request:
        sender(subject="s", message="m", payload={"k": "v"})

    assert mock_request.call_args[0][1] == "https://example.com/generic"


def test_notification_service_delivers_through_a_totally_unnamed_channel():
    # Full round-trip: config -> NotificationService -> an arbitrary
    # channel name this codebase has never heard of -> delivered.
    cfg = {
        "enabled": True,
        "channel": "opsgenie",
        "notify_on": {"ingestion_failure": True},
        "opsgenie": {"url": "https://api.opsgenie.com/v2/alerts",
                     "headers": {"Authorization": "GenieKey secret"},
                     "body_template": {"message": "$subject", "description": "$message"}},
    }
    notifier = create_notification_service(cfg)

    with patch("requests.request") as mock_request:
        notifier.notify("ingestion_failure", source="employee_details.csv", error="boom")

    assert mock_request.called
    args, kwargs = mock_request.call_args
    assert args[1] == "https://api.opsgenie.com/v2/alerts"
    assert kwargs["headers"]["Authorization"] == "GenieKey secret"
    assert "boom" in kwargs["json"]["description"]
