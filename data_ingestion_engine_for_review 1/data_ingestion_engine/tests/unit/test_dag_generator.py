import ast
import json
import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from orchestration.dag_generator import generate_dag, generate_dag_file

# Safety note: every test below that can reach the generated DAG's
# start_glue_job/monitor_glue_job (which call real boto3.client("glue"))
# is wrapped in @mock_aws, which intercepts at the botocore request layer —
# not just unittest.mock.patch("boto3.client"), which was tried first here
# and, for reasons not fully pinned down (likely a pytest/exec namespace
# interaction), let a real call through during development and actually
# started a real Glue job run in the AWS account before being caught and
# stopped. @mock_aws can't be bypassed the same way: it never lets boto3
# reach the network at all, regardless of how the client object was
# constructed or which namespace called it.


BASE_ARGS = dict(
    dag_id="test_dag",
    glue_job_name="aimatic-structured-test",
    glue_arguments={
        "--config_path": "s3://bucket/config.yaml",
        "--reader": "batch_cloud",
        "--architecture": "lambda",
        "--writer": "cloud",
        "--writer_bucket": "my-bucket",
    },
)


# --- generate_dag(): structure and content ---

def test_generate_dag_produces_valid_python():
    dag_code = generate_dag(**BASE_ARGS)
    ast.parse(dag_code)  # raises SyntaxError if invalid


def test_generate_dag_embeds_exact_glue_arguments():
    dag_code = generate_dag(**BASE_ARGS)
    assert '"--reader": "batch_cloud"' in dag_code
    assert '"--architecture": "lambda"' in dag_code
    assert '"--writer": "cloud"' in dag_code
    assert '"--writer_bucket": "my-bucket"' in dag_code


def test_generate_dag_passes_through_any_future_parameter_unmodified():
    # The whole point of the generic glue_arguments dict: a parameter this
    # file has never heard of (e.g. a brand new writer type added later)
    # still comes through correctly, with zero code changes here.
    args = {**BASE_ARGS, "glue_arguments": {
        **BASE_ARGS["glue_arguments"],
        "--writer_some_future_param": "future_value",
        "--kappa_window_seconds": "45",
        "--logger": "json",
    }}
    dag_code = generate_dag(**args)
    assert '"--writer_some_future_param": "future_value"' in dag_code
    assert '"--kappa_window_seconds": "45"' in dag_code
    assert '"--logger": "json"' in dag_code


def test_generate_dag_supports_unstructured_mode_ecs_endpoint():
    # Regression: the old generator claimed pipeline_mode="unstructured"
    # support but never actually included --ecs_endpoint anywhere.
    args = {**BASE_ARGS, "glue_arguments": {
        "--config_path": "s3://bucket/config.yaml",
        "--ecs_endpoint": "http://docling-service:8000",
        "--writer_bucket": "my-bucket",
    }}
    dag_code = generate_dag(**args)
    assert '"--ecs_endpoint": "http://docling-service:8000"' in dag_code


def test_generate_dag_uses_exact_job_name_no_bad_default():
    # Regression: the old generator defaulted to glue_job_name=
    # "aimatic-unstructured" even for structured-pipeline DAGs.
    dag_code = generate_dag(**BASE_ARGS)
    assert 'GLUE_JOB_NAME = "aimatic-structured-test"' in dag_code
    assert "aimatic-unstructured" not in dag_code


def test_generate_dag_requires_glue_job_name_explicitly():
    import inspect
    sig = inspect.signature(generate_dag)
    assert sig.parameters["glue_job_name"].default is inspect.Parameter.empty


def test_generate_dag_default_description_built_from_reader_architecture_writer():
    dag_code = generate_dag(**BASE_ARGS)
    assert "Pipeline: batch_cloud -> lambda -> cloud" in dag_code


def test_generate_dag_custom_description_overrides_default():
    dag_code = generate_dag(**BASE_ARGS, description="My custom DAG description")
    assert "My custom DAG description" in dag_code
    assert "Pipeline: batch_cloud -> lambda -> cloud" not in dag_code


def test_generate_dag_file_writes_to_disk(tmp_path):
    output_path = str(tmp_path / "subdir" / "my_dag.py")
    result = generate_dag_file(output_path=output_path, **BASE_ARGS)
    assert result == output_path
    with open(output_path) as f:
        content = f.read()
    ast.parse(content)
    assert "my-bucket" in content


# --- Executing the generated DAG with airflow mocked (no real install needed) ---

@pytest.fixture
def exec_generated_dag():
    """Mocks the airflow imports, executes a generated DAG's source, and
    returns its module namespace — so the actual generated alert/task
    logic can be called directly and proven correct, not just inspected
    as a string."""
    def _exec(alert_channel=None, alert_config=None, glue_arguments=None):
        fake_airflow = MagicMock()
        fake_ops = MagicMock()
        fake_exceptions = MagicMock()

        class FakeAirflowException(Exception):
            pass
        fake_exceptions.AirflowException = FakeAirflowException

        with patch.dict(sys.modules, {
            "airflow": fake_airflow,
            "airflow.operators.python": fake_ops,
            "airflow.exceptions": fake_exceptions,
        }):
            dag_code = generate_dag(
                dag_id="exec_test",
                glue_job_name="aimatic-structured-test",
                glue_arguments=glue_arguments or BASE_ARGS["glue_arguments"],
                alert_channel=alert_channel,
                alert_config=alert_config,
            )
            ns = {}
            exec(compile(dag_code, "exec_test_dag.py", "exec"), ns)
            ns["_FakeAirflowException"] = FakeAirflowException
            return ns
    return _exec


def test_executed_dag_validate_params_passes_with_required_fields(exec_generated_dag):
    ns = exec_generated_dag()
    ns["validate_params"]()  # should not raise


def test_executed_dag_validate_params_raises_on_missing_required(exec_generated_dag):
    ns = exec_generated_dag(glue_arguments={"--config_path": "s3://b/c.yaml"})
    with pytest.raises(ns["_FakeAirflowException"]):
        ns["validate_params"]()


@mock_aws
def test_executed_dag_start_glue_job_calls_boto3_with_exact_arguments(exec_generated_dag):
    # Real moto-backed Glue job — start_glue_job's boto3.client("glue")
    # call genuinely executes against moto's simulated API, not a Python
    # mock, so this is immune to any patch-resolution subtlety.
    boto3.client("glue", region_name="us-east-1").create_job(
        Name="aimatic-structured-test",
        Role="arn:aws:iam::123456789012:role/x",
        Command={"Name": "pythonshell", "ScriptLocation": "s3://b/s.py"},
    )
    ns = exec_generated_dag()
    mock_ti = MagicMock()

    run_id = ns["start_glue_job"](ti=mock_ti)

    assert run_id.startswith("jr_")
    mock_ti.xcom_push.assert_called_once_with(key="glue_run_id", value=run_id)

    # Confirm moto actually recorded the real Arguments this call sent.
    recorded = boto3.client("glue", region_name="us-east-1").get_job_run(
        JobName="aimatic-structured-test", RunId=run_id)
    assert recorded["JobRun"]["Arguments"]["--reader"] == "batch_cloud"


@mock_aws
def test_executed_dag_monitor_glue_job_succeeds(exec_generated_dag):
    # moto's simulated job runs always report SUCCEEDED — a genuine,
    # real exercise of the success path, not a hand-built response.
    glue = boto3.client("glue", region_name="us-east-1")
    glue.create_job(Name="aimatic-structured-test", Role="arn:aws:iam::123456789012:role/x",
                     Command={"Name": "pythonshell", "ScriptLocation": "s3://b/s.py"})
    start_resp = glue.start_job_run(JobName="aimatic-structured-test", Arguments={})

    ns = exec_generated_dag()
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = start_resp["JobRunId"]

    status = ns["monitor_glue_job"](ti=mock_ti)

    assert status == "SUCCEEDED"
    assert mock_ti.xcom_push.call_args[1]["key"] == "duration_seconds"


def test_executed_dag_monitor_glue_job_raises_on_failure(exec_generated_dag):
    # moto has no way to simulate a FAILED job run via its public API, so
    # this one case still needs a hand-built response — kept inside
    # @mock_aws regardless, as a hard safety net: even if patch() somehow
    # doesn't take effect, moto guarantees no real network call can happen.
    with mock_aws():
        ns = exec_generated_dag()
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = "jr_fake"
        mock_glue_client = MagicMock()
        mock_glue_client.get_job_run.return_value = {
            "JobRun": {"JobRunState": "FAILED", "ErrorMessage": "boom", "ExecutionTime": 10}
        }

        with patch("boto3.client", return_value=mock_glue_client):
            with pytest.raises(ns["_FakeAirflowException"], match="boom"):
                ns["monitor_glue_job"](ti=mock_ti)


# --- Inlined alert logic: all 8 named channels + generic webhook fallback ---

@pytest.mark.parametrize("channel,cfg,expected_url", [
    ("slack", {"webhook_url": "https://hooks.slack.com/x"}, "https://hooks.slack.com/x"),
    ("teams", {"webhook_url": "https://outlook.office.com/x"}, "https://outlook.office.com/x"),
    ("pagerduty", {"routing_key": "R1"}, "https://events.pagerduty.com/v2/enqueue"),
])
def test_executed_dag_build_sender_for_requests_based_channels(exec_generated_dag, channel, cfg, expected_url):
    # No boto3 involved for these three — patch("requests.post") alone is
    # safe here (no AWS call exists in this code path to leak through).
    ns = exec_generated_dag(alert_channel=channel, alert_config=cfg)
    sender = ns["_build_sender"](channel, cfg)
    assert sender is not None

    with patch("requests.post") as mock_post:
        sender("subject", "message", {"event_type": "ingestion_failure"})

    assert mock_post.call_args[0][0] == expected_url


@mock_aws
def test_executed_dag_build_sender_sns_publishes_for_real_against_moto(exec_generated_dag):
    sns = boto3.client("sns", region_name="us-east-1")
    sqs = boto3.client("sqs", region_name="us-east-1")
    topic_arn = sns.create_topic(Name="aimatic-alert-test")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="aimatic-alert-test-queue")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    ns = exec_generated_dag(alert_channel="sns", alert_config={"topic_arn": topic_arn})
    sender = ns["_build_sender"]("sns", {"topic_arn": topic_arn})
    sender("[FAILURE] test", "body text", {"event_type": "ingestion_failure"})

    received = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
    messages = received.get("Messages", [])
    assert len(messages) == 1
    body = json.loads(messages[0]["Body"])
    assert body["Subject"] == "[FAILURE] test"
    assert body["Message"] == "body text"


@mock_aws
def test_executed_dag_build_sender_sqs_delivers_for_real_against_moto(exec_generated_dag):
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = sqs.create_queue(QueueName="aimatic-alert-direct-queue")["QueueUrl"]

    ns = exec_generated_dag(alert_channel="sqs", alert_config={"queue_url": queue_url})
    sender = ns["_build_sender"]("sqs", {"queue_url": queue_url})
    sender("subject", "message", {"event_type": "ingestion_failure"})

    received = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1,
                                    MessageAttributeNames=["All"])
    messages = received.get("Messages", [])
    assert len(messages) == 1
    assert messages[0]["MessageAttributes"]["event_type"]["StringValue"] == "ingestion_failure"


@mock_aws
def test_executed_dag_build_sender_ses_sends_for_real_against_moto(exec_generated_dag):
    ses = boto3.client("ses", region_name="us-east-1")
    ses.verify_email_identity(EmailAddress="alerts@example.com")
    ses.verify_email_identity(EmailAddress="oncall@example.com")

    cfg = {"from_addr": "alerts@example.com", "to_addrs": ["oncall@example.com"]}
    ns = exec_generated_dag(alert_channel="ses", alert_config=cfg)
    sender = ns["_build_sender"]("ses", cfg)
    sender("[FAILURE] test", "body text", {"event_type": "ingestion_failure"})  # should not raise


def test_executed_dag_build_sender_email_uses_smtplib(exec_generated_dag):
    ns = exec_generated_dag(alert_channel="email",
                             alert_config={"smtp_host": "smtp.x.com", "to_addrs": ["a@x.com"]})
    sender = ns["_build_sender"]("email", {"smtp_host": "smtp.x.com", "to_addrs": ["a@x.com"]})
    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp = MagicMock()
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp
        sender("subject", "message", {})
    mock_smtp.sendmail.assert_called_once()


def test_executed_dag_build_sender_unknown_channel_falls_back_to_webhook(exec_generated_dag):
    # The exact "channels could be anything" requirement — a name this
    # generator has never heard of still works via the generic fallback.
    ns = exec_generated_dag(alert_channel="opsgenie",
                             alert_config={"url": "https://api.opsgenie.com/v2/alerts",
                                          "body_template": {"message": "$subject"}})
    sender = ns["_build_sender"]("opsgenie",
                                  {"url": "https://api.opsgenie.com/v2/alerts",
                                   "body_template": {"message": "$subject"}})
    assert sender is not None
    with patch("requests.request") as mock_request:
        sender("[FAILURE] test", "body text", {})
    args, kwargs = mock_request.call_args
    assert args[1] == "https://api.opsgenie.com/v2/alerts"
    assert kwargs["json"] == {"message": "[FAILURE] test"}


def test_executed_dag_build_sender_unknown_channel_without_url_returns_none(exec_generated_dag):
    ns = exec_generated_dag()
    assert ns["_build_sender"]("carrier_pigeon", {}) is None


def test_executed_dag_render_template_handles_special_characters(exec_generated_dag):
    ns = exec_generated_dag()
    tricky = 'Error: {"code": 500}'
    result = ns["_render_template"]({"text": "$message"}, "subj", tricky)
    assert result == {"text": tricky}


def test_executed_dag_send_alert_logs_only_when_no_channel_configured(exec_generated_dag, caplog):
    import logging
    ns = exec_generated_dag(alert_channel=None)
    with caplog.at_level(logging.INFO):
        ns["_send_alert"]("ingestion_success", run_id="jr_1", duration=5)
    assert any("log only" in m for m in caplog.messages)


def test_executed_dag_send_alert_includes_reader_architecture_writer(exec_generated_dag):
    # Mirrors the pipeline's own alert-message fix — DAG-level alerts
    # should carry the same context.
    ns = exec_generated_dag(alert_channel="slack", alert_config={"webhook_url": "https://hooks.slack.com/x"})
    with patch("requests.post") as mock_post:
        ns["_send_alert"]("ingestion_failure", run_id="jr_1", error="boom", duration=5)
    sent_message = mock_post.call_args[1]["json"]["text"]
    assert "batch_cloud" in sent_message
    assert "lambda" in sent_message
    assert "cloud" in sent_message
