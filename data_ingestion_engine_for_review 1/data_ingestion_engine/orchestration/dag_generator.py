"""
orchestration/dag_generator.py
------------------------------

Airflow DAG generator — same output for AWS MWAA (managed) and a
self-hosted, open-source Airflow. Both run the exact same DAG file; the
only difference is deployment (upload to MWAA's S3 dags/ bucket vs. copy
into a self-hosted Airflow's dags/ folder) and which AWS credentials that
environment has (MWAA's execution role, automatic; self-hosted needs its
own IAM role or an Airflow Connection) — see orchestration/generate_dag.py
for the CLI that produces and deploys one.

Generates a single, architecture-independent DAG that triggers the
aimatic Glue job with whichever parameters you choose. The Glue job does
all the real work (read -> architecture -> write -> alert); this DAG's
only job is to trigger it on a schedule and watch it.

WHY A GENERIC glue_arguments DICT, NOT NAMED reader=/writer=/... KWARGS:
The Glue job's own argument surface changes per writer (cloud needs
bucket/prefix; data_warehouse needs cluster_identifier/database/table/...;
database needs db_type/host/...) and keeps growing (kappa_window_seconds,
logger, ...). Re-enumerating every one of those as a named Python
parameter here is exactly what went stale last time — this file predated
the warehouse/database writers and kappa/data_vault architectures, so it
could only ever drive the cloud writer. Passing the real --key value dict
straight through means this generator never needs to change again when a
new writer parameter is added — the same reasoning
core/pipeline_builder.py's own --writer_<x> passthrough already uses.

ALERTING:
  - Airflow's built-in email_on_failure is always available (SES in MWAA,
    or your self-hosted instance's own SMTP config)
  - A custom channel (optional) delivers richer context (reader,
    architecture, writer, run id, duration) via the same 8 named channels
    alerts/channel_senders.py supports (sns, ses, sqs, slack, teams, email,
    pagerduty) PLUS a generic webhook fallback for any other channel name
    (Discord, Opsgenie, a custom endpoint, ...) as long as it has a url —
    the identical "any channel" design alerts/channel_senders.py uses.
    Deliberately inlined into the generated DAG rather than imported from
    this codebase, so the DAG stays a single self-contained file with no
    extra package to install in the Airflow environment (MWAA
    requirements.txt, or a self-hosted worker's site-packages) — the same
    self-contained-file choice this generator already made for
    start_glue_job/monitor_glue_job.

GENERATED DAG FLOW:
  validate_params -> start_glue_job -> monitor_glue_job -> notify (optional)

USAGE:

  from orchestration.dag_generator import generate_dag

  dag_content = generate_dag(
      dag_id="batch_cloud_lambda_warehouse",
      glue_job_name="aimatic-structured-test",
      glue_arguments={
          "--config_path": "s3://goml-aimatic-de-config/config.yaml",
          "--reader": "batch_cloud",
          "--architecture": "lambda",
          "--writer": "data_warehouse",
          "--writer_warehouse_type": "redshift",
          "--writer_cluster_identifier": "my-cluster",
          "--writer_database": "analytics",
          "--writer_db_user": "glue_iam_user",
          "--writer_table": "events",
          "--writer_mode": "merge",
          "--writer_merge_keys": "id",
          "--writer_staging_bucket": "my-staging-bucket",
          "--writer_iam_role": "arn:aws:iam::123456789012:role/redshift-copy-role",
      },
      alert_channel="slack",
      alert_config={"webhook_url": "https://hooks.slack.com/services/..."},
  )

See orchestration/generate_dag.py for a CLI that builds glue_arguments
from --reader/--architecture/--writer/--writer_<x>-style flags directly
(mirroring the Glue job's own argument style) and deploys the result.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict

log = logging.getLogger(__name__)


# =============================================================================
# DAG TEMPLATE
# =============================================================================

DAG_TEMPLATE = '''"""
Auto-generated Airflow DAG: {dag_id}
Generated at: {generated_at}

Glue job   : {glue_job_name}
Arguments  : {glue_arguments_summary}
Alerts     : Airflow email (built-in){custom_alert_desc}

Runs identically on AWS MWAA (managed) or a self-hosted, open-source
Airflow — deployment is the only difference, see
orchestration/generate_dag.py.
"""

import json
import logging
import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION (generated from your --reader/--architecture/--writer_<x> choices)
# ═══════════════════════════════════════════════════════════════════════════════

GLUE_JOB_NAME = "{glue_job_name}"
REGION = "{region}"
DAG_ID = "{dag_id}"

# The exact --key value arguments this run passes to the Glue job — the
# same shape as `aws glue start-job-run --arguments '{{...}}'`. Whatever
# --reader/--architecture/--writer/--writer_<x>/--kappa_<x>/--logger/
# --ecs_endpoint flags you chose live here verbatim; this file never
# special-cases any of them.
GLUE_ARGUMENTS = {glue_arguments_json}

# Alert configuration (None = Airflow email only, no custom channel)
ALERT_CHANNEL = {alert_channel_repr}
ALERT_CONFIG = {alert_config_repr}


# ═══════════════════════════════════════════════════════════════════════════════
# ALERT SENDERS — same 8 named channels + generic webhook fallback as
# alerts/channel_senders.py, inlined so this DAG stays a single
# self-contained file (no extra package needed in the Airflow environment)
# ═══════════════════════════════════════════════════════════════════════════════

def _render_template(template, subject, message):
    """Walk a dict/list/string template, substituting the literal
    placeholders "$subject"/"$message" — native Python values, not a JSON
    string, so nothing needs escaping no matter what the message contains."""
    if isinstance(template, dict):
        return {{k: _render_template(v, subject, message) for k, v in template.items()}}
    if isinstance(template, list):
        return [_render_template(v, subject, message) for v in template]
    if template == "$subject":
        return subject
    if template == "$message":
        return message
    if isinstance(template, str):
        return template.replace("$subject", subject).replace("$message", message)
    return template


def _build_sender(channel, cfg):
    """Returns a sender(subject, message, payload) callable for the named
    channel, or a generic webhook sender if the name isn't one of the
    8 below but cfg has a url — the same "any channel" fallback
    alerts/channel_senders.py uses. Returns None if nothing usable is
    configured."""
    cfg = cfg or {{}}

    if channel == "sns":
        topic_arn = cfg.get("topic_arn")
        if not topic_arn:
            return None
        def sender(subject, message, payload):
            boto3.client("sns", region_name=REGION).publish(
                TopicArn=topic_arn, Subject=subject[:100], Message=message)
        return sender

    if channel == "ses":
        from_addr, to_addrs = cfg.get("from_addr"), cfg.get("to_addrs") or []
        if not from_addr or not to_addrs:
            return None
        def sender(subject, message, payload):
            boto3.client("ses", region_name=REGION).send_email(
                Source=from_addr, Destination={{"ToAddresses": to_addrs}},
                Message={{"Subject": {{"Data": subject[:998]}}, "Body": {{"Text": {{"Data": message}}}}}})
        return sender

    if channel == "sqs":
        queue_url = cfg.get("queue_url")
        if not queue_url:
            return None
        def sender(subject, message, payload):
            boto3.client("sqs", region_name=REGION).send_message(
                QueueUrl=queue_url, MessageBody=json.dumps(payload),
                MessageAttributes={{"event_type": {{"DataType": "String",
                                    "StringValue": payload.get("event_type", "unknown")}}}})
        return sender

    if channel in ("slack",):
        webhook_url = cfg.get("webhook_url")
        if not webhook_url:
            return None
        def sender(subject, message, payload):
            import requests
            requests.post(webhook_url, json={{"text": f"*{{subject}}*\\n```{{message}}```"}}, timeout=10)
        return sender

    if channel == "teams":
        webhook_url = cfg.get("webhook_url")
        if not webhook_url:
            return None
        def sender(subject, message, payload):
            import requests
            requests.post(webhook_url, json={{
                "@type": "MessageCard", "summary": subject,
                "sections": [{{"activityTitle": subject, "text": message.replace("\\n", "<br>")}}],
            }}, timeout=10)
        return sender

    if channel in ("email", "mail"):
        smtp_host, to_addrs = cfg.get("smtp_host"), cfg.get("to_addrs") or []
        if not smtp_host or not to_addrs:
            return None
        def sender(subject, message, payload):
            import smtplib
            from email.mime.text import MIMEText
            smtp_port = cfg.get("smtp_port", 587)
            username, password = cfg.get("username"), cfg.get("password")
            from_addr = cfg.get("from_addr") or username
            msg = MIMEText(message)
            msg["Subject"], msg["From"], msg["To"] = subject, from_addr, ", ".join(to_addrs)
            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as smtp:
                if cfg.get("use_tls", True):
                    smtp.starttls()
                if username and password:
                    smtp.login(username, password)
                smtp.sendmail(from_addr, to_addrs, msg.as_string())
        return sender

    if channel == "pagerduty":
        routing_key = cfg.get("routing_key")
        if not routing_key:
            return None
        def sender(subject, message, payload):
            import requests
            severity_map = {{"critical": "critical", "high": "error", "medium": "warning", "low": "info"}}
            requests.post("https://events.pagerduty.com/v2/enqueue", json={{
                "routing_key": routing_key, "event_action": "trigger",
                "payload": {{"summary": subject,
                            "source": payload.get("context", {{}}).get("source", DAG_ID),
                            "severity": severity_map.get(payload.get("severity", "medium"), "warning"),
                            "custom_details": payload}},
            }}, timeout=10)
        return sender

    if channel == "webhook" or cfg.get("url"):
        url = cfg.get("url")
        if not url:
            return None
        def sender(subject, message, payload):
            import requests
            body_template = cfg.get("body_template")
            body = _render_template(body_template, subject, message) if body_template else payload
            requests.request(cfg.get("method", "POST").upper(), url, json=body,
                             headers=cfg.get("headers") or {{}}, timeout=10)
        return sender

    log.warning("Unknown alert channel '%s' with no url for generic webhook delivery — "
                "alerts will be logged only", channel)
    return None


def _send_alert(event_type, **context):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    sep = "-" * 40
    reader = GLUE_ARGUMENTS.get("--reader", "unknown")
    architecture = GLUE_ARGUMENTS.get("--architecture", "unknown")
    writer = GLUE_ARGUMENTS.get("--writer", "unknown")

    if event_type == "ingestion_success":
        subject = f"[SUCCESS] Pipeline Complete - {{DAG_ID}}"
        message = (
            f"Pipeline SUCCESS\\n{{sep}}\\n"
            f"DAG           : {{DAG_ID}}\\n"
            f"Glue Job      : {{GLUE_JOB_NAME}}\\n"
            f"Run ID        : {{context.get('run_id', 'N/A')}}\\n"
            f"Duration      : {{context.get('duration', 'N/A')}}s\\n"
            f"Reader        : {{reader}}\\n"
            f"Architecture  : {{architecture}}\\n"
            f"Writer        : {{writer}}\\n"
            f"Timestamp     : {{ts}}\\n"
        )
    elif event_type == "ingestion_failure":
        subject = f"[FAILURE] Pipeline Failed - {{DAG_ID}}"
        message = (
            f"Pipeline FAILURE\\n{{sep}}\\n"
            f"DAG           : {{DAG_ID}}\\n"
            f"Glue Job      : {{GLUE_JOB_NAME}}\\n"
            f"Run ID        : {{context.get('run_id', 'N/A')}}\\n"
            f"Error         : {{context.get('error', 'Unknown')}}\\n"
            f"Duration      : {{context.get('duration', 'N/A')}}s\\n"
            f"Reader        : {{reader}}\\n"
            f"Architecture  : {{architecture}}\\n"
            f"Writer        : {{writer}}\\n"
            f"Timestamp     : {{ts}}\\n"
            f"\\nAction: Check Glue job logs in CloudWatch.\\n"
        )
    else:
        subject = f"[ALERT] {{event_type}} - {{DAG_ID}}"
        message = f"Event: {{event_type}}\\n{{sep}}\\n" + "\\n".join(
            f"{{k:14}}: {{v}}" for k, v in context.items()
        ) + f"\\nTimestamp     : {{ts}}\\n"

    payload = {{"event_type": event_type, "dag_id": DAG_ID, "glue_job": GLUE_JOB_NAME,
               "timestamp": ts, "context": {{"source": reader}}, **context}}

    if not ALERT_CHANNEL:
        log.info("Alert (log only, no custom channel configured): %s", subject)
        return

    sender = _build_sender(ALERT_CHANNEL, ALERT_CONFIG)
    if sender:
        try:
            sender(subject=subject, message=message, payload=payload)
            log.info("Alert sent via %s: %s", ALERT_CHANNEL, subject)
        except Exception as exc:
            log.error("Failed to send alert via %s: %s", ALERT_CHANNEL, exc)
    else:
        log.info("Alert (log only): %s", subject)


# ═══════════════════════════════════════════════════════════════════════════════
# TASK FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def validate_params(**kwargs):
    """Validate that all required Glue parameters are present."""
    log.info("Validating pipeline parameters...")

    required = ["--config_path", "--reader", "--architecture", "--writer"]
    missing = [k for k in required if not GLUE_ARGUMENTS.get(k)]

    if missing:
        raise AirflowException(f"Missing required parameters: {{missing}}")

    log.info("Parameters valid: %s", json.dumps(GLUE_ARGUMENTS, indent=2))


def start_glue_job(**kwargs):
    """Start the AWS Glue job and push the run ID to XCom."""
    log.info("Starting Glue job: %s", GLUE_JOB_NAME)

    glue = boto3.client("glue", region_name=REGION)

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments=GLUE_ARGUMENTS,
    )

    run_id = response["JobRunId"]
    log.info("Glue job started. Run ID: %s", run_id)

    kwargs["ti"].xcom_push(key="glue_run_id", value=run_id)
    return run_id


def monitor_glue_job(**kwargs):
    """Poll the Glue job until it completes or fails."""
    ti = kwargs["ti"]
    run_id = ti.xcom_pull(task_ids="start_glue_job", key="glue_run_id")

    if not run_id:
        raise AirflowException("No Glue run ID found from start_glue_job task")

    log.info("Monitoring Glue job run: %s", run_id)

    glue = boto3.client("glue", region_name=REGION)
    poll_interval = 15

    while True:
        response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
        job_run = response["JobRun"]
        status = job_run["JobRunState"]

        log.info("Glue job status: %s", status)

        if status == "SUCCEEDED":
            duration = job_run.get("ExecutionTime", 0)
            ti.xcom_push(key="duration_seconds", value=duration)
            log.info("Glue job completed in %ds", duration)
            return "SUCCEEDED"

        elif status in ("FAILED", "STOPPED", "ERROR", "TIMEOUT"):
            error_msg = job_run.get("ErrorMessage", "Unknown error")
            duration = job_run.get("ExecutionTime", 0)
            ti.xcom_push(key="error_message", value=error_msg)
            ti.xcom_push(key="duration_seconds", value=duration)
            raise AirflowException(
                f"Glue job {{GLUE_JOB_NAME}} {{status}}: {{error_msg}}"
            )

        time.sleep(poll_interval)


def notify_success(**kwargs):
    """Send success alert via the configured channel (if any)."""
    ti = kwargs["ti"]
    run_id = ti.xcom_pull(task_ids="start_glue_job", key="glue_run_id") or "N/A"
    duration = ti.xcom_pull(task_ids="monitor_glue_job", key="duration_seconds") or "N/A"

    _send_alert("ingestion_success", run_id=run_id, duration=duration)


def notify_failure(**kwargs):
    """Send failure alert via the configured channel (if any)."""
    ti = kwargs["ti"]
    run_id = ti.xcom_pull(task_ids="start_glue_job", key="glue_run_id") or "N/A"
    error_msg = ti.xcom_pull(task_ids="monitor_glue_job", key="error_message") or "Unknown"
    duration = ti.xcom_pull(task_ids="monitor_glue_job", key="duration_seconds") or "N/A"

    _send_alert("ingestion_failure", run_id=run_id, error=error_msg, duration=duration)


# ═══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════════════════

default_args = {{
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime({start_year}, {start_month}, {start_day}),
    "retries": {retries},
    "retry_delay": timedelta(minutes={retry_delay_minutes}),
    # Built-in Airflow email alert — always active (SES in MWAA, or your
    # self-hosted instance's own SMTP config).
    "email_on_failure": {email_on_failure},
    "email_on_retry": False,
    "email": {email_list_repr},
}}

with DAG(
    dag_id="{dag_id}",
    default_args=default_args,
    description="{description}",
    schedule_interval="{schedule}",
    catchup=False,
    max_active_runs=1,
    tags=["aimatic", "{reader}", "{architecture}", "{writer}"],
) as dag:

    t_validate = PythonOperator(
        task_id="validate_params",
        python_callable=validate_params,
    )

    t_start = PythonOperator(
        task_id="start_glue_job",
        python_callable=start_glue_job,
    )

    t_monitor = PythonOperator(
        task_id="monitor_glue_job",
        python_callable=monitor_glue_job,
        execution_timeout=timedelta(minutes={timeout_minutes}),
    )

    t_notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    t_notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        trigger_rule="one_failed",
    )

    # Flow: validate -> start -> monitor -> success/failure
    t_validate >> t_start >> t_monitor >> t_notify_success
    t_monitor >> t_notify_failure
'''


# =============================================================================
# PUBLIC API
# =============================================================================

def generate_dag(
    dag_id: str,
    glue_job_name: str,
    glue_arguments: Dict[str, str],
    schedule: str = "0 22 * * *",
    retries: int = 2,
    retry_delay_minutes: int = 5,
    timeout_minutes: int = 60,
    region: str = "us-east-1",
    alert_channel: Optional[str] = None,
    alert_config: Optional[Dict] = None,
    email_on_failure: bool = True,
    alert_emails: Optional[list] = None,
    description: Optional[str] = None,
) -> str:
    """
    Generate an Airflow DAG file (.py) that triggers the aimatic Glue job.

    Works identically for any reader/architecture/writer combination,
    structured or unstructured — glue_arguments is passed through to
    start_job_run() verbatim, so this function never needs to know what's
    inside it (see the module docstring for why that's deliberate).

    Args:
        dag_id: Unique DAG identifier (e.g., "batch_cloud_lambda_warehouse")
        glue_job_name: Name of the Glue job to trigger (required — no
            default, since a wrong default here would run the wrong job)
        glue_arguments: The exact --key value arguments for this run, e.g.
            {{"--config_path": "...", "--reader": "batch_cloud",
              "--architecture": "lambda", "--writer": "cloud",
              "--writer_bucket": "...", ...}} — anything the Glue job
            itself accepts, including unstructured mode's --ecs_endpoint.
        schedule: Cron schedule (default: daily at 10 PM UTC)
        retries: Number of retry attempts on failure
        retry_delay_minutes: Wait between retries
        timeout_minutes: Max time to wait for the Glue job
        region: AWS region for the Glue API calls (start_job_run/get_job_run)
        alert_channel: Optional custom alert channel — any of
            alerts/channel_senders.py's named channels (sns, ses, sqs,
            slack, teams, email, pagerduty) or any other name at all, as
            long as alert_config has a url (generic webhook fallback)
        alert_config: Channel-specific config dict — same shape as that
            channel's alerting.<channel> block in config.yaml
        email_on_failure: Enable Airflow's built-in email alert (default: True)
        alert_emails: List of email addresses for Airflow's built-in alert
        description: DAG description shown in the Airflow UI (default:
            auto-generated from reader/architecture/writer)

    Returns:
        String containing the complete DAG Python file content.
    """
    now = datetime.utcnow()
    alert_config = alert_config or {}
    alert_emails = alert_emails or ["data-team@company.com"]

    reader = glue_arguments.get("--reader", "unknown")
    architecture = glue_arguments.get("--architecture", "unknown")
    writer = glue_arguments.get("--writer", "unknown")

    custom_alert_desc = f" + {alert_channel} (custom)" if alert_channel else ""
    glue_arguments_summary = ", ".join(f"{k}={v}" for k, v in glue_arguments.items())
    final_description = description or f"Pipeline: {reader} -> {architecture} -> {writer}"

    dag_content = DAG_TEMPLATE.format(
        dag_id=dag_id,
        generated_at=now.isoformat(),
        glue_job_name=glue_job_name,
        glue_arguments_json=json.dumps(glue_arguments, indent=4),
        glue_arguments_summary=glue_arguments_summary,
        reader=reader,
        architecture=architecture,
        writer=writer,
        description=final_description,
        schedule=schedule,
        retries=retries,
        retry_delay_minutes=retry_delay_minutes,
        timeout_minutes=timeout_minutes,
        region=region,
        start_year=now.year,
        start_month=now.month,
        start_day=now.day,
        alert_channel_repr=repr(alert_channel),
        alert_config_repr=repr(alert_config),
        email_on_failure=repr(email_on_failure),
        email_list_repr=repr(alert_emails),
        custom_alert_desc=custom_alert_desc,
    )

    log.info("Generated DAG: %s", dag_id)
    log.info("  Glue job: %s | %s", glue_job_name, glue_arguments_summary)
    log.info("  Schedule: %s", schedule)
    log.info("  Alerts: email=%s, custom=%s", email_on_failure, alert_channel or "none")

    return dag_content


def generate_dag_file(output_path: str, **kwargs) -> str:
    """
    Generate a DAG and write it to a file.

    Args:
        output_path: Local file path to write (e.g., "dags/my_pipeline.py")
        **kwargs: All arguments passed to generate_dag()

    Returns:
        The output file path.
    """
    dag_content = generate_dag(**kwargs)

    import os
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(dag_content)

    log.info("DAG file written to: %s", output_path)
    return output_path
