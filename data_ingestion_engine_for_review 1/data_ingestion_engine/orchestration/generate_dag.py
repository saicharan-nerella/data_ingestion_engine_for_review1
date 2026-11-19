"""
orchestration/generate_dag.py
------------------------------
CLI: generate an Airflow DAG from your reader/architecture/writer/alert
choices, and deploy it — to AWS MWAA (managed) or a self-hosted,
open-source Airflow. Both run the exact same generated DAG file; this
script's --deploy-target flag only changes where the file ends up.

USAGE — generate only, don't deploy:
    python -m orchestration.generate_dag \\
        --dag-id batch_cloud_lambda_cloud \\
        --glue-job-name aimatic-structured-test \\
        --config_path s3://goml-aimatic-de-config/config.yaml \\
        --reader batch_cloud --architecture lambda --writer cloud \\
        --writer_bucket my-output-bucket --writer_prefix structured/ \\
        --output dags/batch_cloud_lambda_cloud.py

USAGE — generate and deploy to MWAA:
    python -m orchestration.generate_dag \\
        --dag-id batch_cloud_lambda_cloud \\
        --glue-job-name aimatic-structured-test \\
        --config_path s3://goml-aimatic-de-config/config.yaml \\
        --reader batch_cloud --architecture lambda --writer cloud \\
        --writer_bucket my-output-bucket \\
        --deploy-target mwaa --mwaa-dags-bucket my-mwaa-env-bucket

USAGE — generate and deploy to a self-hosted Airflow:
    python -m orchestration.generate_dag \\
        --dag-id batch_cloud_lambda_cloud \\
        --glue-job-name aimatic-structured-test \\
        --config_path s3://goml-aimatic-de-config/config.yaml \\
        --reader batch_cloud --architecture lambda --writer cloud \\
        --writer_bucket my-output-bucket \\
        --deploy-target opensource --dags-folder /opt/airflow/dags

HOW GLUE ARGUMENTS ARE BUILT:
Any flag this script doesn't recognize as one of its own DAG-level options
(--dag-id, --glue-job-name, --schedule, --alert-channel, --alert-config,
--deploy-target, --mwaa-dags-bucket, --dags-folder, --output, ...) is
passed straight through as a --key value Glue argument — the identical
style you'd use with `aws glue start-job-run`. This means every current
and future --reader/--architecture/--writer_<x>/--kappa_<x>/--logger/
--ecs_endpoint flag works here with zero changes to this script.

DEPLOYMENT DIFFERENCES (both run the same DAG file):
  MWAA (managed)       : uploaded to s3://<mwaa-dags-bucket>/dags/ —
                         MWAA's own execution IAM role provides AWS access
  Open-source (self-hosted) : copied into a local dags/ folder — that
                         Airflow instance needs its own IAM role (EC2/EKS)
                         or an AWS Connection configured for Glue access
"""

import argparse
import logging
import sys

from orchestration.dag_generator import generate_dag_file

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Generate (and optionally deploy) an Airflow DAG that triggers the aimatic Glue job.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dag-id", required=True, help="Unique DAG identifier")
    parser.add_argument("--glue-job-name", required=True, help="Name of the Glue job to trigger")
    parser.add_argument("--schedule", default="0 22 * * *", help="Cron schedule (default: daily 10 PM UTC)")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-delay-minutes", type=int, default=5)
    parser.add_argument("--timeout-minutes", type=int, default=60)
    parser.add_argument("--region", default="us-east-1", help="AWS region for the Glue API calls")
    parser.add_argument("--description", default=None)

    parser.add_argument("--alert-channel", default=None,
                        help="sns | ses | sqs | slack | teams | email | pagerduty | webhook | "
                             "any other name (generic webhook fallback, needs a url in --alert-config)")
    parser.add_argument("--alert-config", default=None,
                        help="JSON string of that channel's config, e.g. "
                             '\'{"webhook_url": "https://hooks.slack.com/..."}\'')
    parser.add_argument("--no-email-on-failure", action="store_true",
                        help="Disable Airflow's built-in email alert")
    parser.add_argument("--alert-email", action="append", default=None,
                        help="Email address for Airflow's built-in alert (repeatable)")

    parser.add_argument("--output", default=None,
                        help="Local file path to write the DAG to (default: <dag-id>.py)")
    parser.add_argument("--deploy-target", choices=["mwaa", "opensource"], default=None,
                        help="Where to deploy after generating — omit to only write the local file")
    parser.add_argument("--mwaa-dags-bucket", default=None,
                        help="Required with --deploy-target mwaa — the MWAA environment's S3 bucket")
    parser.add_argument("--dags-folder", default=None,
                        help="Required with --deploy-target opensource — the local/mounted Airflow dags/ folder")

    known_args, remainder = parser.parse_known_args(argv)

    # Everything else becomes a --key value Glue argument, exactly the
    # style `aws glue start-job-run` itself uses.
    glue_arguments = {}
    i = 0
    while i < len(remainder):
        token = remainder[i]
        if not token.startswith("--"):
            parser.error(f"unexpected argument: {token}")
        if i + 1 < len(remainder) and not remainder[i + 1].startswith("--"):
            glue_arguments[token] = remainder[i + 1]
            i += 2
        else:
            glue_arguments[token] = "true"  # a bare flag
            i += 1

    return known_args, glue_arguments


def main(argv=None):
    args, glue_arguments = _parse_args(argv if argv is not None else sys.argv[1:])

    if "--reader" not in glue_arguments or "--architecture" not in glue_arguments or "--writer" not in glue_arguments:
        log.error("At minimum, --reader, --architecture, and --writer must be passed through "
                  "to the Glue job (got: %s)", list(glue_arguments))
        return 1

    alert_config = None
    if args.alert_config:
        import json
        alert_config = json.loads(args.alert_config)

    output_path = args.output or f"{args.dag_id}.py"

    generate_dag_file(
        output_path=output_path,
        dag_id=args.dag_id,
        glue_job_name=args.glue_job_name,
        glue_arguments=glue_arguments,
        schedule=args.schedule,
        retries=args.retries,
        retry_delay_minutes=args.retry_delay_minutes,
        timeout_minutes=args.timeout_minutes,
        region=args.region,
        alert_channel=args.alert_channel,
        alert_config=alert_config,
        email_on_failure=not args.no_email_on_failure,
        alert_emails=args.alert_email,
        description=args.description,
    )
    log.info("DAG written to %s", output_path)

    if args.deploy_target == "mwaa":
        if not args.mwaa_dags_bucket:
            log.error("--deploy-target mwaa requires --mwaa-dags-bucket")
            return 1
        _deploy_to_mwaa(output_path, args.mwaa_dags_bucket, args.dag_id)

    elif args.deploy_target == "opensource":
        if not args.dags_folder:
            log.error("--deploy-target opensource requires --dags-folder")
            return 1
        _deploy_to_opensource(output_path, args.dags_folder, args.dag_id)

    return 0


def _deploy_to_mwaa(local_path: str, dags_bucket: str, dag_id: str) -> None:
    import boto3
    s3_key = f"dags/{dag_id}.py"
    boto3.client("s3").upload_file(local_path, dags_bucket, s3_key)
    log.info("Uploaded to MWAA: s3://%s/%s", dags_bucket, s3_key)
    log.info("MWAA picks up new/changed DAG files automatically within a few minutes "
             "(or trigger a manual sync from the MWAA console).")


def _deploy_to_opensource(local_path: str, dags_folder: str, dag_id: str) -> None:
    import os
    import shutil
    os.makedirs(dags_folder, exist_ok=True)
    dest = os.path.join(dags_folder, f"{dag_id}.py")
    shutil.copyfile(local_path, dest)
    log.info("Copied to self-hosted Airflow dags folder: %s", dest)
    log.info("Airflow's scheduler picks up new/changed DAG files within its own "
             "dag_dir_list_interval (default 5 minutes).")


if __name__ == "__main__":
    sys.exit(main())
