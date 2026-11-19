"""
logger/cloudwatch_logger.py
─────────────────────────────
Sends log records to a specific, stable CloudWatch Logs group/stream via
watchtower — ADDS a handler rather than replacing the root logger's
existing one, since Glue already ships stdout/stderr to its own per-run
log group automatically (/aws-glue/python-jobs/output and .../error) —
this is for the separate case of wanting ALL runs aggregated into one
well-known, consistently-named log group (for a saved CloudWatch Logs
Insights query or a metric filter/alarm that shouldn't have to chase a new
log stream name every run), not a replacement for that automatic shipping.

Requires the `watchtower` package (see requirements.txt — commented out by
default, since most jobs don't need a second log destination; add it to
the Glue job's --additional-python-modules if you use --logger cloudwatch).

Auth: uses the job's own IAM role via the default boto3 credential chain
(the same role already used for S3/Redshift/etc. in this codebase) — NOT a
named AWS profile. aws_profile is accepted only as an explicit opt-in for
local/dev testing outside Glue, where no IAM role is attached to the process.

log_stream defaults to a fresh name generated per run (a timestamp + a
short random suffix), not a fixed string — CloudWatch log streams have a
sequencing constraint, so two runs (overlapping, or even just back-to-back
after a retry) writing to the SAME fixed stream risks a real
InvalidSequenceTokenException, not just "can't tell the runs apart" in the
log output. Pass an explicit log_stream only if you specifically want
every run to share one stream and have reasoned about that risk yourself.

Config: level, log_group, log_stream (optional — unique per run if
omitted), region, aws_profile (local/dev only)
"""
import logging
import time
import uuid

from logger.base_logger import BaseLogger

log = logging.getLogger(__name__)


class CloudWatchLogger(BaseLogger):
    ADDITIVE = True  # adds a handler; doesn't touch whatever's already configured

    def configure(self):
        log_group = self.config.get("log_group", "/aimatic/pipeline")
        log_stream = self.config.get("log_stream") or (
            f"pipeline-{time.strftime('%Y-%m-%dT%H-%M-%S')}-{uuid.uuid4().hex[:8]}"
        )
        region = self.config.get("region", "us-east-1")
        aws_profile = self.config.get("aws_profile")

        try:
            import boto3
            import watchtower
        except ImportError as exc:
            raise ImportError(
                "CloudWatchLogger: the 'watchtower' package is required for "
                "--logger cloudwatch. Add it to the Glue job's "
                "--additional-python-modules (see requirements.txt)."
            ) from exc

        session = boto3.Session(region_name=region, profile_name=aws_profile)
        handler = watchtower.CloudWatchLogHandler(
            log_group_name=log_group,
            log_stream_name=log_stream,
            boto3_session=session,
        )

        root = logging.getLogger()
        root.addHandler(handler)
        root.setLevel(self.level_int)

        log.info("CloudWatchLogger: added handler routing to %s/%s in %s",
                 log_group, log_stream, region)
