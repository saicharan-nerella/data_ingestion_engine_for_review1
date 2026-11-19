"""
core/pipeline_builder.py
─────────────────────────
Reads config.yaml for DATA CONFIG (credentials, buckets, tables, etc.)
Reads Glue job parameters for COMPONENT SELECTION (which reader, writer, etc.)

SEPARATION OF CONCERNS:
  Glue job parameters → WHAT to run  (reader, writer, architecture, alerts, etc.)
  config.yaml         → HOW to run it (host, bucket, credentials, table names, etc.)

GLUE JOB PARAMETERS (passed via --key value):
  --reader        batch_database | batch_api | batch_cloud | streaming_kafka | ...
  --writer        cloud | data_warehouse | database | data_lake
  --architecture  medallion | lambda | kappa | data_vault
  --orchestrator  airflow | prefect | dagster | cron  (optional)
  --logger        json | text | cloudwatch              (optional)
  --logger_<x>    level, log_group, log_stream, region, format, ...
                  (backend-specific — see logger/*.py; falls back to
                  config.yaml's `logger_config` section when not passed)

  Alerting is NOT a Glue parameter — it's entirely config.yaml-driven (the
  top-level `alerting:` section); see alerts/channel_senders.py. A previous
  --alerts parameter here tried to load classes that were never written, so
  it silently did nothing no matter what was passed — removed rather than
  left looking like it works (passing it now just logs a warning).

EXAMPLE GLUE JOB PARAMETERS:
  --config_path    s3://aimatic-config/config.yaml
  --pipeline_mode  structured
  --pkg_s3_uri     s3://aimatic-config/data_ingestion_engine.zip
  --reader         batch_api
  --writer         cloud
  --architecture   medallion
  --orchestrator   airflow
  --logger         json
"""

import importlib
import yaml
import logging
from core.registry import REGISTRY

log = logging.getLogger(__name__)


def _resolve(layer: str, name: str):
    """
    Resolve a component name to its class via the registry.
    Only called when the user has explicitly chosen this component.
    """
    if name not in REGISTRY[layer]:
        available = list(REGISTRY[layer].keys())
        raise KeyError(
            f"Unknown {layer} '{name}'. Available: {available}\n"
            f"Register it in core/registry.py to make it available."
        )
    class_path = REGISTRY[layer][name]
    module_path, class_name = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class PipelineBuilder:
    """
    Assembles a fully wired pipeline.

    Component selection comes from Glue job parameters (args dict).
    Data configuration comes from config.yaml (config_path).

    Only components chosen via parameters are loaded — everything else is skipped.
    """

    def __init__(self, config_path: str, args: dict = None):
        self.config_path = config_path
        self.args = args or {}
        self.config = self._load_config()

    def _load_config(self) -> dict:
        config_path = str(self.config_path)

        # Handle S3 paths (s3://bucket/key)
        if config_path.startswith("s3://"):
            import boto3
            s3 = boto3.client("s3")
            bucket, key = config_path.replace("s3://", "").split("/", 1)
            obj = s3.get_object(Bucket=bucket, Key=key)
            cfg = yaml.safe_load(obj["Body"].read())
            log.info("PipelineBuilder: loaded config from S3: %s", config_path)
            return cfg

        with open(config_path, encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
        log.info("PipelineBuilder: loaded config from %s", config_path)
        return cfg

    def build(self):
        """
        Build and return the fully assembled pipeline.

        Component selection is read from Glue job parameters (self.args).
        Data config is read from config.yaml (self.config).
        """
        cfg  = self.config
        args = self.args

        log.info("PipelineBuilder: building pipeline from parameters: %s",
                 {k: v for k, v in args.items() if "password" not in k.lower()})

        # ── ARCHITECTURE (from --architecture parameter) ──────────────────────
        arch_name = args.get("architecture", "medallion")
        if not arch_name:
            raise ValueError(
                "--architecture parameter is required. "
                "Options: medallion | lambda | kappa | data_vault"
            )
        ArchClass = _resolve("architecture", arch_name)
        log.info("PipelineBuilder: architecture = %s", arch_name)

        # ── LAMBDA ARCHITECTURE TUNING (from --speed_layer_window_seconds) ────
        # A Glue job parameter (WHAT/HOW this run behaves), not config.yaml
        # data — same separation used for writer settings below. Threaded
        # into cfg here so architecture/lambda_architecture/speed_layer.py's
        # existing config.get("lambda", {}).get("speed_layer_window_seconds",
        # 30) read doesn't need to change; only where the value comes from does.
        if arch_name == "lambda":
            window_arg = args.get("speed_layer_window_seconds")
            if window_arg is not None:
                try:
                    window_seconds = int(window_arg)
                except (TypeError, ValueError):
                    raise ValueError(
                        f"--speed_layer_window_seconds must be an integer, got: {window_arg!r}"
                    )
            else:
                window_seconds = 30
            cfg.setdefault("lambda", {})["speed_layer_window_seconds"] = window_seconds
            log.info("PipelineBuilder: lambda speed_layer_window_seconds = %d", window_seconds)

        # ── KAPPA ARCHITECTURE TUNING (from --kappa_window_seconds, --kappa_replay) ──
        # Same reasoning as Lambda's speed_layer_window_seconds above — a Glue
        # job runs to completion, but Kappa's streaming drain is conceptually
        # continuous, so it needs the same bounded-window compromise.
        if arch_name == "kappa":
            window_arg = args.get("kappa_window_seconds")
            if window_arg is not None:
                try:
                    kappa_window_seconds = int(window_arg)
                except (TypeError, ValueError):
                    raise ValueError(
                        f"--kappa_window_seconds must be an integer, got: {window_arg!r}"
                    )
            else:
                kappa_window_seconds = 30
            cfg.setdefault("kappa", {})["window_seconds"] = kappa_window_seconds
            cfg["kappa"]["replay"] = args.get("kappa_replay", "false").lower() == "true"
            log.info("PipelineBuilder: kappa window_seconds = %d, replay = %s",
                     kappa_window_seconds, cfg["kappa"]["replay"])

        # ── READERS (from --reader parameter, comma-separated) ────────────────
        # e.g. --reader batch_api
        # e.g. --reader batch_database,batch_api  (multiple readers)
        reader_param = args.get("reader", "")
        if not reader_param:
            raise ValueError(
                "--reader parameter is required. "
                "Options: batch_api | batch_cloud | batch_database | "
                "streaming_kafka | streaming_cloud | streaming_socket | streaming_webhook"
            )
        reader_names = [r.strip() for r in reader_param.split(",") if r.strip()]

        # Medallion and Data Vault both have no bounded-drain concept — each
        # reader loop calls reader.read() to exhaustion, but a streaming
        # reader's read() runs `while True:` forever by design (the bounded
        # draining that makes that safe is Lambda/Kappa-specific — see
        # architecture/lambda_architecture/speed_layer.py and Kappa's own
        # bounded drain). Selecting a streaming reader with either would hang
        # the job forever instead of failing — caught here, before anything
        # connects, rather than left to time out with no explanation.
        _NO_STREAMING_ARCHITECTURES = {"medallion", "data_vault"}
        if arch_name in _NO_STREAMING_ARCHITECTURES:
            streaming_picked = [n for n in reader_names if n.startswith("streaming_")]
            if streaming_picked:
                raise ValueError(
                    f"--architecture {arch_name} does not support streaming reader(s) "
                    f"{streaming_picked} — {arch_name} has no bounded-drain mechanism, so "
                    f"a streaming reader's read() would loop forever rather than "
                    f"complete. Use --architecture lambda or kappa instead, both of "
                    f"which drain streaming readers for a bounded time window."
                )

        readers = []
        for reader_name in reader_names:
            ReaderClass = _resolve("reader", reader_name)
            readers.append(ReaderClass(cfg))
            log.info("PipelineBuilder: reader loaded — %s", reader_name)

        # ── WRITERS (from --writer parameter, comma-separated) ────────────────
        # e.g. --writer cloud
        # Writer settings come from Glue parameters, NOT config.yaml:
        #   --writer_provider  aws | gcp | azure
        #   --writer_bucket    target-bucket-name       (object-storage writers only: cloud/data_lake/s3_*/gcs_*/azure_*)
        #   --writer_prefix    output/prefix/
        #   --writer_region    us-east-1
        #   --writer_format    json | csv | parquet | xml | html | md | txt
        #   --writer_mode      append | overwrite        (cloud/data_lake — see target/write_cloud.py)
        #   --writer_partition_cols  col1,col2           (cloud/data_lake — Hive-style sub-paths, e.g. col1=val1/col2=val2/)
        # Data warehouse / database writers take their OWN parameters instead
        # (--writer_warehouse_type, --writer_host, --writer_database,
        # --writer_schema, --writer_table, --writer_staging_bucket,
        # --writer_cluster_identifier, --writer_db_user, --writer_merge_keys, ...)
        # — passed through generically below rather than hardcoded here, since
        # each writer class defines and validates what it actually needs.
        writer_param = args.get("writer", "")
        if not writer_param:
            raise ValueError(
                "--writer parameter is required. "
                "Options: cloud | data_warehouse | database | data_lake"
            )
        writer_names = [w.strip() for w in writer_param.split(",") if w.strip()]

        # Only object-storage-shaped writers require --writer_bucket. A
        # data_warehouse/database writer validates its own required fields
        # (host, database, table, ...) in its own __init__ instead — it
        # shouldn't be blocked by a bucket check that doesn't apply to it.
        _OBJECT_STORAGE_WRITERS = {
            "cloud", "data_lake",
            "s3_json", "s3_csv", "s3_parquet", "s3_xml", "s3_html", "s3_md", "s3_txt",
            "gcs_json", "gcs_csv", "gcs_parquet",
            "azure_json", "azure_csv", "azure_parquet",
        }
        needs_bucket = any(name in _OBJECT_STORAGE_WRITERS for name in writer_names)

        # Build writer config entirely from Glue parameters
        writer_config = {
            "provider":    args.get("writer_provider", args.get("write_provider", "aws")),
            "bucket":      args.get("writer_bucket", args.get("write_bucket", "")),
            "prefix":      args.get("writer_prefix", args.get("write_prefix", "")),
            "region":      args.get("writer_region", args.get("write_region", "us-east-1")),
            "file_format": args.get("writer_format", args.get("write_format", "json")),
            "batch_mode":  args.get("writer_batch_mode", args.get("write_batch_mode", "false")).lower() == "true",
            "mode":        args.get("writer_mode", "append"),
            "partition_cols": [c.strip() for c in args.get("writer_partition_cols", "").split(",") if c.strip()],
            "auth": {
                "type": args.get("writer_auth_type", args.get("write_auth_type", "iam_role")),
            },
        }

        if needs_bucket and not writer_config["bucket"]:
            raise ValueError(
                "--writer_bucket parameter is required for cloud/data_lake writers. "
                "Example: --writer_bucket my-output-bucket"
            )

        # Any OTHER --writer_<x> parameter not already captured above (e.g.
        # warehouse_type, host, port, database, schema, table, staging_bucket,
        # staging_prefix, cluster_identifier, workgroup_name, db_user,
        # secret_arn, merge_keys, password, ...) is passed through as-is under
        # its own key, so each writer class can read whatever config it
        # specifically needs without this file hardcoding every writer's
        # parameter names.
        _HANDLED_WRITER_ARGS = {
            "writer_provider", "writer_bucket", "writer_prefix", "writer_region",
            "writer_format", "writer_batch_mode", "writer_mode", "writer_partition_cols",
            "writer_auth_type",
        }
        for key, value in args.items():
            if key.startswith("writer_") and key not in _HANDLED_WRITER_ARGS:
                writer_config[key[len("writer_"):]] = value

        log.info("PipelineBuilder: writer config from parameters — %s",
                 {k: v for k, v in writer_config.items()
                  if k != "auth" and "password" not in k.lower() and "secret" not in k.lower()})

        writers = []
        for writer_name in writer_names:
            WriterClass = _resolve("writer", writer_name)
            writers.append(WriterClass(writer_config))
            log.info("PipelineBuilder: writer loaded — %s", writer_name)

        # ── LOGGER (from --logger parameter, comma-separated) — OPTIONAL ──────
        # Same pattern as writers: config comes from --logger_<x> Glue
        # parameters first (so a UI can set it per-job), falling back to
        # config.yaml's `logger_config` section for anything not passed —
        # not the other way around, so Glue params always win when both exist.
        #
        # More than one backend can be combined — e.g. --logger json,cloudwatch
        # gives structured local output AND a copy routed to a stable,
        # custom CloudWatch log group. At most one of the selected backends
        # may be non-additive (json/text — each replaces the root logger's
        # handlers; combining two of those is a config error, since the
        # second would just erase the first's effect). Non-additive backends
        # are always configured before additive ones regardless of the
        # order they were listed, so an additive handler never gets wiped
        # out by a later replace.
        logger_param = args.get("logger", "")
        if logger_param:
            logger_names = [n.strip() for n in logger_param.split(",") if n.strip()]
            logger_config = dict(cfg.get("logger_config", {}))
            for key, value in args.items():
                if key.startswith("logger_") and key != "logger":
                    logger_config[key[len("logger_"):]] = value

            try:
                loggers = [(name, _resolve("logger", name)) for name in logger_names]
                non_additive = [name for name, cls in loggers if not cls.ADDITIVE]
                if len(non_additive) > 1:
                    raise ValueError(
                        f"--logger named more than one backend that replaces the root "
                        f"handlers ({non_additive}) — combine at most one of json/text "
                        f"with any number of additive backends (cloudwatch)."
                    )

                loggers.sort(key=lambda pair: pair[1].ADDITIVE)  # non-additive first
                for name, LoggerClass in loggers:
                    LoggerClass(logger_config).configure()
                    log.info("PipelineBuilder: logger = %s", name)
            except Exception as e:
                log.warning("PipelineBuilder: logger '%s' failed (%s) — using default",
                           logger_param, e)
        else:
            log.info("PipelineBuilder: no --logger passed — using default stdout logging")

        # ── ALERTS — NOT a Glue parameter ──────────────────────────────────────
        # Real alerting is entirely config.yaml-driven: the top-level
        # `alerting:` section (channel/channels + per-channel settings),
        # delivered via alerts/channel_senders.py — see that file's
        # CHANNEL_BUILDERS for the full list (sns, ses, sqs, slack, teams,
        # email, pagerduty). alert_handlers is kept here only because
        # MedallionArchitecture/LambdaArchitecture's __init__ still accept
        # it; it's always empty now. A previous --alerts parameter tried to
        # load classes (alerts.slack_alert.SlackAlert, etc.) that were never
        # actually written, so it silently did nothing no matter what was
        # passed — removed rather than left as a parameter that looks like
        # it works.
        alert_handlers = []
        if args.get("alerts"):
            log.warning(
                "PipelineBuilder: --alerts was passed but is not implemented — "
                "it has no effect. Configure alerting entirely under "
                "config.yaml's `alerting:` section instead (channel/channels "
                "+ that channel's settings); see alerts/channel_senders.py."
            )

        # ── ORCHESTRATOR — NOT a Glue parameter ──────────────────────────────
        # Scheduling can't work as a Glue-job-parameter plugin the way every
        # other section does — an orchestrator has to exist and act BEFORE
        # this job starts (it's the thing that calls start_job_run), not
        # something the job loads on itself once it's already running. A
        # previous --orchestrator parameter instantiated a class here and
        # passed it to the Pipeline, but no Pipeline.execute() anywhere ever
        # called it — dead code, the same "looks wired, does nothing"
        # pattern as the old --alerts parameter. Removed rather than left
        # looking like it works. Real orchestration: generate an Airflow DAG
        # with orchestration/generate_dag.py that triggers this same Glue
        # job with whichever --reader/--architecture/--writer_* parameters
        # you choose, deployed to MWAA or a self-hosted Airflow — see that
        # module.
        orchestrator = None
        if args.get("orchestrator"):
            log.warning(
                "PipelineBuilder: --orchestrator was passed but is not implemented "
                "as a Glue parameter — it has no effect. Orchestration happens "
                "outside this job: generate an Airflow DAG with "
                "orchestration/generate_dag.py instead, which triggers this job "
                "with your chosen --reader/--architecture/--writer_* parameters."
            )

        # ── ASSEMBLE PIPELINE ─────────────────────────────────────────────────
        pipeline = ArchClass(
            config=cfg,
            readers=readers,
            writers=writers,
            orchestrator=orchestrator,
            alert_handlers=alert_handlers,
        )

        log.info(
            "PipelineBuilder: pipeline assembled — "
            "architecture=%s | readers=%s | writers=%s | alerts=%s",
            arch_name,
            reader_names,
            writer_names,
            [a.__class__.__name__ for a in alert_handlers],
        )
        return pipeline
