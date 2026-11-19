"""
core/registry.py
─────────────────
Central component registry.
Maps string identifiers (from config.yaml) to importable class paths.

HOW PLUGGABILITY WORKS:
    1. User picks components in the UI → UI writes config.yaml
    2. PipelineBuilder reads the config
    3. Registry resolves string names → Python class paths
    4. PipelineBuilder imports and instantiates each class dynamically

TO ADD A NEW COMPONENT:
    1. Create the file in the correct layer folder
    2. Add its entry here in REGISTRY
    3. It immediately appears in the UI selector
"""

REGISTRY = {

    # ── Architecture patterns ─────────────────────────────────────────────────
    # Controls the overall data flow topology
    "architecture": {
        "lambda":     "architecture.lambda_architecture.pipeline.Pipeline",
        "kappa":      "architecture.kappa_architecture.pipeline.Pipeline",
        "medallion":  "architecture.medallion_architecture.pipeline.Pipeline",
        "data_vault": "architecture.data_vault_architecture.pipeline.Pipeline",
    },

    # ── Ingestion readers ─────────────────────────────────────────────────────
    # Source connectors — multiple can be active simultaneously
    #
    # The unstructured pipeline (pipelines/unstructured_pipeline.py) does NOT
    # go through this registry — it hardcodes GenericUnstructuredReader +
    # ECSHttpParser directly, since format detection/parsing there is
    # delegated entirely to docling_service over HTTP. A prior
    # "unstructured_docling" reader entry and a whole "unstructured_reader_plugin"
    # category (per-format PDF/image/audio/video/... readers) pointed at
    # modules that were never built and aren't on this pipeline's actual
    # execution path — removed rather than left as a crash waiting to happen
    # if someone picked one of those names.
    "reader": {
        "batch_api":          "structured_readers.batch_api_read.BatchAPIReader",
        "batch_cloud":        "structured_readers.batch_s3_read.BatchCloudReader",
        "batch_database":     "structured_readers.batch_database_read.BatchDatabaseReader",
        "streaming_kafka":    "structured_readers.streaming_kafka_read.StreamingKafkaReader",
        "streaming_cloud":    "structured_readers.streaming_cloud_read.StreamingCloudReader",
        "streaming_socket":   "structured_readers.streaming_socket_read.StreamingSocketReader",
        "streaming_webhook":  "structured_readers.streaming_webhook_read.StreamingWebhookReader",
    },

    # ── Writers / targets ─────────────────────────────────────────────────────
    # Sink connectors — multiple can be active simultaneously
    "writer": {
        "cloud":           "target.write_cloud.CloudWriter",
        "data_warehouse":  "target.write_data_warehouse.DataWarehouseWriter",
        "database":        "target.write_database.DatabaseWriter",
        # "data_lake" is CloudWriter too, not a separate class — they were
        # ~90% duplicated code (same formats, same 3 providers) with only
        # two real differences, and write_data_lake.py's versions of those
        # two (mode, partition_cols) were accepted but never implemented.
        # CloudWriter now implements both for real (see target/write_cloud.py)
        # — pick "data_lake" when you want --writer_partition_cols and/or
        # --writer_mode=overwrite; "cloud" when you don't need either.
        "data_lake":       "target.write_cloud.CloudWriter",
        # "streaming_sink" (writing back OUT to a Kafka topic / Pub/Sub /
        # Kinesis as a sink) is not implemented — target/write_streaming_sink.py
        # doesn't exist, so selecting it crashed with ModuleNotFoundError
        # rather than doing nothing visibly, which is worse than just not
        # listing it. Removed rather than left looking like a real option —
        # same call made earlier for the old --alerts classes. Streaming
        # readers' output already writes correctly through cloud/
        # data_warehouse/database today (LambdaArchitecture's Speed layer
        # writes it the same way as every other layer); a real producer-style
        # sink would be new work, not a registration fix, if actually wanted.
        # Generic CloudWriter supports all formats: json, csv, parquet, xml, html, md, txt
        "s3_json":         "target.write_cloud.CloudWriter",
        "s3_csv":          "target.write_cloud.CloudWriter",
        "s3_parquet":      "target.write_cloud.CloudWriter",
        "s3_xml":          "target.write_cloud.CloudWriter",
        "s3_html":         "target.write_cloud.CloudWriter",
        "s3_md":           "target.write_cloud.CloudWriter",
        "s3_txt":          "target.write_cloud.CloudWriter",
        "gcs_json":        "target.write_cloud.CloudWriter",
        "gcs_csv":         "target.write_cloud.CloudWriter",
        "gcs_parquet":     "target.write_cloud.CloudWriter",
        "azure_json":      "target.write_cloud.CloudWriter",
        "azure_csv":       "target.write_cloud.CloudWriter",
        "azure_parquet":   "target.write_cloud.CloudWriter",
    },

    # NOTE: there is no "orchestrator" registry section here on purpose.
    # Scheduling/orchestration (Airflow, managed or open-source) can't work
    # as a Glue-job-parameter plugin the way every section above does —
    # an orchestrator has to exist and act BEFORE the Glue job starts (it's
    # the thing that calls start_job_run), not something the job loads on
    # itself once it's already running. A previous "orchestrator" section
    # here pointed at orchestration.airflow_orchestrator.AirflowOrchestrator
    # and similar classes that were never written, instantiated inside
    # core/pipeline_builder.py and passed to every Pipeline's __init__ —
    # but no Pipeline.execute() anywhere ever called it. Dead code, the
    # same "looks wired, does nothing" pattern as the old --alerts
    # parameter. Removed rather than left looking like it works.
    #
    # Real orchestration: orchestration/dag_generator.py generates a
    # standalone Airflow DAG file that triggers this Glue job with whichever
    # --reader/--architecture/--writer_* parameters you choose, then
    # monitors it — deployed to MWAA's S3 dags/ bucket (managed) or copied
    # into a self-hosted Airflow's dags/ folder (open-source), both running
    # the exact same DAG file. See orchestration/generate_dag.py for the CLI.

    # NOTE: there is no "alert" registry section here on purpose. Real
    # alerting is entirely config.yaml-driven — see the top-level
    # `alerting:` section and alerts/channel_senders.py's CHANNEL_BUILDERS
    # (sns, ses, sqs, slack, teams, email, pagerduty). A previous "alert"
    # registry here pointed at alerts.slack_alert.SlackAlert and similar
    # classes that were never actually written — --alerts silently did
    # nothing. Don't recreate that: add a new channel in
    # alerts/channel_senders.py instead, not here.

    # ── Loggers ───────────────────────────────────────────────────────────────
    # Structured logging backend
    "logger": {
        "json":        "logger.json_logger.JSONLogger",
        "text":        "logger.text_logger.TextLogger",
        "cloudwatch":  "logger.cloudwatch_logger.CloudWatchLogger",
    },
}


# ── Metadata for the UI ───────────────────────────────────────────────────────
# Displayed in the pipeline builder UI cards
COMPONENT_META = {
    "architecture": {
        "lambda":     {"label": "Lambda",      "icon": "λ",  "desc": "Batch + speed dual path", "tag": "mixed latency"},
        "kappa":      {"label": "Kappa",       "icon": "κ",  "desc": "One path for batch and streaming; bounded replay window", "tag": "low latency"},
        "medallion":  {"label": "Medallion",   "icon": "⬡",  "desc": "Bronze → Silver → Gold layers", "tag": "lakehouse"},
        "data_vault": {"label": "Data Vault",  "icon": "⬡",  "desc": "Hub-Link-Satellite model", "tag": "audit trail"},
    },
    "reader": {
        "batch_api":         {"label": "REST API",        "desc": "HTTP polling, paginated", "tag": "batch"},
        "batch_cloud":       {"label": "Cloud Storage",   "desc": "GCS / S3 / Azure Blob",  "tag": "batch"},
        "batch_database":    {"label": "Database",        "desc": "JDBC / SQLAlchemy",       "tag": "batch"},
        "streaming_kafka":   {"label": "Kafka",           "desc": "Consumer group, offsets", "tag": "stream"},
        "streaming_cloud":   {"label": "Pub/Sub",         "desc": "GCP / Kinesis / EventHub","tag": "stream"},
        "streaming_socket":  {"label": "TCP Socket",      "desc": "Raw TCP/UDP structured_readers",   "tag": "stream"},
        "streaming_webhook": {"label": "Webhook",         "desc": "HTTP POST receiver",      "tag": "stream"},
    },
    "writer": {
        "cloud":          {"label": "Cloud Storage",   "desc": "GCS / S3 / Azure Blob"},
        "data_warehouse": {"label": "Data Warehouse",  "desc": "Redshift (Snowflake / BigQuery / Synapse not yet implemented)"},
        "database":       {"label": "Database",        "desc": "Postgres / MySQL / Oracle / SQL Server"},
        "data_lake":      {"label": "Data Lake",       "desc": "Same as Cloud Storage, with partitioned writes and overwrite mode"},
        # CloudWriter supports all formats and providers
        "s3_json":        {"label": "S3 JSON",         "desc": "AWS S3 + JSON format"},
        "s3_csv":         {"label": "S3 CSV",          "desc": "AWS S3 + CSV format"},
        "s3_parquet":     {"label": "S3 Parquet",      "desc": "AWS S3 + Parquet format"},
        "s3_xml":         {"label": "S3 XML",          "desc": "AWS S3 + XML format"},
        "s3_html":        {"label": "S3 HTML",         "desc": "AWS S3 + HTML format"},
        "s3_md":          {"label": "S3 Markdown",     "desc": "AWS S3 + Markdown format"},
        "s3_txt":         {"label": "S3 Text",         "desc": "AWS S3 + Text format"},
        "gcs_json":       {"label": "GCS JSON",        "desc": "GCP Cloud Storage + JSON format"},
        "gcs_csv":        {"label": "GCS CSV",         "desc": "GCP Cloud Storage + CSV format"},
        "gcs_parquet":    {"label": "GCS Parquet",     "desc": "GCP Cloud Storage + Parquet format"},
        "azure_json":     {"label": "Azure JSON",      "desc": "Azure Blob Storage + JSON format"},
        "azure_csv":      {"label": "Azure CSV",       "desc": "Azure Blob Storage + CSV format"},
        "azure_parquet":  {"label": "Azure Parquet",   "desc": "Azure Blob Storage + Parquet format"},
    },
    "logger": {
        "json":        {"label": "JSON",              "desc": "Structured one-JSON-object-per-line logging"},
        "text":        {"label": "Text",               "desc": "Human-readable formatted console logging"},
        "cloudwatch":  {"label": "CloudWatch",        "desc": "Adds a handler routing to a stable, custom CloudWatch Logs group"},
    },
}
