"""aimatic.py
AWS Glue entry point for data processing pipelines (structured and unstructured).

This script is the generic entry point that:
- Detects pipeline mode (structured or unstructured)
- Delegates to the appropriate pipeline module
- Handles configuration loading and parameter parsing

GLUE JOB PARAMETERS:

COMMON (REQUIRED FOR ALL MODES):
  --config_path         s3://bucket/config.yaml

OPTIONAL (AUTO-DETECTION):
  --pipeline_mode       unstructured | structured     (explicit mode - overrides auto-detection)

FOR UNSTRUCTURED PIPELINE (REQUIRED):
  --ecs_endpoint        http://<ALB_DNS>              (triggers unstructured mode)
  --writer_bucket       output-bucket-name
  --writer_prefix       unstructured/                 (optional, default: unstructured/)
  --writer_region       us-east-1                     (optional, default: us-east-1)
  --writer_format       json                          (optional, default: json)

FOR STRUCTURED PIPELINE (REQUIRED):
  --reader              batch_api | batch_database | batch_cloud | streaming_kafka | ...
                         (comma-separated for multiple, e.g. "batch_database,batch_cloud")
                         All three batch readers read their source config from config.yaml
                         under structured.batch.<databases|cloud|apis>[] consistently.
  --architecture        medallion | lambda | kappa | data_vault
                         Resolved via core/registry.py -> executed by
                         core/pipeline_builder.py's PipelineBuilder (NOT pipelines/structured_pipeline.py,
                         which is legacy PySpark code, unused, and currently broken).
  --speed_layer_window_seconds   integer seconds  (optional, default: 30 — lambda architecture only.
                         How long the Speed layer drains streaming readers before merging whatever
                         it collected. Not read from config.yaml — core/pipeline_builder.py threads
                         this Glue parameter into the architecture directly, same as writer settings.)
  --writer              cloud | data_warehouse | database | data_lake
  --writer_bucket       output-bucket-name             (required — this is the S3 target)
  --writer_prefix       structured/                    (optional, default: structured/)
                         Medallion layers each append their own sub-prefix under this
                         (bronze/, silver/, gold/) — set automatically, do not include in this param.
  --writer_format       parquet | json | csv | xml | html | md | txt   (optional, default: parquet)
                         parquet requires pyarrow to be installed in the Glue Python env.
  --writer_region       us-east-1                      (optional, default: us-east-1)
  --writer_provider     aws | gcp | azure               (optional, default: aws)
  --writer_batch_mode   true | false                    (optional, default: false — true buffers
                         all records per layer into one file instead of one file per record)
  --writer_auth_type    iam_role | access_key | secrets_manager  (optional, default: iam_role
                         — use iam_role for Glue; the job's IAM role needs s3:PutObject on writer_bucket)

ALERTING (both pipeline modes — NOT a Glue parameter):
  Entirely config.yaml-driven, via the top-level `alerting:` section
  (channel/channels + that channel's settings) — see
  alerts/channel_senders.py for the full channel list (sns, ses, sqs,
  slack, teams, email, pagerduty). There used to be a --alerts parameter
  here; it never actually worked (the classes it tried to load were never
  written), so it's been removed rather than left looking functional.

STRUCTURED PIPELINE — OPTIONAL (component selection, skipped if omitted):
  --logger              json | text | cloudwatch       (optional — falls back to stdout logging)
  --orchestrator        not a Glue parameter — passing it just logs a warning and has no
                         effect. Orchestration happens outside this job: generate an Airflow
                         DAG with orchestration/generate_dag.py (managed MWAA or self-hosted),
                         which triggers this job with your chosen parameters on a schedule.
  --pkg_s3_uri          s3://bucket/data_ingestion_engine.zip  (needed so bootstrap_glue_environment()
                         can extract the package if it isn't already unzipped in /tmp)

EXAMPLES:

Unstructured (auto-detected):
  --config_path s3://bucket/config.yaml
  --ecs_endpoint http://my-alb.elb.amazonaws.com
  --writer_bucket my-output-bucket

Structured (auto-detected):
  --config_path s3://bucket/config.yaml
  --pkg_s3_uri s3://bucket/data_ingestion_engine.zip
  --reader batch_api
  --architecture medallion
  --writer cloud
  --writer_bucket my-output-bucket

  NOTE: --pkg_s3_uri (or an --extra-py-files job-level setting pointing at
  the same zip) is REQUIRED here on a cold container — without it, nothing
  puts this package's modules on sys.path, and `from core.pipeline_builder
  import PipelineBuilder` fails with ModuleNotFoundError. A warm container
  that already extracted the package in a previous run, or a Glue job with
  --extra-py-files configured, doesn't strictly need it repeated — but
  always including it is the only way to make a run self-contained.

REUSING AN EXISTING JOB'S DEFAULT ARGUMENTS — READ THIS FIRST:
  If you're overriding an existing job's arguments for a one-off run (e.g.
  via `aws glue start-job-run --arguments '{...}'`) rather than building a
  fresh job, --pipeline_mode is checked BEFORE any other parameter and wins
  outright (see detect_pipeline_mode()) — it is NOT merely a hint. A job
  whose persisted defaults already include --pipeline_mode/--ecs_endpoint
  (e.g. this repo's unstructured job) will keep running unstructured even
  if your override adds --reader/--architecture/--writer, UNLESS your
  override also explicitly sets --pipeline_mode structured (or clears
  --pipeline_mode and --ecs_endpoint entirely). detect_pipeline_mode() now
  raises immediately if it sees an explicit --pipeline_mode alongside the
  other mode's parameters, specifically so this mismatch fails loudly
  instead of silently running the wrong pipeline.
"""

import os
import sys
import yaml
import boto3
import logging
import zipfile
import shutil
from pathlib import Path
from typing import Optional

# =============================================================================
# BOOTSTRAP: Extract and setup Python path for Glue
# =============================================================================

# When running in Glue, the zip is downloaded but not extracted
# We need to extract it and add to sys.path
def _find_module_root(extract_dir: str) -> Optional[str]:
    """Locate the directory that contains the package modules after extraction."""
    extract_path = Path(extract_dir)
    candidates = [
        extract_path,
        extract_path / "data_ingestion_engine",
    ]

    for candidate in candidates:
        if (candidate / "pipelines" / "unstructured_pipeline.py").exists() or \
           (candidate / "main" / "aimatic.py").exists() or \
           (candidate / "core" / "registry.py").exists():
            return str(candidate)

    for root, _, _ in os.walk(str(extract_path)):
        root_path = Path(root)
        if (root_path / "pipelines" / "unstructured_pipeline.py").exists():
            return str(root_path)

    return None


def _download_package_from_s3(pkg_s3_uri: str, destination: str) -> None:
    """Download a package zip from S3 to the local Glue temp directory."""
    bucket, key = pkg_s3_uri.replace("s3://", "").split("/", 1)
    boto3.client("s3").download_file(bucket, key, destination)


def bootstrap_glue_environment(args: Optional[dict] = None,
                               package_path: Optional[str] = None,
                               extract_dir: Optional[str] = None) -> Optional[str]:
    """Extract the package and add the module root to sys.path for Glue execution."""
    args = args or {}
    pkg_s3_uri = args.get("pkg_s3_uri") or os.getenv("PKG_S3_URI")

    if package_path is None:
        if pkg_s3_uri and pkg_s3_uri.startswith("s3://"):
            package_path = "/tmp/data_ingestion_engine_package.zip"
            log.info("Downloading Glue package from %s", pkg_s3_uri)
            _download_package_from_s3(pkg_s3_uri, package_path)
        else:
            candidates = [
                "/tmp/data_ingestion_engine_v2.zip",
                "/tmp/data_ingestion_engine.zip",
                "/tmp/data_ingestion_engine_package.zip",
            ]
            for candidate in candidates:
                if os.path.exists(candidate):
                    package_path = candidate
                    break

    if not package_path or not os.path.exists(package_path):
        log.info("No Glue package zip found; skipping bootstrap")
        return None

    extract_path = Path(extract_dir or "/tmp/data_ingestion_engine_extracted")
    # Always re-extract: Glue Python Shell can reuse a warm container across
    # job runs, so a stale extraction from a previous (possibly differently
    # laid out) package zip must never be reused.
    if extract_path.exists():
        shutil.rmtree(str(extract_path))
    log.info("Extracting %s to %s", package_path, extract_path)
    extract_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(package_path, 'r') as zip_ref:
        zip_ref.extractall(str(extract_path))

    module_root = _find_module_root(str(extract_path))
    if module_root:
        if str(module_root) not in sys.path:
            sys.path.insert(0, str(module_root))
            log.info("✓ Added %s to sys.path", module_root)
        return module_root

    log.warning("Could not determine module root from extracted package: %s", package_path)
    return None

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,  # Glue's Python Shell runtime pre-attaches a root handler,
                 # which makes basicConfig() a no-op (and silently drops
                 # every INFO log) unless we force reconfiguration here.
)
log = logging.getLogger("aimatic")
log.setLevel(logging.INFO)

# Bootstrap environment before any imports
bootstrap_glue_environment()

# =============================================================================
# ARGUMENT PARSING
# =============================================================================

def get_glue_args():
    """Parse Glue job arguments from sys.argv."""
    args = {}
    argv = sys.argv
    i = 1
    while i < len(argv):
        if argv[i].startswith("--"):
            key = argv[i][2:]
            if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                args[key] = argv[i + 1]
                i += 2
            else:
                args[key] = True
                i += 1
        else:
            i += 1
    return args

# =============================================================================
# YAML CONFIG LOADER
# =============================================================================

def load_yaml_config(path):
    """Load YAML config from local file or S3."""
    if path.startswith("s3://"):
        s3 = boto3.client("s3")
        bucket, key = path.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return yaml.safe_load(obj["Body"].read())
    else:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

# =============================================================================
# PIPELINE MODE DETECTION
# =============================================================================

def detect_pipeline_mode(args):
    """
    Auto-detect pipeline mode based on parameters passed.

    UNSTRUCTURED indicators:
      - --ecs_endpoint (required for unstructured)

    STRUCTURED indicators:
      - --reader (required for structured)
      - --architecture (required for structured)

    Returns:
      "unstructured" or "structured"
    """
    has_ecs_endpoint = "ecs_endpoint" in args
    has_reader = "reader" in args
    has_architecture = "architecture" in args

    # Explicit mode takes precedence over auto-detection — but an explicit
    # --pipeline_mode alongside the OTHER mode's parameters is almost always
    # a mistake, not an intentional choice: it means a Glue job's persisted
    # --pipeline_mode/--ecs_endpoint defaults are still active while a
    # one-off --arguments override added --reader/--architecture (or vice
    # versa), and the override would otherwise be silently ignored — the
    # job "succeeds" having run the wrong pipeline with no visible error.
    # Fail loudly here instead, before any real work starts.
    explicit_mode = args.get("pipeline_mode", "").lower()
    if explicit_mode:
        if explicit_mode not in ["unstructured", "structured"]:
            raise ValueError(
                f"Invalid --pipeline_mode: '{explicit_mode}'. "
                f"Valid options: 'unstructured', 'structured'"
            )

        if explicit_mode == "unstructured" and (has_reader or has_architecture):
            raise ValueError(
                "Conflicting parameters: --pipeline_mode is explicitly 'unstructured', but "
                "--reader/--architecture (structured-only parameters) are also present. This "
                "usually means a Glue job's persisted --pipeline_mode/--ecs_endpoint defaults "
                "are still active while you overrode --reader/--architecture for this run — "
                "left as-is, this run would silently keep running unstructured and ignore your "
                "override. Either remove --reader/--architecture, or pass "
                "--pipeline_mode structured (and drop --ecs_endpoint) to actually run structured."
            )

        if explicit_mode == "structured" and has_ecs_endpoint:
            raise ValueError(
                "Conflicting parameters: --pipeline_mode is explicitly 'structured', but "
                "--ecs_endpoint (an unstructured-only parameter) is also present. Remove "
                "--ecs_endpoint, or pass --pipeline_mode unstructured to run the unstructured "
                "pipeline instead."
            )

        log.info("Pipeline mode explicitly set: %s", explicit_mode)
        return explicit_mode

    # Auto-detect based on parameters
    log.info("Auto-detecting pipeline mode...")
    log.info("  --ecs_endpoint present: %s", has_ecs_endpoint)
    log.info("  --reader present: %s", has_reader)
    log.info("  --architecture present: %s", has_architecture)
    
    if has_ecs_endpoint and not has_reader and not has_architecture:
        detected_mode = "unstructured"
        log.info("✓ Detected: UNSTRUCTURED pipeline (ecs_endpoint found)")
        return detected_mode
    
    elif (has_reader or has_architecture) and not has_ecs_endpoint:
        detected_mode = "structured"
        log.info("✓ Detected: STRUCTURED pipeline (reader/architecture found)")
        return detected_mode
    
    elif has_ecs_endpoint and (has_reader or has_architecture):
        raise ValueError(
            "Ambiguous parameters: Found both unstructured (--ecs_endpoint) "
            "and structured (--reader/--architecture) parameters. "
            "Please specify --pipeline_mode explicitly."
        )
    
    else:
        raise ValueError(
            "Cannot detect pipeline mode. Please provide either:\n"
            "  For UNSTRUCTURED: --ecs_endpoint (or --pipeline_mode unstructured)\n"
            "  For STRUCTURED: --reader and --architecture (or --pipeline_mode structured)"
        )


if __name__ == "__main__":
    log.info("=" * 80)
    log.info("AIMATIC - DATA INGESTION PIPELINE")
    log.info("=" * 80)
    
    try:
        # Parse arguments
        args = get_glue_args()
        bootstrap_glue_environment(args)

        log.info("Arguments received: %s", {k: v for k, v in args.items() 
                                            if k not in ["password", "secret", "key"]})
        
        # Load configuration
        config_path = args.get("config_path")
        if not config_path:
            raise ValueError("--config_path is required")
        
        log.info("Loading config: %s", config_path)
        config = load_yaml_config(config_path)
        log.info("✓ Config loaded")
        
        # Detect or get pipeline mode
        pipeline_mode = detect_pipeline_mode(args)
        
        log.info("=" * 80)
        log.info("PIPELINE MODE: %s", pipeline_mode.upper())
        log.info("=" * 80)
        
        # Route to appropriate pipeline
        if pipeline_mode == "unstructured":
            log.info("Delegating to UNSTRUCTURED pipeline module")
            log.info("Required parameters:")
            log.info("  ✓ --config_path: %s", config_path)
            log.info("  ✓ --ecs_endpoint: %s", args.get("ecs_endpoint", "MISSING"))
            log.info("  ✓ --writer_bucket: %s", args.get("writer_bucket", "MISSING"))
            log.info("  • --writer_prefix: %s (optional)", args.get("writer_prefix", "unstructured/"))
            log.info("  • --writer_region: %s (optional)", args.get("writer_region", "us-east-1"))
            log.info("─" * 80)
            
            # Import and run unstructured pipeline
            # When running as python -m main.aimatic from data_ingestion_engine directory
            # or as AWS Glue job, need to use correct import path
            try:
                from pipelines.unstructured_pipeline import run_unstructured_pipeline
            except ModuleNotFoundError:
                # Fallback for different execution contexts
                import sys
                from pathlib import Path
                parent = Path(__file__).parent.parent
                if str(parent) not in sys.path:
                    sys.path.insert(0, str(parent))
                from pipelines.unstructured_pipeline import run_unstructured_pipeline
            
            # Build writer config from Glue parameters
            writer_config = {
                "provider": "aws",
                "bucket": args.get("writer_bucket"),
                "prefix": args.get("writer_prefix", "unstructured/"),
                "region": args.get("writer_region", "us-east-1"),
                "file_format": "json",
                "batch_mode": False
            }

            # Fail fast with a clear message rather than silently overwriting
            # any valid target.bucket already in config.yaml with None —
            # the structured pipeline's PipelineBuilder already validates
            # --writer_bucket the same way; this path didn't.
            if not writer_config["bucket"]:
                raise ValueError(
                    "--writer_bucket parameter is required for the unstructured pipeline. "
                    "Example: --writer_bucket my-output-bucket"
                )

            # Add writer config to config dict for the pipeline
            if "unstructured" not in config:
                config["unstructured"] = {}
            if "target" not in config["unstructured"]:
                config["unstructured"]["target"] = {}
            
            config["unstructured"]["target"].update({
                "bucket": writer_config["bucket"],
                "output_prefix": writer_config["prefix"],
                "region": writer_config["region"],
                "batch_mode": writer_config["batch_mode"]
            })
            
            # Get ECS endpoint from parameter
            ecs_endpoint = args.get("ecs_endpoint")
            
            # Run unstructured pipeline
            run_unstructured_pipeline(config, ecs_endpoint)
            
            log.info("=" * 80)
            log.info("✓ UNSTRUCTURED PIPELINE COMPLETED SUCCESSFULLY")
            log.info("=" * 80)
            
        elif pipeline_mode == "structured":
            log.info("Delegating to STRUCTURED pipeline module")
            log.info("Required parameters:")
            log.info("  ✓ --config_path: %s", config_path)
            log.info("  ✓ --reader: %s", args.get("reader", "MISSING"))
            log.info("  ✓ --architecture: %s", args.get("architecture", "MISSING"))
            log.info("  ✓ --writer: %s", args.get("writer", "MISSING"))
            log.info("  ✓ --writer_bucket: %s", args.get("writer_bucket", "MISSING"))
            log.info("  • --writer_prefix: %s (optional)", args.get("writer_prefix", "structured/"))
            log.info("  • --writer_format: %s (optional)", args.get("writer_format", "parquet"))
            log.info("  • --writer_region: %s (optional)", args.get("writer_region", "us-east-1"))
            log.info("─" * 80)
            
            # Import and run structured pipeline via PipelineBuilder.
            # This is the ONLY structured code path — pipelines/structured_pipeline.py
            # is legacy PySpark code, is never imported here, and is currently broken
            # (imports functions that don't exist on the Bronze/Silver/Gold classes).
            #
            # Required Glue params consumed below: --reader, --architecture, --writer,
            # --writer_bucket. Optional: --writer_prefix, --writer_format, --writer_region,
            # --writer_provider, --writer_batch_mode, --writer_auth_type, --logger.
            # See module docstring at the top of this file for full details.
            # (Alerting is config.yaml's `alerting:` section, not a Glue param — see above.
            # Orchestration is also not a Glue param — see above — it happens outside this
            # job via orchestration/generate_dag.py.)
            from core.pipeline_builder import PipelineBuilder

            # Build pipeline using PipelineBuilder
            builder = PipelineBuilder(config_path, args)
            pipeline = builder.build()
            
            # Execute pipeline
            result = pipeline.execute()
            
            log.info("=" * 80)
            log.info("✓ STRUCTURED PIPELINE COMPLETED SUCCESSFULLY")
            log.info("=" * 80)
            log.info("Result: %s", result)
        
        else:
            raise ValueError(f"Unknown pipeline mode: {pipeline_mode}")
    
    except Exception as e:
        log.error("=" * 80)
        log.error("✗ PIPELINE FAILED")
        log.error("=" * 80)
        log.error("Error: %s", e, exc_info=True)
        sys.exit(1)
