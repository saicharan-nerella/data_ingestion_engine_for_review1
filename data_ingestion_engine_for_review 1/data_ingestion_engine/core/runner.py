"""
core/runner.py
──────────────
Main entrypoint. Reads config, builds pipeline, runs it.
Called by orchestrators (Airflow task, Prefect flow, cron job, etc.)

Usage:
    python -m core.runner
    python -m core.runner --config config/config.yaml
"""

import argparse
import sys
import logging
from core.pipeline_builder import PipelineBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
log = logging.getLogger(__name__)


def main(config_path: str = "config/config.yaml"):
    log.info("Runner: starting pipeline from config=%s", config_path)
    try:
        builder = PipelineBuilder(config_path)
        pipeline = builder.build()
        pipeline.execute()
        log.info("Runner: pipeline completed successfully")
        return 0
    except Exception as exc:
        log.error("Runner: pipeline failed — %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Ingestion Engine runner")
    parser.add_argument("--config", default="config/config.yaml",
                        help="Path to config.yaml")
    args = parser.parse_args()
    sys.exit(main(args.config))
