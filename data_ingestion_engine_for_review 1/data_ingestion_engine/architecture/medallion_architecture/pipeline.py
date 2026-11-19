"""
architecture/medallion_architecture/pipeline.py
────────────────────────────────────────────────
Medallion Architecture: Three-Layer Data Processing with Separate Outputs

Responsibility:
- Orchestrates Bronze, Silver, and Gold layers
- Each layer writes its own output to target storage
- Handles PII masking (via SilverLayer reading config)
- Returns all layer outputs to caller

Data Flow:
    raw_records → Bronze Layer (add metadata) → Write to Bronze Target
              → Silver Layer (clean, normalize, validate, mask PII) → Write to Silver Target
              → Gold Layer (enrich) → Write to Gold Target

Key Points:
- Each layer has its own output location
- Bronze: Raw data + ingestion metadata
- Silver: Cleaned, normalized, PII-masked data
- Gold: Business-ready, enriched data
- Each layer is self-contained
- SilverLayer reads PII config internally
"""

import logging
import time
from typing import List, Dict, Any
from datetime import datetime

from architecture.medallion_architecture.bronze_layer import BronzeLayer
from architecture.medallion_architecture.silver_layer import SilverLayer
from architecture.medallion_architecture.gold_layer import GoldLayer
from alerts.channel_senders import create_notification_service
from core.alert_rules import check_alert_rules

log = logging.getLogger(__name__)


class MedallionArchitecture:
    """
    Medallion Architecture: Three-layer data processing with separate outputs.
    
    Orchestrates three layers of data transformation:
    1. Bronze: Add metadata to raw records → Write to bronze target
    2. Silver: Clean, normalize, validate, mask PII → Write to silver target
    3. Gold: Enrich and deduplicate → Write to gold target
    
    Each layer is self-contained and writes its own output.
    """
    
    def __init__(self, config: dict, writers: List = None):
        """
        Initialize the Medallion architecture.
        
        Args:
            config: Full configuration from config.yaml
            writers: List of writer instances (optional, for direct writing)
        """
        self.config = config
        self.writers = writers or []
        self.bronze_layer = BronzeLayer(config)
        self.silver_layer = SilverLayer(config)
        self.gold_layer = GoldLayer(config)
        
        # Force batch mode on all writers so records are buffered
        # and written as a single file per layer
        for writer in self.writers:
            writer._batch_mode = True
        
        log.info("MedallionArchitecture: initialized with %d writer(s)", len(self.writers))

    
    def execute(self, raw_records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Execute the full Medallion architecture with separate layer outputs.
        
        Process:
        1. Bronze layer: Add metadata to raw records → Write to bronze target
        2. Silver layer: Clean, normalize, validate, mask PII → Write to silver target
        3. Gold layer: Enrich records → Write to gold target
        
        Args:
            raw_records: Raw records from reader
        
        Returns:
            Dictionary with outputs from each layer:
            {
                "bronze": [bronze_records],
                "silver": [silver_records],
                "gold": [gold_records]
            }
        """
        try:
            log.info("MedallionArchitecture: executing with %d records", len(raw_records))
            
            # ── BRONZE LAYER: Add metadata ────────────────────────────────────
            log.info("MedallionArchitecture: STEP 1 - Bronze layer")
            bronze_records = self.bronze_layer.process(raw_records)
            log.info("  → %d records", len(bronze_records))
            
            # Write bronze layer output
            self._write_layer_output("bronze", bronze_records)
            
            # ── SILVER LAYER: Clean, normalize, validate, mask PII ────────────
            log.info("MedallionArchitecture: STEP 2 - Silver layer")
            silver_records = self.silver_layer.process(bronze_records)
            log.info("  → %d records", len(silver_records))
            
            # Write silver layer output
            self._write_layer_output("silver", silver_records)
            
            # ── GOLD LAYER: Enrich ────────────────────────────────────────────
            log.info("MedallionArchitecture: STEP 3 - Gold layer")
            gold_records = self.gold_layer.process(silver_records)
            log.info("  → %d records", len(gold_records))
            
            # Write gold layer output
            self._write_layer_output("gold", gold_records)
            
            log.info("MedallionArchitecture: completed successfully")
            
            return {
                "bronze": bronze_records,
                "silver": silver_records,
                "gold": gold_records
            }
        
        except Exception as exc:
            log.error("MedallionArchitecture: execution failed — %s", exc, exc_info=True)
            raise
    
    def _write_layer_output(self, layer_name: str, records: List[Dict[str, Any]]) -> None:
        """
        Write layer output to target storage, routed per layer the same way
        DataVaultArchitecture routes Hub/Link/Satellite (see its module
        docstring): a table-name override on flush() for table-based
        writers (DatabaseWriter/DataWarehouseWriter — which otherwise
        always write to the ONE configured table), or prefix reassignment
        for CloudWriter (which has no table concept at all).

        Bronze/Silver/Gold are not three independent datasets — Silver is a
        cleaned copy of Bronze's rows, Gold an enriched copy of Silver's —
        so sending all three into one shared table would both crash
        (Silver/Gold add columns Bronze's auto-created table doesn't have)
        and silently triple-count every record if it didn't crash. Each
        layer gets its own table/prefix instead, exactly like each layer
        already gets its own file under object storage.

        Args:
            layer_name: Name of the layer (bronze, silver, gold)
            records: Records to write
        """
        if not self.writers:
            log.debug("MedallionArchitecture: no writers configured, skipping %s layer output", layer_name)
            return

        for writer in self.writers:
            try:
                # Write all records (buffered because batch_mode=True)
                for record in records:
                    if isinstance(record, dict):
                        writer.write(record)
                    else:
                        log.warning("MedallionArchitecture: skipping non-dict record in %s layer: %s",
                                    layer_name, type(record))

                base_table = getattr(writer, "table_name", None)
                if base_table:
                    # Table-based writer — one table per layer.
                    writer.flush(table_name=f"{base_table}_{layer_name}")
                else:
                    # Object-storage writer — one prefix per layer.
                    original_prefix = getattr(writer, "prefix", "") or ""
                    writer.prefix = f"{original_prefix}{layer_name}/"
                    writer.flush()
                    writer.prefix = original_prefix

                log.info("MedallionArchitecture: wrote %d %s records via %s",
                         len(records), layer_name, writer.__class__.__name__)

            except Exception as exc:
                log.error("MedallionArchitecture: writer %s failed for %s layer — %s",
                          writer.__class__.__name__, layer_name, exc)
                raise


# ── Entry point ─────────────────────────────────────────────────────────────
# This is the class core/registry.py actually resolves "medallion" to and
# core/pipeline_builder.py instantiates — not a legacy/back-compat shim.
# It owns reading from every configured reader, then delegates the real
# bronze/silver/gold processing and per-layer writing to
# MedallionArchitecture above (self.architecture.execute(...)).

class Pipeline:
    """
    Entry point the UI/CLI runs when --architecture medallion is selected.
    Resolved by core/registry.py and instantiated by
    core/pipeline_builder.py with the same five arguments every other
    architecture gets.

    Collects raw records from every configured reader, then hands them to
    MedallionArchitecture, which runs Bronze → Silver → Gold and writes
    each layer's output through whichever --writer the user chose.
    """

    def __init__(self, config: dict, readers: List, writers: List,
                 orchestrator, alert_handlers: List):
        """
        Initialize the Medallion pipeline.
        
        Args:
            config: Full configuration from config.yaml
            readers: List of reader instances
            writers: List of writer instances
            orchestrator: Orchestrator instance
            alert_handlers: List of alert handler instances
        """
        self.config = config
        self.readers = readers
        self.writers = writers
        self.orchestrator = orchestrator
        self.alert_handlers = alert_handlers
        # Pass writers to architecture so each layer can write its output
        self.architecture = MedallionArchitecture(config, writers=writers)

        # Real, working notification path — driven entirely by config.yaml's
        # `alerting` section (channel + on/off per event type). Channel-
        # agnostic: this doesn't care whether that section says sns, slack,
        # teams, email, or nothing at all — see alerts/channel_senders.py.
        # (Separate from self.alert_handlers above, which is the --alerts
        # CLI/registry mechanism; that one's classes don't exist yet and
        # stays untouched/unused here.)
        self.notifier = create_notification_service(config.get("alerting", {}))

        log.info("MedallionPipeline: initialized with %d readers, %d writers",
                 len(readers), len(writers))

    def _reader_source_label(self) -> str:
        """Best-effort, human-readable label for notification messages."""
        return ", ".join(r.__class__.__name__ for r in self.readers) or "unknown"

    def _writer_label(self) -> str:
        """Best-effort, human-readable label for notification messages."""
        return ", ".join(w.__class__.__name__ for w in self.writers) or "none"

    def execute(self) -> Dict[str, Any]:
        """
        Execute the full Medallion pipeline.

        Returns:
            Dictionary with execution results and layer outputs
        """
        start_time = time.time()
        try:
            # Collect raw records from all readers
            # reader.read() is a generator that yields List[Dict] batches
            raw_records = []
            for reader in self.readers:
                try:
                    reader.connect()
                    for batch in reader.read():
                        if isinstance(batch, list):
                            raw_records.extend(batch)
                        elif isinstance(batch, dict):
                            raw_records.append(batch)
                finally:
                    reader.close()

            log.info("MedallionPipeline: collected %d raw records from %d reader(s)",
                     len(raw_records), len(self.readers))

            if not raw_records:
                log.warning("MedallionPipeline: no records from readers")
                return {"status": "warning", "records": 0}

            # Execute architecture (writes each layer output)
            layer_outputs = self.architecture.execute(raw_records)

            self.notifier.notify(
                "ingestion_success",
                source=self._reader_source_label(),
                architecture="medallion",
                writer=self._writer_label(),
                record_count=len(layer_outputs["gold"]),
                duration_seconds=round(time.time() - start_time, 2),
            )

            # alerting.rules — error rate / run duration / throughput checks.
            # "Failed" here means Silver caught a per-record exception and
            # tagged it with _silver_validation_error rather than dropping
            # it (see silver_layer.py's process()) — the one place in this
            # architecture that actually classifies a record as failed.
            failed_records = sum(
                1 for r in layer_outputs["silver"] if "_silver_validation_error" in r
            )
            check_alert_rules(
                self.config, self.notifier,
                source=self._reader_source_label(),
                start_time=start_time,
                total_records=len(raw_records),
                failed_records=failed_records,
            )

            return {
                "status": "success",
                "bronze_records": len(layer_outputs["bronze"]),
                "silver_records": len(layer_outputs["silver"]),
                "gold_records": len(layer_outputs["gold"]),
            }

        except Exception as exc:
            log.error("MedallionPipeline: execution failed — %s", exc, exc_info=True)
            self.notifier.notify(
                "ingestion_failure",
                source=self._reader_source_label(),
                architecture="medallion",
                writer=self._writer_label(),
                error=str(exc),
                duration_seconds=round(time.time() - start_time, 2),
            )
            raise

    # NOTE: this previously had _execute_gold_layer/_write_to_target/
    # _get_writer_options helper methods and a create_pipeline() factory
    # function below. None were ever called from execute() (dead code —
    # confirmed via repo-wide grep) and _execute_gold_layer's call to
    # GoldLayer(config, silver_output_path=..., writer_class=...,
    # writer_options=...) didn't even match the real GoldLayer.__init__(self,
    # config) — it would have raised TypeError if anything had ever called
    # it. Removed rather than left looking like a real alternate code path.
    #
    # Similarly, _notify_success/_notify_warning/_notify_failure (which
    # iterated self.alert_handlers, calling .notify_success()/
    # .notify_warning()/.notify_failure() on each) were removed earlier —
    # execute() never called them, and self.alert_handlers is always empty
    # in practice (see core/pipeline_builder.py). Real success/failure
    # notification is self.notifier.notify(...) above, driven by
    # config.yaml's `alerting:` section via alerts/channel_senders.py.
