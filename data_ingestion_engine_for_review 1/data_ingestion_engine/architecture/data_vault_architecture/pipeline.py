"""
architecture/data_vault_architecture/pipeline.py
─────────────────────────────────────────────────
Data Vault Architecture: Hub → Link → Satellite load order. Insert-only.

Model:
  Hub       — a business key, nothing else (customer_id, order_id).
              Insert-only, immutable once created.
  Link      — a relationship between two hubs (order <-> customer).
              Insert-only.
  Satellite — descriptive attributes WITH full history: a changed
              attribute inserts a NEW row rather than updating in place,
              so "what we knew, as of any point in time" is reconstructible.

Load order is critical: Hubs must exist before Links/Satellites reference
them by hash key.

Rebuilt from scratch — the previous version had three real problems:
  1. It imported `from architecture.base import BaseArchitecture`, a
     module that doesn't exist anywhere in this repo — ModuleNotFoundError
     before any of its own logic ran.
  2. It had no execute() method (only setup()/run()/teardown()), but
     main/aimatic.py calls pipeline.execute() — what every other
     architecture actually implements.
  3. Every load stage called writer.write([]) — an empty list, with the
     real row-construction logic entirely commented out. It would have
     logged "all entities loaded" and written zero rows anywhere, even
     past the import crash.

Honest limitation, by design, not fixed here: Satellite loading compares
hash_diff only WITHIN the current run's input — if a key's value hasn't
changed since a PREVIOUS run, this will still insert a duplicate satellite
row, because none of this codebase's writers support a read-back query
(they're write(record)/flush() only — there's no way to ask "what hash_diff
did we last load for this key?"). A fully behaving Data Vault pipeline
needs that read-back before deciding whether to insert; adding it would
mean adding a query capability to CloudWriter/DataWarehouseWriter/
DatabaseWriter, which is separate work, not done here.

Hub/Link/Satellite rows are routed to different physical destinations
using the same mechanism Lambda/Medallion use to route layer output: a
table-name override on flush() for DatabaseWriter/DataWarehouseWriter
(which otherwise always write to one configured table), or prefix
reassignment for CloudWriter (which has no table concept at all).

Config (config.yaml, under `data_vault:`):
    hubs:
      - business_key: customer_id   # field name on the staged records
        source: customers            # record_source tag
    links:
      - hub_a: customer_id           # field name for the first hub's key
        hub_b: order_id              # field name for the second hub's key
        source: orders
    satellites:
      - business_key: customer_id    # which hub this satellite describes
        attribute_columns: [name, address, status]
        source: customers

Interface: write(record)/flush(table_name=...) per entity — this
architecture reads batch sources only (see pipeline_builder.py's
streaming-reader guard: Data Vault has no bounded-drain mechanism, same
reasoning as Medallion).
"""

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Dict, List

from alerts.channel_senders import create_notification_service
from core.alert_rules import check_alert_rules

log = logging.getLogger(__name__)


def md5_hash(value: Any) -> str:
    """Deterministic surrogate key. Uppercase + strip before hashing."""
    normalised = str(value).strip().upper() if value not in (None, "") else "^^"
    return hashlib.md5(normalised.encode()).hexdigest()


class DataVaultArchitecture:
    """Hub -> Link -> Satellite loader. Each stage writes its own entity."""

    def __init__(self, config: dict, writers: List = None):
        self.config = config
        self.writers = writers or []
        self.dv_config = config.get("data_vault", {})

        for writer in self.writers:
            writer._batch_mode = True

        log.info("DataVaultArchitecture: initialized with %d writer(s)", len(self.writers))

    def execute(self, raw_records: List[Dict[str, Any]]) -> Dict[str, int]:
        log.info("DataVaultArchitecture: executing with %d record(s)", len(raw_records))

        hub_count = self._load_hubs(raw_records)
        link_count = self._load_links(raw_records)
        satellite_count = self._load_satellites(raw_records)

        log.info("DataVaultArchitecture: all entities loaded (%d hub, %d link, %d satellite row(s))",
                 hub_count, link_count, satellite_count)
        return {"hub_rows": hub_count, "link_rows": link_count, "satellite_rows": satellite_count}

    # ── Hubs ─────────────────────────────────────────────────────────────────

    def _load_hubs(self, staged: List[Dict[str, Any]]) -> int:
        """Insert-only: one row per unique business key seen in this run."""
        hubs_cfg = self.dv_config.get("hubs", [])
        load_dts = datetime.utcnow().isoformat()
        total = 0

        for hub in hubs_cfg:
            biz_key = hub.get("business_key")
            source_name = hub.get("source", "unknown")
            if not biz_key:
                log.warning("DataVaultArchitecture: hub config missing business_key — skipping: %s", hub)
                continue

            seen = {}
            for row in staged:
                value = row.get(biz_key)
                if value is not None and value not in seen:
                    seen[value] = True

            rows = [
                {
                    "hash_key": md5_hash(value),
                    "business_key": value,
                    "load_dts": load_dts,
                    "record_source": source_name,
                }
                for value in seen
            ]
            self._write_entity(f"hub_{biz_key}", rows)
            total += len(rows)

        return total

    # ── Links ────────────────────────────────────────────────────────────────

    def _load_links(self, staged: List[Dict[str, Any]]) -> int:
        """Insert-only: one row per unique (hub_a, hub_b) pair seen in this run."""
        links_cfg = self.dv_config.get("links", [])
        load_dts = datetime.utcnow().isoformat()
        total = 0

        for link in links_cfg:
            hub_a, hub_b = link.get("hub_a"), link.get("hub_b")
            source_name = link.get("source", "unknown")
            if not hub_a or not hub_b:
                log.warning("DataVaultArchitecture: link config missing hub_a/hub_b — skipping: %s", link)
                continue

            seen = {}
            for row in staged:
                a_val, b_val = row.get(hub_a), row.get(hub_b)
                if a_val is None or b_val is None:
                    continue
                seen[(a_val, b_val)] = True

            rows = [
                {
                    "link_hk": md5_hash(f"{a_val}|{b_val}"),
                    "hub_a_hk": md5_hash(a_val),
                    "hub_b_hk": md5_hash(b_val),
                    "load_dts": load_dts,
                    "record_source": source_name,
                }
                for (a_val, b_val) in seen
            ]
            self._write_entity(f"link_{hub_a}_{hub_b}", rows)
            total += len(rows)

        return total

    # ── Satellites ───────────────────────────────────────────────────────────

    def _load_satellites(self, staged: List[Dict[str, Any]]) -> int:
        """
        Insert a new row only when hash_diff changes for a given key,
        tracked across this run's input order. See the module docstring
        for why this can't detect "unchanged since a previous run" —
        no writer here supports the read-back that would require.
        """
        satellites_cfg = self.dv_config.get("satellites", [])
        load_dts = datetime.utcnow().isoformat()
        total = 0

        for sat in satellites_cfg:
            biz_key = sat.get("business_key")
            attr_cols = sat.get("attribute_columns", [])
            source_name = sat.get("source", "unknown")
            if not biz_key or not attr_cols:
                log.warning(
                    "DataVaultArchitecture: satellite config missing business_key/"
                    "attribute_columns — skipping: %s", sat
                )
                continue

            last_hash_diff: Dict[Any, str] = {}
            rows = []
            for row in staged:
                key_value = row.get(biz_key)
                if key_value is None:
                    continue
                hash_diff = md5_hash("|".join(str(row.get(c, "")) for c in attr_cols))
                if last_hash_diff.get(key_value) == hash_diff:
                    continue  # unchanged since the last time this key appeared in this run
                last_hash_diff[key_value] = hash_diff
                rows.append({
                    "hub_hk": md5_hash(key_value),
                    "hash_diff": hash_diff,
                    "load_dts": load_dts,
                    "record_source": source_name,
                    **{col: row.get(col) for col in attr_cols},
                })

            self._write_entity(f"satellite_{biz_key}", rows)
            total += len(rows)

        return total

    # ── Writing (routes each entity to its own table/prefix) ────────────────

    def _write_entity(self, entity_name: str, records: List[Dict[str, Any]]) -> None:
        if not self.writers:
            log.debug("DataVaultArchitecture: no writers configured, skipping %s", entity_name)
            return
        if not records:
            log.warning("DataVaultArchitecture: no %s record(s) to write", entity_name)
            return

        for writer in self.writers:
            try:
                use_table_override = bool(getattr(writer, "table_name", None))
                original_prefix = getattr(writer, "prefix", "") or ""
                if not use_table_override:
                    writer.prefix = f"{original_prefix}{entity_name}/"

                for record in records:
                    writer.write(record)

                if use_table_override:
                    writer.flush(table_name=f"{writer.table_name}_{entity_name}")
                else:
                    writer.flush()
                    writer.prefix = original_prefix

                log.info("DataVaultArchitecture: wrote %d %s record(s) via %s",
                         len(records), entity_name, writer.__class__.__name__)
            except Exception as exc:
                log.error("DataVaultArchitecture: writer %s failed for %s — %s",
                          writer.__class__.__name__, entity_name, exc)
                raise


class Pipeline:
    """
    Entry point the UI/CLI runs when --architecture data_vault is
    selected. Resolved by core/registry.py and instantiated by
    core/pipeline_builder.py with the same five arguments every other
    architecture gets.
    """

    def __init__(self, config: dict, readers: List, writers: List,
                 orchestrator=None, alert_handlers: List = None):
        self.config = config
        self.readers = readers
        self.writers = writers
        self.orchestrator = orchestrator
        self.alert_handlers = alert_handlers or []
        self.architecture = DataVaultArchitecture(config, writers=writers)
        self.notifier = create_notification_service(config.get("alerting", {}))

        log.info("DataVaultPipeline: initialized with %d readers, %d writers",
                 len(readers), len(writers))

    def _reader_source_label(self) -> str:
        return ", ".join(r.__class__.__name__ for r in self.readers) or "unknown"

    def _writer_label(self) -> str:
        return ", ".join(w.__class__.__name__ for w in self.writers) or "none"

    def execute(self) -> Dict[str, Any]:
        start_time = time.time()
        try:
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

            log.info("DataVaultPipeline: collected %d record(s) from %d reader(s)",
                     len(raw_records), len(self.readers))

            if not raw_records:
                log.warning("DataVaultPipeline: no records from readers")
                return {"status": "warning", "records": 0}

            counts = self.architecture.execute(raw_records)
            total_rows = sum(counts.values())

            self.notifier.notify(
                "ingestion_success",
                source=self._reader_source_label(),
                architecture="data_vault",
                writer=self._writer_label(),
                record_count=total_rows,
                duration_seconds=round(time.time() - start_time, 2),
            )
            check_alert_rules(
                self.config, self.notifier,
                source=self._reader_source_label(),
                start_time=start_time,
                total_records=len(raw_records),
                failed_records=0,
            )

            return {"status": "success", **counts}

        except Exception as exc:
            log.error("DataVaultPipeline: execution failed — %s", exc, exc_info=True)
            self.notifier.notify(
                "ingestion_failure",
                source=self._reader_source_label(),
                architecture="data_vault",
                writer=self._writer_label(),
                error=str(exc),
                duration_seconds=round(time.time() - start_time, 2),
            )
            raise
