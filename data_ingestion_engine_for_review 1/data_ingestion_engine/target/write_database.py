"""
target/write_database.py
────────────────────────
Relational Database Writer — PostgreSQL, MySQL, Oracle, SQL Server, all
through one SQLAlchemy-based implementation (SQLAlchemy is already a hard
dependency of structured_readers/batch_database_read.py, so this isn't a
new dependency — it's removing four parallel, drifting copies of the same
connect/insert logic down to one, the same way batch_database_read.py
already connects to all four engines through one `create_engine()` call).

Rebuilt from scratch — the previous version had four real problems:
  1. write() took a batch list with mode/partition_cols as call arguments.
     LambdaArchitecture/MedallionArchitecture call every writer the same
     way — write(record) one at a time, then flush() — so this would have
     crashed (no flush() existed) the moment anyone selected this writer
     through a real pipeline. Same class of bug the warehouse writer had
     before being fixed to match that contract; fixed here the same way.
  2. mode="merge" silently fell through to a plain INSERT in every
     _write_* method — no merge branch existed anywhere.
  3. upsert() built a MERGE SQL string and only logged it
     ("Execute this MERGE SQL in database:") — never ran it. Worse, that
     SQL was invalid everywhere it would have been run: Postgres and
     MySQL have no MERGE statement at all, and even Oracle/SQL Server's
     real MERGE doesn't support the `UPDATE SET *` / `INSERT *` shorthand
     it generated.
  4. _write_mysql imported `mysql.connector` (mysql-connector-python), but
     the Glue job's --additional-python-modules only installs `pymysql` —
     MySQL writes would fail to even import in the deployed environment.

Real per-dialect upsert, now:
  - Postgres: one bulk `INSERT ... ON CONFLICT (merge_keys) DO UPDATE SET
    ...` statement (sqlalchemy.dialects.postgresql.insert).
  - MySQL: one bulk `INSERT ... ON DUPLICATE KEY UPDATE ...` statement
    (sqlalchemy.dialects.mysql.insert).
  - Oracle / SQL Server: neither has a bulk-upsert statement shape, so
    these stage into a side table, then run one real, explicit-column
    MERGE against it — the same stage-then-merge shape as the Redshift
    writer (target/write_data_warehouse.py), not a MERGE per row.
  Postgres' ON CONFLICT and MySQL's ON DUPLICATE KEY UPDATE both require a
  unique constraint/index on the merge columns to have anything to match
  against — if the table already existed without one, the database will
  raise a clear error (Postgres) or silently insert plain duplicates
  (MySQL) rather than upserting. auto_create_table adds that constraint
  when it creates the table; it does not retroactively alter an existing
  one.

Network reachability: a Glue Python-shell job has no VPC Connection by
default, and these are raw TCP DB connections (unlike the Redshift
writer's Data API, there's no HTTPS-only alternative for traditional
RDS/self-hosted Postgres/MySQL/Oracle/SQL Server) — reaching a database in
a private subnet needs a Glue VPC Connection plus a security-group rule
allowing inbound from it. That's infrastructure, not something this file
can do for itself; connection failures that look like a timeout are
reported with that pointer rather than a bare driver traceback.

Config (all via --writer_<x> Glue parameters — see
core/pipeline_builder.py's generic writer_* passthrough):
    db_type              postgres | mysql | oracle | sqlserver (required)
    host, port           connection target (port defaults per db_type)
    database             database/service name (required)
    username, password   credentials (required, unless secret_arn is set)
    secret_arn           Secrets Manager secret ARN — alternative to
                         username/password, expects the standard RDS
                         secret JSON shape (username/password/host/port/
                         dbname), avoiding plaintext credentials in Glue
                         job parameters
    schema               optional schema (Postgres/SQL Server only)
    table                target table name (required)
    mode                 append | overwrite | merge (default: append)
    merge_keys           comma-separated column(s) identifying a unique
                         row — required when mode=merge
    auto_create_table    true/false (default: true) — infer column types
                         from the buffered records and CREATE TABLE IF NOT
                         EXISTS, with a unique constraint on merge_keys
                         when mode=merge
    batch_size           rows per batch insert (default: 1000)
    connect_retries      connection attempts before giving up (default: 3)
    connect_retry_backoff_seconds  base backoff between attempts,
                         doubled each retry (default: 2)

Interface: write(record)/flush() — identical contract to CloudWriter and
DataWarehouseWriter, for the same reason: LambdaArchitecture and
MedallionArchitecture call every configured writer this way regardless of
type.

Same caveat as the warehouse writer for mode=append under Lambda/Medallion
specifically: those architectures flush() once per layer (batch, speed,
serving — or bronze, silver, gold), and serving/gold overlaps heavily with
what batch/silver already wrote, so append double-loads those rows into
one table. Prefer mode=overwrite or mode=merge there; append is safe when
only one layer has a writer attached, or for architectures that flush once.
"""

import logging
import time
import uuid
from datetime import date, datetime, time as time_
from decimal import Decimal
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
from sqlalchemy.engine import URL

log = logging.getLogger(__name__)

_DEFAULT_PORTS = {"postgres": 5432, "mysql": 3306, "oracle": 1521, "sqlserver": 1433}
_DRIVERNAMES = {
    "postgres": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
    "oracle": "oracle+cx_oracle",
    "sqlserver": "mssql+pyodbc",
}


class DatabaseWriter:
    """SQLAlchemy-based writer for Postgres/MySQL/Oracle/SQL Server. See module docstring."""

    def __init__(self, config: dict):
        self.config = config
        self.db_type = config.get("db_type", "postgres").lower()
        if self.db_type == "mssql":
            self.db_type = "sqlserver"
        if self.db_type not in _DEFAULT_PORTS:
            raise ValueError(
                f"DatabaseWriter: unsupported db_type '{self.db_type}'. "
                f"Supported: postgres, mysql, oracle, sqlserver"
            )

        self.host = config.get("host")
        self.port = int(config.get("port", _DEFAULT_PORTS[self.db_type]))
        self.database = config.get("database")
        self.username = config.get("username")
        self.password = config.get("password")
        self.secret_arn = config.get("secret_arn")
        self.region = config.get("region", "us-east-1")

        if not self.host:
            raise ValueError("DatabaseWriter: --writer_host is required")
        if not self.database:
            raise ValueError("DatabaseWriter: --writer_database is required")
        if not self.secret_arn and not (self.username and self.password):
            raise ValueError(
                "DatabaseWriter: --writer_username and --writer_password are required "
                "(or --writer_secret_arn as an alternative)"
            )

        self.schema = config.get("schema")
        self.table_name = config.get("table")
        if not self.table_name:
            raise ValueError("DatabaseWriter: --writer_table is required")

        self.mode = config.get("mode", "append").lower()
        if self.mode not in ("append", "overwrite", "merge"):
            raise ValueError(
                f"Unsupported mode for DatabaseWriter: '{self.mode}'. "
                f"Supported: append, overwrite, merge"
            )
        raw_merge_keys = config.get("merge_keys", [])
        self.merge_keys = (
            [c.strip() for c in raw_merge_keys.split(",") if c.strip()]
            if isinstance(raw_merge_keys, str) else list(raw_merge_keys or [])
        )
        if self.mode == "merge" and not self.merge_keys:
            raise ValueError(
                "DatabaseWriter: --writer_merge_keys is required when "
                "--writer_mode=merge, e.g. --writer_merge_keys id"
            )

        self.auto_create_table = str(config.get("auto_create_table", "true")).lower() == "true"
        self.batch_size = int(config.get("batch_size", 1000))
        self.connect_retries = int(config.get("connect_retries", 3))
        self.connect_retry_backoff_seconds = float(config.get("connect_retry_backoff_seconds", 2))

        # Not used for routing (a database table has no concept of a path
        # prefix) — present only so the architecture layers' generic
        # `writer.prefix = f"{original_prefix}{layer_name}/"` reassignment
        # (done identically for every writer) is a harmless no-op here.
        self.prefix = ""

        self._buffer: List[Dict[str, Any]] = []
        self._engine = None

        log.info(
            "DatabaseWriter: initialized — db_type=%s, host=%s, database=%s, "
            "table=%s, mode=%s, auto_create_table=%s",
            self.db_type, self.host, self.database, self.table_name,
            self.mode, self.auto_create_table,
        )

    # ── Public API (write(record)/flush() — same contract as CloudWriter) ──

    def write(self, record: Dict[str, Any]) -> None:
        self._buffer.append(record)

    def flush(self, table_name: str = None) -> None:
        if not self._buffer:
            log.warning("DatabaseWriter: flush() called with no buffered records")
            return

        records = self._buffer
        self._buffer = []
        table = table_name or self.table_name

        log.info("DatabaseWriter: writing %d record(s) to %s (mode=%s)",
                 len(records), table, self.mode)

        engine = self._get_engine()
        try:
            with self._connect_with_retry(engine) as conn:
                table_obj = self._ensure_table(conn, table, records)

                if self.mode == "append":
                    self._bulk_insert(conn, table_obj, records)
                elif self.mode == "overwrite":
                    conn.execute(table_obj.delete())
                    self._bulk_insert(conn, table_obj, records)
                else:  # merge
                    self._upsert(conn, table_obj, records)

                conn.commit()

            log.info("DatabaseWriter: successfully wrote %d record(s) to %s",
                     len(records), table)
        except sa.exc.OperationalError as exc:
            raise ConnectionError(
                f"DatabaseWriter: could not reach {self.db_type} database at "
                f"{self.host}:{self.port} after {self.connect_retries} attempt(s). "
                f"A Glue Python-shell job has no VPC Connection by default — if this "
                f"database is in a private subnet, check that the Glue job has a VPC "
                f"Connection configured and that the database's security group allows "
                f"inbound access from it. Original error: {exc}"
            ) from exc

    # ── Connection ───────────────────────────────────────────────────────────

    def _resolve_credentials(self):
        if not self.secret_arn:
            return self.username, self.password, self.host, self.port, self.database
        import boto3
        import json as _json
        client = boto3.client("secretsmanager", region_name=self.region)
        secret = _json.loads(client.get_secret_value(SecretId=self.secret_arn)["SecretString"])
        return (
            secret.get("username", self.username),
            secret.get("password"),
            secret.get("host", self.host),
            int(secret.get("port", self.port)),
            secret.get("dbname", self.database),
        )

    def _get_engine(self):
        if self._engine is not None:
            return self._engine

        username, password, host, port, database = self._resolve_credentials()
        query = {}
        if self.db_type == "oracle":
            query["service_name"] = database
        if self.db_type == "sqlserver":
            query["driver"] = "ODBC Driver 17 for SQL Server"

        url = URL.create(
            drivername=_DRIVERNAMES[self.db_type],
            username=username,
            password=password,
            host=host,
            port=port,
            database=None if self.db_type == "oracle" else database,
            query=query,
        )
        self._engine = sa.create_engine(url, pool_pre_ping=True)
        return self._engine

    def _connect_with_retry(self, engine):
        last_exc = None
        for attempt in range(1, self.connect_retries + 1):
            try:
                return engine.connect()
            except sa.exc.OperationalError as exc:
                last_exc = exc
                if attempt < self.connect_retries:
                    backoff = self.connect_retry_backoff_seconds * (2 ** (attempt - 1))
                    log.warning(
                        "DatabaseWriter: connection attempt %d/%d failed (%s) — "
                        "retrying in %.1fs",
                        attempt, self.connect_retries, exc, backoff,
                    )
                    time.sleep(backoff)
        raise last_exc

    # ── Table management ─────────────────────────────────────────────────────

    def _ensure_table(self, conn, table: str, sample_records: List[Dict[str, Any]]) -> sa.Table:
        metadata = sa.MetaData(schema=self.schema)
        inspector = sa.inspect(conn)

        if inspector.has_table(table, schema=self.schema):
            table_obj = sa.Table(table, metadata, autoload_with=conn)
            if self.auto_create_table:
                # The table already existing doesn't mean its columns match
                # this batch — e.g. one shared table flushed from multiple
                # sources, or an upstream API that added a field. Widen the
                # table rather than letting _bulk_insert's "Unconsumed
                # column names" surface as the first sign of drift.
                table_obj = self._reconcile_columns(conn, table_obj, sample_records)
            return table_obj

        if not self.auto_create_table:
            raise ValueError(
                f"DatabaseWriter: table '{table}' does not exist and "
                f"--writer_auto_create_table is false. Create it manually or "
                f"enable auto_create_table."
            )

        # Union of every record's keys, not just the first record's — a key
        # that's only present (or only non-None) on a later record in the
        # batch must still get a column, or its insert fails the same way.
        all_keys = self._union_keys(sample_records)
        columns = [
            sa.Column(name, self._infer_column_type(name, sample_records),
                      primary_key=(self.mode == "merge" and name in self.merge_keys))
            for name in all_keys
        ]
        table_obj = sa.Table(table, metadata, *columns)
        table_obj.create(conn, checkfirst=True)
        log.info("DatabaseWriter: auto-created table %s (%d column(s)%s)",
                 table, len(columns),
                 f", primary key on {self.merge_keys}" if self.mode == "merge" else "")
        return table_obj

    def _reconcile_columns(self, conn, table_obj: sa.Table,
                            sample_records: List[Dict[str, Any]]) -> sa.Table:
        existing = {c.name for c in table_obj.columns}
        missing = [name for name in self._union_keys(sample_records) if name not in existing]
        if not missing:
            return table_obj

        qualified = self._qualified_name(table_obj.name)
        for name in missing:
            ddl_type = self._infer_column_type(name, sample_records)().compile(dialect=conn.dialect)
            if self.db_type == "oracle":
                stmt = f"ALTER TABLE {qualified} ADD ({name} {ddl_type})"
            elif self.db_type == "sqlserver":
                stmt = f"ALTER TABLE {qualified} ADD {name} {ddl_type}"
            else:
                stmt = f"ALTER TABLE {qualified} ADD COLUMN {name} {ddl_type}"
            conn.execute(sa.text(stmt))
        conn.commit()
        log.info("DatabaseWriter: widened table %s with %d new column(s): %s",
                 table_obj.name, len(missing), missing)

        # Re-reflect so callers (insert/upsert) see the new columns.
        metadata = sa.MetaData(schema=self.schema)
        return sa.Table(table_obj.name, metadata, autoload_with=conn)

    @staticmethod
    def _union_keys(records: List[Dict[str, Any]]) -> List[str]:
        keys: Dict[str, bool] = {}
        for record in records:
            for key in record.keys():
                keys.setdefault(key, True)
        return list(keys.keys())

    def _infer_column_type(self, name: str, records: List[Dict[str, Any]]):
        value = None
        for record in records:
            candidate = record.get(name)
            if candidate is not None:
                value = candidate
                break

        if isinstance(value, bool):
            return sa.Boolean
        if isinstance(value, int):
            return sa.BigInteger
        if isinstance(value, Decimal):
            return sa.Numeric
        if isinstance(value, float):
            return sa.Float
        if isinstance(value, datetime):
            return sa.DateTime
        if isinstance(value, date):
            return sa.Date
        if isinstance(value, time_):
            return sa.Time
        if isinstance(value, uuid.UUID):
            return sa.String
        if isinstance(value, (bytes, bytearray)):
            return sa.LargeBinary
        if isinstance(value, (dict, list)):
            return sa.JSON
        return sa.Text

    # ── Append / overwrite ───────────────────────────────────────────────────

    def _bulk_insert(self, conn, table_obj: sa.Table, records: List[Dict[str, Any]]) -> None:
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            conn.execute(sa.insert(table_obj), batch)
            log.info("DatabaseWriter: inserted batch of %d record(s)", len(batch))

    # ── Merge / upsert ───────────────────────────────────────────────────────

    def _upsert(self, conn, table_obj: sa.Table, records: List[Dict[str, Any]]) -> None:
        if self.db_type == "postgres":
            self._upsert_postgres(conn, table_obj, records)
        elif self.db_type == "mysql":
            self._upsert_mysql(conn, table_obj, records)
        else:
            self._upsert_via_staging_merge(conn, table_obj, records)

    def _upsert_postgres(self, conn, table_obj: sa.Table, records: List[Dict[str, Any]]) -> None:
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        update_cols = [c.name for c in table_obj.columns if c.name not in self.merge_keys]
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            stmt = pg_insert(table_obj).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=self.merge_keys,
                set_={col: stmt.excluded[col] for col in update_cols},
            )
            conn.execute(stmt)
            log.info("DatabaseWriter: upserted batch of %d record(s)", len(batch))

    def _upsert_mysql(self, conn, table_obj: sa.Table, records: List[Dict[str, Any]]) -> None:
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        update_cols = [c.name for c in table_obj.columns if c.name not in self.merge_keys]
        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            stmt = mysql_insert(table_obj).values(batch)
            stmt = stmt.on_duplicate_key_update(
                **{col: stmt.inserted[col] for col in update_cols}
            )
            conn.execute(stmt)
            log.info("DatabaseWriter: upserted batch of %d record(s)", len(batch))

    def _upsert_via_staging_merge(self, conn, table_obj: sa.Table,
                                   records: List[Dict[str, Any]]) -> None:
        """
        Oracle / SQL Server: neither supports a bulk upsert statement shape,
        so stage into a side table, then run one real, explicit-column
        MERGE — the same stage-then-merge shape as the Redshift writer,
        never a MERGE per row.
        """
        staging_name = f"{table_obj.name}_upsert_staging"
        metadata = sa.MetaData(schema=self.schema)
        staging_cols = [sa.Column(c.name, c.type) for c in table_obj.columns]
        staging_obj = sa.Table(staging_name, metadata, *staging_cols)
        staging_obj.create(conn, checkfirst=True)

        conn.execute(staging_obj.delete())
        self._bulk_insert(conn, staging_obj, records)

        all_cols = [c.name for c in table_obj.columns]
        update_cols = [c for c in all_cols if c not in self.merge_keys]
        target = self._qualified_name(table_obj.name)
        staging = self._qualified_name(staging_name)
        condition = " AND ".join(f"t.{k} = s.{k}" for k in self.merge_keys)
        update_clause = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join(f"s.{c}" for c in all_cols)

        merge_sql = (
            f"MERGE INTO {target} t USING {staging} s ON ({condition}) "
            f"WHEN MATCHED THEN UPDATE SET {update_clause} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        if self.db_type == "sqlserver":
            merge_sql += ";"

        conn.execute(sa.text(merge_sql))
        conn.execute(staging_obj.delete())
        log.info("DatabaseWriter: merged %d record(s) into %s via staging table %s",
                 len(records), target, staging_name)

    def _qualified_name(self, table_name: str) -> str:
        return f"{self.schema}.{table_name}" if self.schema else table_name
