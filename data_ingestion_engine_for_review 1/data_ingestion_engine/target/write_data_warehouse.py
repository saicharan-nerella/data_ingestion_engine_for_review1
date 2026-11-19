"""
target/write_data_warehouse.py
───────────────────────────────
Data Warehouse Writer. Redshift is fully implemented; Snowflake, BigQuery,
and Synapse raise a clear NotImplementedError instead of silently pretending
to work (the previous version of this file connected to all three, logged
"write prepared", and returned — the formatted records were never actually
referenced again. No COPY, no INSERT, no staging upload. It looked
functional and moved zero rows.)

How this actually loads data (true for every real warehouse, not just
Redshift): never row-by-row INSERT — that's slow and expensive on an MPP
columnar warehouse. Instead:
  1. Stage the formatted records as a file in cloud storage.
  2. Run the warehouse's own native bulk-load command against that staged
     file (Redshift: COPY ... FROM 's3://...' IAM_ROLE '...'; the same
     shape applies to Snowflake's COPY INTO from a stage, BigQuery's
     load_table_from_uri from GCS, Synapse's COPY INTO from Azure Blob —
     only the exact SQL/API differs per product).

Redshift specifically talks to the cluster via the Redshift Data API
(boto3 client "redshift-data"), not a direct psycopg2/JDBC connection —
deliberately. A Glue Python-shell job has no VPC Connection by default, so
a raw TCP connection to a Redshift cluster in a private subnet hits the
exact same "security group has no inbound rule reachable from here" wall
already documented for structured_readers/batch_database_read.py. The
Data API is a plain HTTPS AWS API call — the same mechanism that already
works for S3/SNS in this codebase without any VPC setup — so it sidesteps
that whole problem, and authenticates via IAM (a database user backed by
the job's own IAM role, or a Secrets Manager secret) instead of a shipped
password.

Config (all via --writer_<x> Glue parameters — see
core/pipeline_builder.py's generic writer_* passthrough):
    warehouse_type       redshift  (required — anything else raises NotImplementedError)
    cluster_identifier   provisioned Redshift cluster id
    workgroup_name       Redshift Serverless workgroup (alternative to cluster_identifier)
    database             target database name (required)
    db_user              IAM database user — Data API uses temporary IAM
                          credentials for this user (required, unless secret_arn is set)
    secret_arn           Secrets Manager secret ARN (alternative to db_user)
    schema               target schema (default: public)
    table                target table name (or pass table_name to flush())
    mode                 append | overwrite | merge (default: append)
    merge_keys           comma-separated column(s) identifying a unique row —
                          required when mode=merge, e.g. "id" or "id,tenant_id"
    staging_bucket       S3 bucket COPY loads from (required)
    staging_prefix       S3 prefix under staging_bucket (default: "warehouse_staging/")
    staging_format       csv | json | parquet (default: csv)
    iam_role             IAM role ARN Redshift itself assumes to read
                          staging_bucket — COPY ... IAM_ROLE '...' (required)
    region               AWS region (default: us-east-1)
    auto_create_table    true/false (default: true) — CREATE TABLE IF NOT
                          EXISTS on first write to a table, using the union
                          of keys across the batch, every column VARCHAR(MAX)
                          (the staging file already stringifies every value
                          before COPY, so this is never a type mismatch —
                          just a cast you may want to do downstream). On
                          every later flush, widens an existing table with
                          ALTER TABLE ADD COLUMN for any new key the batch
                          has that the table doesn't — e.g. an architecture
                          that tags records with its own metadata fields
                          (Kappa's _kappa_layer/_kappa_source/etc.).
                          Deliberately NOT trying to guess a good DISTKEY/
                          SORTKEY — Redshift's own DISTSTYLE AUTO adapts
                          over time, and a wrong guess is worse than no
                          guess; tune those by hand once a table has real
                          query patterns, same as you would for a table
                          anyone created manually.

Interface: write(record)/flush() — identical contract to CloudWriter, one
record buffered at a time, one bulk load issued on flush(). Required so
this is a drop-in choice inside LambdaArchitecture/MedallionArchitecture,
which call every configured writer the same way regardless of type.

Caveat for mode=append under Lambda architecture specifically: Lambda
calls flush() once per layer (batch, speed, serving), and serving is a
merge of batch ∪ speed — so append mode there double-loads whatever rows
both layers share. Medallion has the same shape (bronze/silver/gold each
flush). This isn't new to this writer — CloudWriter sees the same calls,
it just lands each layer under its own S3 prefix so duplication is
invisible there. For a single warehouse table, prefer mode=overwrite or
mode=merge with Lambda/Medallion; append is safe when only one layer
actually has a writer attached, or for architectures that flush once
(e.g. Kappa, or a single-pass batch-only pipeline).
"""

import csv
import io
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 1
_STATEMENT_TIMEOUT_SECONDS = 300


class DataWarehouseWriter:
    """Redshift writer via the Redshift Data API. See module docstring."""

    def __init__(self, config: dict):
        self.config = config
        self.warehouse_type = config.get("warehouse_type", "redshift").lower()

        if self.warehouse_type != "redshift":
            raise NotImplementedError(
                f"DataWarehouseWriter: warehouse_type='{self.warehouse_type}' is not "
                f"implemented yet — only 'redshift' is built today. To add another "
                f"warehouse, follow the same pattern as _copy_into_redshift()/"
                f"_upsert_redshift() below: stage to cloud storage, then run that "
                f"warehouse's own native bulk-load command (Snowflake: COPY INTO from "
                f"a stage; BigQuery: load_table_from_uri; Synapse: COPY INTO from Azure Blob)."
            )

        self.cluster_identifier = config.get("cluster_identifier")
        self.workgroup_name = config.get("workgroup_name")
        if not self.cluster_identifier and not self.workgroup_name:
            raise ValueError(
                "DataWarehouseWriter: --writer_cluster_identifier (provisioned Redshift) "
                "or --writer_workgroup_name (Redshift Serverless) is required"
            )

        self.database = config.get("database")
        self.db_user = config.get("db_user")
        self.secret_arn = config.get("secret_arn")
        if not self.database:
            raise ValueError("DataWarehouseWriter: --writer_database is required")
        if not self.db_user and not self.secret_arn:
            raise ValueError(
                "DataWarehouseWriter: --writer_db_user (IAM database user) or "
                "--writer_secret_arn (Secrets Manager secret) is required"
            )

        self.schema = config.get("schema", "public")
        self.table_name = config.get("table")
        self.region = config.get("region", "us-east-1")

        self.mode = config.get("mode", "append").lower()
        if self.mode not in ("append", "overwrite", "merge"):
            raise ValueError(
                f"Unsupported mode for DataWarehouseWriter: '{self.mode}'. "
                f"Supported: append, overwrite, merge"
            )
        raw_merge_keys = config.get("merge_keys", [])
        self.merge_keys = (
            [c.strip() for c in raw_merge_keys.split(",") if c.strip()]
            if isinstance(raw_merge_keys, str) else list(raw_merge_keys or [])
        )
        if self.mode == "merge" and not self.merge_keys:
            raise ValueError(
                "DataWarehouseWriter: --writer_merge_keys is required when "
                "--writer_mode=merge, e.g. --writer_merge_keys id"
            )

        # Not used for routing (a warehouse table has no concept of a path
        # prefix) — present only so the architecture layers' generic
        # `writer.prefix = f"{original_prefix}{layer_name}/"` reassignment
        # (done identically for every writer, CloudWriter included) is a
        # harmless no-op here instead of needing a writer-type special case.
        self.prefix = ""

        # Same incremental contract as CloudWriter: write(record) buffers,
        # flush() does the real work. Required so this writer is a drop-in
        # replacement inside LambdaArchitecture/MedallionArchitecture, which
        # call every configured writer exactly that way regardless of type.
        self._buffer: List[Dict[str, Any]] = []

        self.staging_bucket = config.get("staging_bucket")
        self.staging_prefix = config.get("staging_prefix", "warehouse_staging/")
        self.staging_format = config.get("staging_format", "csv").lower()
        self.iam_role = config.get("iam_role")

        if not self.staging_bucket:
            raise ValueError(
                "DataWarehouseWriter: --writer_staging_bucket is required "
                "(Redshift loads via COPY from a staged S3 file, never row-by-row INSERT)"
            )
        if not self.iam_role:
            raise ValueError(
                "DataWarehouseWriter: --writer_iam_role is required — the IAM role ARN "
                "Redshift itself assumes to read --writer_staging_bucket (COPY ... IAM_ROLE '...')"
            )
        if self.staging_format not in ("csv", "json", "parquet"):
            raise ValueError(
                f"DataWarehouseWriter: unsupported staging_format '{self.staging_format}'. "
                f"Supported: csv, json, parquet"
            )

        self.auto_create_table = str(config.get("auto_create_table", "true")).lower() == "true"

        log.info(
            "DataWarehouseWriter: initialized — warehouse=redshift, cluster=%s, "
            "workgroup=%s, database=%s, schema=%s, staging=s3://%s/%s",
            self.cluster_identifier, self.workgroup_name, self.database, self.schema,
            self.staging_bucket, self.staging_prefix,
        )

    # ── Public API ───────────────────────────────────────────────────────────
    # Same incremental contract as CloudWriter — write(record) buffers one
    # record at a time, flush() does the actual load. This is also the
    # right shape for Redshift specifically: buffer, then issue exactly one
    # COPY per flush, never a COPY per record.

    def write(self, record: Dict[str, Any]) -> None:
        self._buffer.append(record)

    def flush(self, table_name: str = None) -> None:
        """
        Load every buffered record into Redshift, then clear the buffer.

        Args:
            table_name: overrides the configured --writer_table
        """
        if not self._buffer:
            log.warning("DataWarehouseWriter: flush() called with no buffered records")
            return

        records = self._buffer
        self._buffer = []

        table = table_name or self.table_name
        if not table:
            raise ValueError(
                "DataWarehouseWriter: table_name is required (pass to flush(), "
                "or set --writer_table)"
            )
        target_table = f"{self.schema}.{table}"

        log.info("DataWarehouseWriter: writing %d record(s) to %s (mode=%s)",
                 len(records), target_table, self.mode)

        if self.auto_create_table:
            self._ensure_table(table, records)

        staging_key = self._stage_records(records, table)
        try:
            if self.mode == "append":
                self._copy_into_redshift(staging_key, target_table)
            elif self.mode == "overwrite":
                self._execute_sql(f"TRUNCATE TABLE {target_table}; "
                                   f"{self._copy_statement(staging_key, target_table)}")
            else:  # merge
                self._upsert_redshift(staging_key, target_table, self.merge_keys)

            log.info("DataWarehouseWriter: successfully wrote %d record(s) to %s",
                     len(records), target_table)
        finally:
            # The staged file is a transient intermediate, not an output —
            # clean it up regardless of success or failure so staging runs
            # don't accumulate in S3 across every pipeline run.
            self._cleanup_staging_file(staging_key)

    # ── Staging (format + upload to S3) ─────────────────────────────────────

    def _stage_records(self, records: List[Dict[str, Any]], table: str) -> str:
        """
        Format records and upload to a predictable, known S3 key under the
        staging prefix — predictable on purpose, since the very next step
        (COPY FROM) needs to reference this exact key.
        """
        import boto3

        if self.staging_format == "csv":
            data = self._format_csv(records)
        elif self.staging_format == "json":
            data = self._format_json(records)
        else:
            data = self._format_parquet(records)

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        key = f"{self.staging_prefix}{table}/{timestamp}.{self.staging_format}"

        s3 = boto3.client("s3", region_name=self.region)
        s3.put_object(Bucket=self.staging_bucket, Key=key, Body=data)
        log.info("DataWarehouseWriter: staged %d record(s) to s3://%s/%s",
                 len(records), self.staging_bucket, key)
        return key

    def _cleanup_staging_file(self, key: str) -> None:
        try:
            import boto3
            boto3.client("s3", region_name=self.region).delete_object(
                Bucket=self.staging_bucket, Key=key
            )
        except Exception as exc:
            # The load itself already succeeded or failed by this point —
            # a cleanup failure shouldn't mask that outcome, just get logged.
            log.warning("DataWarehouseWriter: could not delete staging file s3://%s/%s — %s",
                        self.staging_bucket, key, exc)

    def _format_csv(self, records: List[Dict[str, Any]]) -> bytes:
        fieldnames = list(records[0].keys())
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow({k: self._stringify(v) for k, v in record.items()})
        return buffer.getvalue().encode("utf-8")

    def _format_json(self, records: List[Dict[str, Any]]) -> bytes:
        # Redshift's "FORMAT AS JSON 'auto'" expects one JSON object per
        # line, object keys matching column names exactly.
        lines = [json.dumps({k: self._stringify(v) for k, v in r.items()}) for r in records]
        return "\n".join(lines).encode("utf-8")

    def _format_parquet(self, records: List[Dict[str, Any]]) -> bytes:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow is required for staging_format=parquet")
        table = pa.Table.from_pylist(records)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        return buffer.getvalue()

    def _stringify(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        return value

    # ── Table management (auto-create / widen on drift) ─────────────────────

    def _ensure_table(self, table: str, records: List[Dict[str, Any]]) -> None:
        target_table = f"{self.schema}.{table}"
        columns = self._union_keys(records)
        existing = self._existing_columns(table)

        if existing is None:
            col_defs = ", ".join(f"{name} VARCHAR(MAX)" for name in columns)
            self._execute_sql(f"CREATE TABLE IF NOT EXISTS {target_table} ({col_defs})")
            log.info("DataWarehouseWriter: auto-created table %s (%d column(s))",
                     target_table, len(columns))
            return

        missing = [name for name in columns if name.lower() not in existing]
        if not missing:
            return
        for name in missing:
            self._execute_sql(f"ALTER TABLE {target_table} ADD COLUMN {name} VARCHAR(MAX)")
        log.info("DataWarehouseWriter: widened table %s with %d new column(s): %s",
                 target_table, len(missing), missing)

    def _existing_columns(self, table: str) -> Optional[List[str]]:
        """None if the table doesn't exist yet; otherwise its real column
        names (always lowercase — Redshift, like Postgres, folds unquoted
        identifiers to lowercase, and these were created unquoted)."""
        sql = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{self.schema}' AND table_name = '{table}'"
        )
        client = self._get_redshift_data_client()
        statement_id = self._submit_and_wait(client, sql)
        result = client.get_statement_result(Id=statement_id)
        if not result["Records"]:
            return None
        return [row[0]["stringValue"] for row in result["Records"]]

    @staticmethod
    def _union_keys(records: List[Dict[str, Any]]) -> List[str]:
        keys: Dict[str, bool] = {}
        for record in records:
            for key in record.keys():
                keys.setdefault(key, True)
        return list(keys.keys())

    # ── Redshift Data API ────────────────────────────────────────────────────

    def _get_redshift_data_client(self):
        import boto3
        return boto3.client("redshift-data", region_name=self.region)

    def _copy_format_clause(self) -> str:
        return {
            "csv": "FORMAT AS CSV IGNOREHEADER 1",
            "json": "FORMAT AS JSON 'auto'",
            "parquet": "FORMAT AS PARQUET",
        }[self.staging_format]

    def _copy_statement(self, staging_key: str, target_table: str) -> str:
        s3_path = f"s3://{self.staging_bucket}/{staging_key}"
        return (
            f"COPY {target_table} FROM '{s3_path}' "
            f"IAM_ROLE '{self.iam_role}' {self._copy_format_clause()};"
        )

    def _copy_into_redshift(self, staging_key: str, target_table: str) -> None:
        self._execute_sql(self._copy_statement(staging_key, target_table))

    def _upsert_redshift(self, staging_key: str, target_table: str,
                          merge_keys: List[str]) -> None:
        """
        Upsert via a temp staging table: COPY into it, delete any target
        rows that match on merge_keys, then insert everything staged.

        Everything here runs as ONE execute_statement call (one multi-
        statement SQL string), not several separate calls — a CREATE TEMP
        TABLE in one Data API call would not be visible in a later, separate
        call, since temp tables are session-scoped in Redshift/Postgres and
        the Data API does not guarantee separate execute_statement calls
        share a session. One call = one session = the temp table's whole
        lifecycle contained within it.
        """
        bare_table = target_table.split(".")[-1]
        staging_table = f"{bare_table}_upsert_staging"
        s3_path = f"s3://{self.staging_bucket}/{staging_key}"
        condition = " AND ".join(f"{target_table}.{k} = {staging_table}.{k}" for k in merge_keys)

        sql = f"""
BEGIN;
CREATE TEMP TABLE {staging_table} (LIKE {target_table});
COPY {staging_table} FROM '{s3_path}' IAM_ROLE '{self.iam_role}' {self._copy_format_clause()};
DELETE FROM {target_table} USING {staging_table} WHERE {condition};
INSERT INTO {target_table} SELECT * FROM {staging_table};
DROP TABLE {staging_table};
COMMIT;
"""
        self._execute_sql(sql)

    def _execute_sql(self, sql: str) -> None:
        """Run one (possibly multi-statement) SQL string via the Redshift
        Data API and block until it finishes, raising on failure/timeout."""
        client = self._get_redshift_data_client()
        self._submit_and_wait(client, sql)

    def _submit_and_wait(self, client, sql: str) -> str:
        """Submit `sql`, block until FINISHED, return the statement id (so
        a caller needing the result — _existing_columns' SELECT — can fetch
        it; callers that don't need a result, like _execute_sql, just
        discard it)."""
        kwargs: Dict[str, Any] = {"Database": self.database, "Sql": sql}
        if self.cluster_identifier:
            kwargs["ClusterIdentifier"] = self.cluster_identifier
        else:
            kwargs["WorkgroupName"] = self.workgroup_name
        if self.secret_arn:
            kwargs["SecretArn"] = self.secret_arn
        else:
            kwargs["DbUser"] = self.db_user

        response = client.execute_statement(**kwargs)
        statement_id = response["Id"]
        log.info("DataWarehouseWriter: submitted Redshift statement %s", statement_id)

        deadline = time.monotonic() + _STATEMENT_TIMEOUT_SECONDS
        while True:
            status = client.describe_statement(Id=statement_id)
            state = status["Status"]

            if state == "FINISHED":
                log.info("DataWarehouseWriter: Redshift statement %s finished", statement_id)
                return statement_id
            if state in ("FAILED", "ABORTED"):
                raise RuntimeError(
                    f"Redshift statement {state.lower()} ({statement_id}): "
                    f"{status.get('Error', 'no error detail returned')}\nSQL:\n{sql}"
                )
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Redshift statement {statement_id} did not finish within "
                    f"{_STATEMENT_TIMEOUT_SECONDS}s"
                )
            time.sleep(_POLL_INTERVAL_SECONDS)
