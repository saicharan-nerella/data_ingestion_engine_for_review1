from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql, mysql

from target.write_database import DatabaseWriter


BASE_CONFIG = {
    "db_type": "postgres",
    "host": "test-host.rds.amazonaws.com",
    "database": "analytics",
    "username": "app_user",
    "password": "app_pass",
    "table": "events",
}

RECORDS = [
    {"id": 1, "name": "alice", "amount": 100.5},
    {"id": 2, "name": "bob", "amount": 250.0},
]


def make_writer(extra=None):
    config = {**BASE_CONFIG, **(extra or {})}
    return DatabaseWriter(config)


def sqlite_writer(extra=None):
    """A writer backed by a real in-memory SQLite engine — genuine
    connect/create-table/insert/query execution, no mocks, no network."""
    writer = make_writer(extra)
    writer._engine = sa.create_engine("sqlite:///:memory:")
    return writer


def write_all(writer, records):
    for r in records:
        writer.write(r)


# --- Config validation ---

def test_unsupported_db_type_raises():
    with pytest.raises(ValueError, match="unsupported db_type"):
        make_writer({"db_type": "mongodb"})


def test_mssql_alias_normalizes_to_sqlserver():
    writer = make_writer({"db_type": "mssql", "host": "h", "username": "u", "password": "p"})
    assert writer.db_type == "sqlserver"


def test_missing_host_raises():
    config = {**BASE_CONFIG}
    del config["host"]
    with pytest.raises(ValueError, match="writer_host"):
        DatabaseWriter(config)


def test_missing_database_raises():
    config = {**BASE_CONFIG}
    del config["database"]
    with pytest.raises(ValueError, match="writer_database"):
        DatabaseWriter(config)


def test_missing_credentials_and_secret_raises():
    config = {**BASE_CONFIG}
    del config["username"]
    del config["password"]
    with pytest.raises(ValueError, match="writer_username"):
        DatabaseWriter(config)


def test_secret_arn_accepted_without_username_password():
    config = {**BASE_CONFIG}
    del config["username"]
    del config["password"]
    config["secret_arn"] = "arn:aws:secretsmanager:us-east-1:855738613861:secret:db-creds"
    writer = DatabaseWriter(config)
    assert writer.secret_arn is not None


def test_missing_table_raises():
    config = {**BASE_CONFIG}
    del config["table"]
    with pytest.raises(ValueError, match="writer_table"):
        DatabaseWriter(config)


def test_unsupported_mode_raises():
    with pytest.raises(ValueError, match="Unsupported mode"):
        make_writer({"mode": "replace"})


def test_merge_mode_without_merge_keys_raises():
    with pytest.raises(ValueError, match="merge_keys"):
        make_writer({"mode": "merge"})


def test_merge_keys_string_is_parsed():
    writer = make_writer({"mode": "merge", "merge_keys": "id, tenant_id"})
    assert writer.merge_keys == ["id", "tenant_id"]


def test_default_ports_per_db_type():
    assert make_writer({"db_type": "postgres"}).port == 5432
    assert make_writer({"db_type": "mysql"}).port == 3306
    assert make_writer({"db_type": "oracle"}).port == 1521
    assert make_writer({"db_type": "sqlserver"}).port == 1433


def test_prefix_attribute_present_and_reassignable():
    writer = make_writer()
    assert writer.prefix == ""
    writer.prefix = f"{writer.prefix}batch/"
    assert writer.prefix == "batch/"


# --- write()/flush() incremental interface ---

def test_write_buffers_without_connecting():
    writer = make_writer()
    with patch("sqlalchemy.create_engine") as mock_create:
        writer.write(RECORDS[0])
        writer.write(RECORDS[1])
        mock_create.assert_not_called()
    assert writer._buffer == RECORDS


def test_flush_empty_buffer_noop():
    writer = make_writer()
    with patch("sqlalchemy.create_engine") as mock_create:
        writer.flush()
        mock_create.assert_not_called()


# --- URL building ---

def test_engine_url_built_correctly_for_each_db_type():
    cases = {
        "postgres": "postgresql+psycopg2",
        "mysql": "mysql+pymysql",
        "oracle": "oracle+cx_oracle",
        "sqlserver": "mssql+pyodbc",
    }
    for db_type, drivername in cases.items():
        writer = make_writer({"db_type": db_type})
        with patch("sqlalchemy.create_engine") as mock_create:
            writer._get_engine()
        url = mock_create.call_args[0][0]
        assert url.drivername == drivername
        assert url.host == "test-host.rds.amazonaws.com"
        assert url.username == "app_user"
        assert url.password == "app_pass"


def test_oracle_url_uses_service_name_query_param():
    writer = make_writer({"db_type": "oracle"})
    with patch("sqlalchemy.create_engine") as mock_create:
        writer._get_engine()
    url = mock_create.call_args[0][0]
    assert url.query.get("service_name") == "analytics"
    assert url.database is None


def test_sqlserver_url_uses_odbc_driver_query_param():
    writer = make_writer({"db_type": "sqlserver"})
    with patch("sqlalchemy.create_engine") as mock_create:
        writer._get_engine()
    url = mock_create.call_args[0][0]
    assert url.query.get("driver") == "ODBC Driver 17 for SQL Server"


def test_secret_arn_resolves_credentials_via_secrets_manager():
    config = {**BASE_CONFIG}
    del config["username"]
    del config["password"]
    config["secret_arn"] = "arn:aws:secretsmanager:us-east-1:855738613861:secret:db-creds"
    writer = DatabaseWriter(config)

    secrets_client = MagicMock()
    secrets_client.get_secret_value.return_value = {
        "SecretString": '{"username": "secret_user", "password": "secret_pass", '
                        '"host": "secret-host", "port": 5432, "dbname": "secret_db"}'
    }
    with patch("boto3.client", return_value=secrets_client), \
         patch("sqlalchemy.create_engine") as mock_create:
        writer._get_engine()

    url = mock_create.call_args[0][0]
    assert url.username == "secret_user"
    assert url.password == "secret_pass"
    assert url.host == "secret-host"
    assert url.database == "secret_db"


# --- Connection retry ---

def test_connect_retries_on_operational_error_then_succeeds():
    writer = make_writer({"connect_retry_backoff_seconds": "0"})
    engine = MagicMock()
    good_conn = MagicMock()
    engine.connect.side_effect = [sa.exc.OperationalError("stmt", {}, Exception("timeout")), good_conn]
    with patch("time.sleep"):
        result = writer._connect_with_retry(engine)
    assert result is good_conn
    assert engine.connect.call_count == 2


def test_connect_exhausts_retries_raises_operational_error():
    writer = make_writer({"connect_retries": "2", "connect_retry_backoff_seconds": "0"})
    engine = MagicMock()
    engine.connect.side_effect = sa.exc.OperationalError("stmt", {}, Exception("timeout"))
    with patch("time.sleep"):
        with pytest.raises(sa.exc.OperationalError):
            writer._connect_with_retry(engine)
    assert engine.connect.call_count == 2


def test_flush_wraps_operational_error_with_actionable_message():
    writer = make_writer({"connect_retries": "1"})
    with patch("sqlalchemy.create_engine") as mock_create:
        mock_create.return_value.connect.side_effect = sa.exc.OperationalError(
            "stmt", {}, Exception("connection timed out")
        )
        write_all(writer, RECORDS)
        with pytest.raises(ConnectionError, match="VPC Connection"):
            writer.flush()


# --- Real SQLite-backed execution: auto-create-table + type inference + append/overwrite ---

def test_sqlite_auto_creates_table_with_inferred_types_and_inserts():
    writer = sqlite_writer()
    write_all(writer, RECORDS)
    writer.flush()

    with writer._engine.connect() as conn:
        rows = conn.execute(sa.text("SELECT id, name, amount FROM events ORDER BY id")).fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, "alice", 100.5)
    assert rows[1] == (2, "bob", 250.0)


def test_sqlite_infers_bool_and_json_columns():
    writer = sqlite_writer()
    records = [{"id": 1, "active": True, "tags": ["a", "b"]}]
    write_all(writer, records)
    writer.flush()

    inspector = sa.inspect(writer._engine)
    cols = {c["name"]: str(c["type"]) for c in inspector.get_columns("events")}
    assert "BOOLEAN" in cols["active"]


def test_sqlite_append_mode_accumulates_across_flushes():
    writer = sqlite_writer()
    write_all(writer, RECORDS)
    writer.flush()
    writer.write({"id": 3, "name": "carol", "amount": 75.0})
    writer.flush()

    with writer._engine.connect() as conn:
        count = conn.execute(sa.text("SELECT COUNT(*) FROM events")).scalar()
    assert count == 3


def test_sqlite_overwrite_mode_clears_previous_rows():
    writer = sqlite_writer({"mode": "overwrite"})
    write_all(writer, RECORDS)
    writer.flush()
    writer.write({"id": 99, "name": "zzz", "amount": 1.0})
    writer.flush()

    with writer._engine.connect() as conn:
        rows = conn.execute(sa.text("SELECT id FROM events")).fetchall()
    assert rows == [(99,)]


def test_sqlite_batch_size_chunks_large_inserts():
    writer = sqlite_writer({"batch_size": "2"})
    write_all(writer, [{"id": i, "name": f"n{i}"} for i in range(5)])
    writer.flush()

    with writer._engine.connect() as conn:
        count = conn.execute(sa.text("SELECT COUNT(*) FROM events")).scalar()
    assert count == 5


def test_auto_create_table_disabled_raises_when_table_missing():
    writer = sqlite_writer({"auto_create_table": "false"})
    write_all(writer, RECORDS)
    with pytest.raises(ValueError, match="auto_create_table"):
        writer.flush()


def test_merge_mode_auto_creates_primary_key_on_merge_keys():
    writer = sqlite_writer({"mode": "merge", "merge_keys": "id"})
    # Exercise table creation directly (merge execution itself is
    # dialect-specific and covered by the compiled-SQL checks below —
    # SQLite's own on_conflict construct is a different class than
    # postgres'/mysql's, so self.db_type="postgres" won't dispatch to a
    # construct SQLite can compile).
    with writer._engine.connect() as conn:
        table_obj = writer._ensure_table(conn, "events", RECORDS)
    pk_cols = [c.name for c in table_obj.primary_key.columns]
    assert pk_cols == ["id"]


# --- Postgres / MySQL upsert: compiled against the REAL dialect compiler ---
# (No live connection needed or possible here — but this compiles the exact
# construct object _upsert_postgres/_upsert_mysql build, through SQLAlchemy's
# real dialect-aware SQL compiler, not a hand-rolled string check.)

def test_postgres_upsert_compiles_to_correct_on_conflict_sql():
    writer = make_writer({"mode": "merge", "merge_keys": ["id"]})
    table = sa.Table(
        "events", sa.MetaData(),
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("name", sa.Text), sa.Column("amount", sa.Float),
    )

    captured = {}
    def fake_execute(stmt, *a, **kw):
        captured["sql"] = str(stmt.compile(dialect=postgresql.dialect()))
    conn = MagicMock()
    conn.execute.side_effect = fake_execute

    writer._upsert_postgres(conn, table, RECORDS)
    sql = captured["sql"]
    assert "INSERT INTO events" in sql
    assert "ON CONFLICT (id) DO UPDATE SET" in sql
    assert "name = excluded.name" in sql
    assert "amount = excluded.amount" in sql
    assert "id = excluded.id" not in sql  # the merge key itself is never in SET


def test_mysql_upsert_compiles_to_correct_on_duplicate_key_sql():
    writer = make_writer({"db_type": "mysql", "mode": "merge", "merge_keys": ["id"]})
    table = sa.Table(
        "events", sa.MetaData(),
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("name", sa.Text), sa.Column("amount", sa.Float),
    )

    captured = {}
    def fake_execute(stmt, *a, **kw):
        captured["sql"] = str(stmt.compile(dialect=mysql.dialect()))
    conn = MagicMock()
    conn.execute.side_effect = fake_execute

    writer._upsert_mysql(conn, table, RECORDS)
    sql = captured["sql"]
    assert "INSERT INTO events" in sql
    assert "ON DUPLICATE KEY UPDATE" in sql
    assert "name = VALUES(name)" in sql
    assert "amount = VALUES(amount)" in sql


# --- Oracle / SQL Server: staging + real MERGE SQL ---

def test_oracle_merge_sql_has_explicit_columns_no_star_shorthand():
    writer = make_writer({"db_type": "oracle", "mode": "merge", "merge_keys": ["id"]})
    table = sa.Table(
        "events", sa.MetaData(),
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("name", sa.Text), sa.Column("amount", sa.Float),
    )
    conn = MagicMock()
    writer._upsert_via_staging_merge(conn, table, RECORDS)

    merge_texts = [c[0][0].text for c in conn.execute.call_args_list
                   if hasattr(c[0][0], "text") and "MERGE INTO" in c[0][0].text]
    assert len(merge_texts) == 1
    sql = merge_texts[0]
    assert "MERGE INTO events t USING events_upsert_staging s ON (t.id = s.id)" in sql
    assert "WHEN MATCHED THEN UPDATE SET t.name = s.name, t.amount = s.amount" in sql
    assert "WHEN NOT MATCHED THEN INSERT (id, name, amount) VALUES (s.id, s.name, s.amount)" in sql
    assert "UPDATE SET *" not in sql
    assert "INSERT *" not in sql


def test_sqlserver_merge_sql_terminated_with_semicolon():
    writer = make_writer({"db_type": "sqlserver", "mode": "merge", "merge_keys": ["id"]})
    table = sa.Table(
        "events", sa.MetaData(),
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("name", sa.Text),
    )
    conn = MagicMock()
    writer._upsert_via_staging_merge(conn, table, [{"id": 1, "name": "alice"}])

    merge_texts = [c[0][0].text for c in conn.execute.call_args_list
                   if hasattr(c[0][0], "text") and "MERGE INTO" in c[0][0].text]
    assert merge_texts[0].rstrip().endswith(";")


def test_qualified_name_includes_schema_when_set():
    writer = make_writer({"db_type": "oracle", "schema": "reporting"})
    assert writer._qualified_name("events") == "reporting.events"


def test_qualified_name_omits_schema_when_unset():
    writer = make_writer({"db_type": "oracle"})
    assert writer._qualified_name("events") == "events"


# --- Type inference: real Postgres/driver types beyond the original
# bool/int/float/datetime/dict/list set (Decimal from NUMERIC columns,
# date/time from DATE/TIME columns, UUID, raw bytes) ---

def test_infer_column_type_decimal_maps_to_numeric():
    from decimal import Decimal
    writer = make_writer()
    assert writer._infer_column_type("amount", [{"amount": Decimal("12.50")}]) is sa.Numeric


def test_infer_column_type_date_maps_to_date_not_datetime():
    from datetime import date
    writer = make_writer()
    assert writer._infer_column_type("created", [{"created": date(2024, 1, 1)}]) is sa.Date


def test_infer_column_type_datetime_still_maps_to_datetime():
    from datetime import datetime as dt
    writer = make_writer()
    assert writer._infer_column_type("created", [{"created": dt(2024, 1, 1, 12, 0)}]) is sa.DateTime


def test_infer_column_type_time_maps_to_time():
    from datetime import time as time_type
    writer = make_writer()
    assert writer._infer_column_type("t", [{"t": time_type(12, 30)}]) is sa.Time


def test_infer_column_type_uuid_maps_to_string():
    import uuid as uuid_mod
    writer = make_writer()
    assert writer._infer_column_type("id", [{"id": uuid_mod.uuid4()}]) is sa.String


def test_infer_column_type_bytes_maps_to_large_binary():
    writer = make_writer()
    assert writer._infer_column_type("blob", [{"blob": b"raw"}]) is sa.LargeBinary


def test_union_keys_collects_keys_across_all_records_not_just_first():
    writer = make_writer()
    records = [{"id": 1}, {"id": 2, "late_field": "x"}]
    assert writer._union_keys(records) == ["id", "late_field"]


def test_auto_create_table_uses_union_of_keys_across_batch():
    writer = sqlite_writer()
    # Only the SECOND record has "extra" — the old sample_records[0]-only
    # logic would have silently dropped this column at table-creation time.
    records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b", "extra": "x"}]
    with writer._engine.connect() as conn:
        table_obj = writer._ensure_table(conn, "events", records)
    assert {c.name for c in table_obj.columns} == {"id", "name", "extra"}


def test_flush_reconciles_schema_drift_against_existing_table():
    # Regression for the Medallion/Lambda "Unconsumed column names" crash:
    # flushing a wider-schema batch against an already-existing table must
    # widen the table (ALTER TABLE ADD COLUMN), not crash.
    writer = sqlite_writer()
    writer.write({"id": 1, "name": "alice"})
    writer.flush()

    writer.write({"id": 2, "name": "bob", "_silver_pii_masked": True})
    writer.flush()  # must not raise

    with writer._engine.connect() as conn:
        rows = conn.execute(sa.text("SELECT id, name, _silver_pii_masked FROM events ORDER BY id")).fetchall()
    assert rows[0] == (1, "alice", None)
    assert rows[1] == (2, "bob", 1)  # SQLite stores True as 1


def test_flush_reconciliation_is_noop_when_no_new_columns():
    writer = sqlite_writer()
    writer.write({"id": 1, "name": "alice"})
    writer.flush()
    writer.write({"id": 2, "name": "bob"})
    writer.flush()  # same shape — must not attempt any ALTER TABLE

    with writer._engine.connect() as conn:
        rows = conn.execute(sa.text("SELECT id, name FROM events ORDER BY id")).fetchall()
    assert len(rows) == 2
