import json
from unittest.mock import MagicMock, patch

import pytest

from target.write_data_warehouse import DataWarehouseWriter


BASE_CONFIG = {
    "warehouse_type": "redshift",
    "cluster_identifier": "test-cluster",
    "database": "analytics",
    "db_user": "glue_iam_user",
    "schema": "public",
    "table": "events",
    "staging_bucket": "test-staging-bucket",
    "staging_prefix": "warehouse_staging/",
    "iam_role": "arn:aws:iam::855738613861:role/redshift-copy-role",
    "region": "us-east-1",
    # Every existing test below exercises COPY/merge/overwrite SQL
    # construction against a table assumed to already exist — auto-create
    # is a separate, newer behavior with its own dedicated tests further
    # down, so it's turned off here to keep these tests' mocks (which
    # don't set up get_statement_result for an auto-create's SELECT/CREATE/
    # ALTER round-trip) exercising exactly what they did before.
    "auto_create_table": "false",
}

RECORDS = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
]


def make_writer(extra=None):
    config = {**BASE_CONFIG, **(extra or {})}
    return DataWarehouseWriter(config)


def mock_data_api_client(final_status="FINISHED", existing_columns=None):
    """existing_columns=None simulates information_schema.columns returning
    no rows (table doesn't exist yet); a list simulates it already existing
    with exactly those column names — only consulted by tests that enable
    auto_create_table and therefore actually call get_statement_result."""
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-123"}
    client.describe_statement.return_value = {"Status": final_status}
    records = [] if existing_columns is None else [[{"stringValue": c}] for c in existing_columns]
    client.get_statement_result.return_value = {"Records": records}
    return client


def write_all(writer, records):
    for r in records:
        writer.write(r)


# --- Config validation ---

def test_non_redshift_warehouse_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="snowflake"):
        make_writer({"warehouse_type": "snowflake"})


def test_missing_cluster_and_workgroup_raises():
    config = {**BASE_CONFIG}
    del config["cluster_identifier"]
    with pytest.raises(ValueError, match="cluster_identifier.*workgroup_name"):
        DataWarehouseWriter(config)


def test_missing_database_raises():
    config = {**BASE_CONFIG}
    del config["database"]
    with pytest.raises(ValueError, match="database"):
        DataWarehouseWriter(config)


def test_missing_db_user_and_secret_raises():
    config = {**BASE_CONFIG}
    del config["db_user"]
    with pytest.raises(ValueError, match="db_user.*secret_arn"):
        DataWarehouseWriter(config)


def test_missing_staging_bucket_raises():
    config = {**BASE_CONFIG}
    del config["staging_bucket"]
    with pytest.raises(ValueError, match="staging_bucket"):
        DataWarehouseWriter(config)


def test_missing_iam_role_raises():
    config = {**BASE_CONFIG}
    del config["iam_role"]
    with pytest.raises(ValueError, match="iam_role"):
        DataWarehouseWriter(config)


def test_unsupported_staging_format_raises():
    with pytest.raises(ValueError, match="staging_format"):
        make_writer({"staging_format": "avro"})


def test_unsupported_mode_raises():
    with pytest.raises(ValueError, match="Unsupported mode"):
        make_writer({"mode": "replace"})


def test_merge_mode_without_merge_keys_raises():
    with pytest.raises(ValueError, match="merge_keys"):
        make_writer({"mode": "merge"})


def test_merge_mode_with_merge_keys_string_is_parsed():
    writer = make_writer({"mode": "merge", "merge_keys": "id, tenant_id"})
    assert writer.merge_keys == ["id", "tenant_id"]


def test_workgroup_name_accepted_without_cluster_identifier():
    config = {**BASE_CONFIG}
    del config["cluster_identifier"]
    config["workgroup_name"] = "serverless-wg"
    writer = DataWarehouseWriter(config)
    assert writer.workgroup_name == "serverless-wg"


def test_secret_arn_accepted_without_db_user():
    config = {**BASE_CONFIG}
    del config["db_user"]
    config["secret_arn"] = "arn:aws:secretsmanager:us-east-1:855738613861:secret:rs-creds"
    writer = DataWarehouseWriter(config)
    assert writer.secret_arn is not None


def test_prefix_attribute_present_and_reassignable():
    # LambdaArchitecture/MedallionArchitecture do
    # `writer.prefix = f"{getattr(writer, 'prefix', '') or ''}{layer}/"`
    # on every configured writer regardless of type — must not crash here.
    writer = make_writer()
    assert writer.prefix == ""
    writer.prefix = f"{writer.prefix}batch/"
    assert writer.prefix == "batch/"


# --- write()/flush() incremental interface ---

def test_write_buffers_without_calling_aws():
    writer = make_writer()
    with patch("boto3.client") as mock_boto:
        writer.write(RECORDS[0])
        writer.write(RECORDS[1])
        mock_boto.assert_not_called()
    assert writer._buffer == RECORDS


def test_flush_empty_buffer_noop():
    writer = make_writer()
    with patch("boto3.client") as mock_boto:
        writer.flush()
        mock_boto.assert_not_called()


def test_flush_clears_buffer():
    writer = make_writer()
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    assert writer._buffer == []


def test_flush_missing_table_name_raises():
    config = {**BASE_CONFIG}
    del config["table"]
    writer = DataWarehouseWriter(config)
    writer.write(RECORDS[0])
    with pytest.raises(ValueError, match="table_name"):
        writer.flush()


# --- append mode: stage + COPY ---

def test_append_mode_stages_to_s3_and_copies():
    writer = make_writer()
    s3_client = MagicMock()
    rs_client = mock_data_api_client()

    def fake_boto_client(service_name, **kwargs):
        return s3_client if service_name == "s3" else rs_client

    with patch("boto3.client", side_effect=fake_boto_client):
        write_all(writer, RECORDS)
        writer.flush()

    s3_client.put_object.assert_called_once()
    put_kwargs = s3_client.put_object.call_args[1]
    assert put_kwargs["Bucket"] == "test-staging-bucket"
    assert put_kwargs["Key"].startswith("warehouse_staging/events/")
    assert put_kwargs["Key"].endswith(".csv")

    rs_client.execute_statement.assert_called_once()
    sql = rs_client.execute_statement.call_args[1]["Sql"]
    assert "COPY public.events FROM" in sql
    assert "IAM_ROLE" in sql
    assert put_kwargs["Key"] in sql

    s3_client.delete_object.assert_called_once_with(
        Bucket="test-staging-bucket", Key=put_kwargs["Key"]
    )


def test_append_mode_uses_cluster_identifier_kwarg():
    writer = make_writer()
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    call_kwargs = rs_client.execute_statement.call_args[1]
    assert call_kwargs["ClusterIdentifier"] == "test-cluster"
    assert call_kwargs["DbUser"] == "glue_iam_user"
    assert call_kwargs["Database"] == "analytics"


def test_workgroup_mode_uses_workgroup_kwarg():
    config = {**BASE_CONFIG}
    del config["cluster_identifier"]
    config["workgroup_name"] = "serverless-wg"
    writer = DataWarehouseWriter(config)
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    call_kwargs = rs_client.execute_statement.call_args[1]
    assert call_kwargs["WorkgroupName"] == "serverless-wg"
    assert "ClusterIdentifier" not in call_kwargs


def test_secret_arn_mode_uses_secret_kwarg():
    config = {**BASE_CONFIG}
    del config["db_user"]
    config["secret_arn"] = "arn:aws:secretsmanager:us-east-1:855738613861:secret:rs-creds"
    writer = DataWarehouseWriter(config)
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    call_kwargs = rs_client.execute_statement.call_args[1]
    assert call_kwargs["SecretArn"] == config["secret_arn"]
    assert "DbUser" not in call_kwargs


# --- overwrite mode ---

def test_overwrite_mode_truncates_then_copies_in_one_statement():
    writer = make_writer({"mode": "overwrite"})
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    sql = rs_client.execute_statement.call_args[1]["Sql"]
    assert "TRUNCATE TABLE public.events" in sql
    assert "COPY public.events FROM" in sql
    assert rs_client.execute_statement.call_count == 1


# --- merge mode (upsert via temp staging table) ---

def test_merge_mode_builds_single_multistatement_upsert():
    writer = make_writer({"mode": "merge", "merge_keys": ["id"]})
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()

    assert rs_client.execute_statement.call_count == 1
    sql = rs_client.execute_statement.call_args[1]["Sql"]
    assert "CREATE TEMP TABLE events_upsert_staging (LIKE public.events)" in sql
    assert "COPY events_upsert_staging FROM" in sql
    assert "DELETE FROM public.events USING events_upsert_staging" in sql
    assert "public.events.id = events_upsert_staging.id" in sql
    assert "INSERT INTO public.events SELECT * FROM events_upsert_staging" in sql
    assert "DROP TABLE events_upsert_staging" in sql
    assert "BEGIN;" in sql
    assert "COMMIT;" in sql


def test_merge_mode_multiple_merge_keys_joined_with_and():
    writer = make_writer({"mode": "merge", "merge_keys": ["id", "name"]})
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    sql = rs_client.execute_statement.call_args[1]["Sql"]
    assert "public.events.id = events_upsert_staging.id" in sql
    assert "public.events.name = events_upsert_staging.name" in sql
    assert " AND " in sql


# --- Redshift Data API polling / failure handling ---

def test_statement_failure_raises_runtime_error():
    writer = make_writer()
    s3_client = MagicMock()
    rs_client = MagicMock()
    rs_client.execute_statement.return_value = {"Id": "stmt-err"}
    rs_client.describe_statement.return_value = {
        "Status": "FAILED",
        "Error": "column count mismatch",
    }
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        with pytest.raises(RuntimeError, match="column count mismatch"):
            writer.flush()
    s3_client.delete_object.assert_called_once()


def test_statement_aborted_raises_runtime_error():
    writer = make_writer()
    s3_client = MagicMock()
    rs_client = MagicMock()
    rs_client.execute_statement.return_value = {"Id": "stmt-abort"}
    rs_client.describe_statement.return_value = {"Status": "ABORTED"}
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        with pytest.raises(RuntimeError, match="aborted"):
            writer.flush()


def test_statement_polls_until_finished():
    writer = make_writer()
    s3_client = MagicMock()
    rs_client = MagicMock()
    rs_client.execute_statement.return_value = {"Id": "stmt-poll"}
    rs_client.describe_statement.side_effect = [
        {"Status": "STARTED"},
        {"Status": "STARTED"},
        {"Status": "FINISHED"},
    ]
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client), \
         patch("time.sleep"):
        write_all(writer, RECORDS)
        writer.flush()
    assert rs_client.describe_statement.call_count == 3


# --- Staging formats ---

def test_csv_staging_format_produces_header_and_rows():
    writer = make_writer({"staging_format": "csv"})
    body = writer._format_csv(RECORDS)
    text = body.decode("utf-8")
    lines = text.strip().split("\r\n") if "\r\n" in text else text.strip().split("\n")
    assert lines[0] == "id,name"
    assert "1,alice" in lines[1]


def test_json_staging_format_produces_one_object_per_line():
    writer = make_writer({"staging_format": "json"})
    body = writer._format_json(RECORDS)
    lines = body.decode("utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1, "name": "alice"}


def test_json_mode_copy_statement_uses_auto_format():
    writer = make_writer({"staging_format": "json"})
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        write_all(writer, RECORDS)
        writer.flush()
    sql = rs_client.execute_statement.call_args[1]["Sql"]
    assert "FORMAT AS JSON 'auto'" in sql


# --- Auto-create / widen table (auto_create_table — default true in
# production; BASE_CONFIG disables it above so the tests before this point
# keep testing exactly what they did before this feature existed) ---

def test_auto_create_table_creates_table_when_missing():
    writer = make_writer({"auto_create_table": "true"})
    write_all(writer, RECORDS)  # RECORDS = [{"id":1,"name":"alice"}, {"id":2,"name":"bob"}]
    s3_client = MagicMock()
    rs_client = mock_data_api_client(existing_columns=None)  # table doesn't exist
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        writer.flush()

    sqls = [c.kwargs["Sql"] for c in rs_client.execute_statement.call_args_list]
    create_stmts = [s for s in sqls if s.startswith("CREATE TABLE IF NOT EXISTS")]
    assert len(create_stmts) == 1
    assert "id VARCHAR(MAX)" in create_stmts[0]
    assert "name VARCHAR(MAX)" in create_stmts[0]


def test_auto_create_table_widens_existing_table_with_new_column():
    writer = make_writer({"auto_create_table": "true"})
    writer.write({"id": 1, "name": "alice", "extra_field": "x"})
    s3_client = MagicMock()
    rs_client = mock_data_api_client(existing_columns=["id", "name"])  # missing extra_field
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        writer.flush()

    sqls = [c.kwargs["Sql"] for c in rs_client.execute_statement.call_args_list]
    alter_stmts = [s for s in sqls if s.startswith("ALTER TABLE")]
    assert len(alter_stmts) == 1
    assert "ADD COLUMN extra_field VARCHAR(MAX)" in alter_stmts[0]
    assert not any(s.startswith("CREATE TABLE") for s in sqls)


def test_auto_create_table_noop_when_columns_already_match():
    writer = make_writer({"auto_create_table": "true"})
    write_all(writer, RECORDS)
    s3_client = MagicMock()
    rs_client = mock_data_api_client(existing_columns=["id", "name"])  # already matches
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        writer.flush()

    sqls = [c.kwargs["Sql"] for c in rs_client.execute_statement.call_args_list]
    assert not any(s.startswith("CREATE TABLE") or s.startswith("ALTER TABLE") for s in sqls)
    assert any(s.startswith("SELECT column_name") for s in sqls)  # the existence/column check ran
    assert any(s.startswith("COPY ") for s in sqls)  # and the actual load still happened


def test_auto_create_table_disabled_skips_column_check_entirely():
    writer = make_writer({"auto_create_table": "false"})
    write_all(writer, RECORDS)
    s3_client = MagicMock()
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        writer.flush()

    rs_client.get_statement_result.assert_not_called()
    sqls = [c.kwargs["Sql"] for c in rs_client.execute_statement.call_args_list]
    assert len(sqls) == 1  # only the COPY — no auto-create overhead at all
    assert sqls[0].startswith("COPY ")


def test_union_keys_collects_keys_across_all_records_not_just_first():
    records = [{"id": 1}, {"id": 2, "late_field": "x"}]
    assert DataWarehouseWriter._union_keys(records) == ["id", "late_field"]


def test_cleanup_failure_does_not_mask_successful_write(caplog):
    import logging
    writer = make_writer()
    s3_client = MagicMock()
    s3_client.delete_object.side_effect = Exception("network blip")
    rs_client = mock_data_api_client()
    with patch("boto3.client", side_effect=lambda svc, **kw: s3_client if svc == "s3" else rs_client):
        with caplog.at_level(logging.WARNING, logger="target.write_data_warehouse"):
            write_all(writer, RECORDS)
            writer.flush()  # should not raise
    assert any("could not delete staging file" in m for m in caplog.messages)
