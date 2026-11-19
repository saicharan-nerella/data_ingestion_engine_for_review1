"""
Regression tests for the Medallion/Lambda -> DatabaseWriter table-routing
fix. Before this fix, _write_layer_output() flushed every layer
(bronze/silver/gold, batch/speed/serving) into the SAME configured table —
harmless for CloudWriter (each layer got its own S3 prefix instead), but
for DatabaseWriter this meant later layers' extra columns crashed with
"Unconsumed column names" the moment the schema drifted between layers.

The fix mirrors DataVaultArchitecture's already-correct pattern: when the
writer exposes a `table_name` (DatabaseWriter/DataWarehouseWriter), each
layer flushes to its own table (`<table>_<layer>`) instead of all layers
sharing one.
"""
import sqlalchemy as sa

from architecture.medallion_architecture.pipeline import MedallionArchitecture
from architecture.lambda_architecture.pipeline import LambdaArchitecture
from target.write_database import DatabaseWriter


def sqlite_database_writer(table="events"):
    writer = DatabaseWriter({
        "db_type": "postgres",
        "host": "unused-with-sqlite-engine",
        "database": "unused",
        "username": "u",
        "password": "p",
        "table": table,
    })
    writer._engine = sa.create_engine("sqlite:///:memory:")
    return writer


def table_names(writer):
    return sa.inspect(writer._engine).get_table_names()


def rows_of(writer, table):
    with writer._engine.connect() as conn:
        return conn.execute(sa.text(f"SELECT * FROM {table}")).fetchall()


def test_medallion_routes_each_layer_to_its_own_table_no_schema_crash():
    writer = sqlite_database_writer("events")
    arch = MedallionArchitecture({}, writers=[writer])

    # Bronze-shaped records
    arch._write_layer_output("bronze", [{"id": 1, "name": "alice"}])
    # Silver-shaped records — extra columns bronze's table never had.
    arch._write_layer_output("silver", [{"id": 1, "name": "alice", "_silver_pii_masked": True}])
    # Gold-shaped records — extra columns again.
    arch._write_layer_output("gold", [{"id": 1, "name": "alice", "_quality_score": 0.9}])

    assert set(table_names(writer)) == {"events_bronze", "events_silver", "events_gold"}
    assert len(rows_of(writer, "events_bronze")) == 1
    assert len(rows_of(writer, "events_silver")) == 1
    assert len(rows_of(writer, "events_gold")) == 1


def test_lambda_routes_each_layer_to_its_own_table_no_schema_crash():
    writer = sqlite_database_writer("events")
    arch = LambdaArchitecture({}, writers=[writer])

    arch._write_layer_output("batch", [{"id": 1, "name": "alice"}])
    arch._write_layer_output("speed", [{"id": 2, "name": "bob", "_serving_source_layer": "speed"}])
    arch._write_layer_output("serving", [{"id": 1, "name": "alice", "_serving_merged_time": "now"}])

    assert set(table_names(writer)) == {"events_batch", "events_speed", "events_serving"}
    assert len(rows_of(writer, "events_batch")) == 1
    assert len(rows_of(writer, "events_speed")) == 1
    assert len(rows_of(writer, "events_serving")) == 1


def test_medallion_with_cloud_writer_still_uses_prefix_routing_not_tables():
    # CloudWriter has no table_name attribute, so the fix must leave its
    # existing prefix-based routing untouched.
    class FakeCloudWriter:
        def __init__(self):
            self.prefix = "structured/"
            self.flushed_with = []

        def write(self, record):
            pass

        def flush(self):
            self.flushed_with.append(self.prefix)

    writer = FakeCloudWriter()
    arch = MedallionArchitecture({}, writers=[writer])
    arch._write_layer_output("bronze", [{"id": 1}])

    assert writer.flushed_with == ["structured/bronze/"]
    assert writer.prefix == "structured/"  # restored after flush
