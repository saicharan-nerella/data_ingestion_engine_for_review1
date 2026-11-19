from unittest.mock import MagicMock

import pytest

from architecture.data_vault_architecture.pipeline import DataVaultArchitecture, md5_hash, Pipeline


def make_config(hubs=None, links=None, satellites=None):
    return {
        "data_vault": {
            "hubs": hubs or [],
            "links": links or [],
            "satellites": satellites or [],
        },
        "alerting": {},
    }


def make_cloud_like_writer():
    """No table_name attribute — routes via prefix, like CloudWriter."""
    writer = MagicMock(spec=["write", "flush", "prefix", "_batch_mode"])
    writer.prefix = ""
    writer._written = []
    writer.write.side_effect = lambda r: writer._written.append(r)
    return writer


def make_table_writer():
    """Has table_name — routes via flush(table_name=...), like Database/Warehouse writers."""
    writer = MagicMock(spec=["write", "flush", "table_name", "_batch_mode"])
    writer.table_name = "events"
    writer._written = []
    writer.write.side_effect = lambda r: writer._written.append(r)
    return writer


STAGED = [
    {"customer_id": "C1", "order_id": "O1", "name": "Alice", "status": "active"},
    {"customer_id": "C1", "order_id": "O2", "name": "Alice", "status": "active"},
    {"customer_id": "C2", "order_id": "O3", "name": "Bob", "status": "pending"},
]


# --- md5_hash ---

def test_md5_hash_deterministic_and_case_insensitive():
    assert md5_hash("abc") == md5_hash("ABC") == md5_hash(" abc ")


def test_md5_hash_none_and_empty_share_sentinel():
    assert md5_hash(None) == md5_hash("")


# --- Hubs ---

def test_load_hubs_one_row_per_unique_key():
    arch = DataVaultArchitecture(make_config(hubs=[{"business_key": "customer_id", "source": "customers"}]))
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    count = arch._load_hubs(STAGED)
    assert count == 2  # C1, C2 — deduped
    hash_keys = {r["hash_key"] for r in writer._written}
    assert hash_keys == {md5_hash("C1"), md5_hash("C2")}
    assert all(r["record_source"] == "customers" for r in writer._written)


def test_load_hubs_skips_config_missing_business_key():
    arch = DataVaultArchitecture(make_config(hubs=[{"source": "x"}]))
    arch.writers = [make_cloud_like_writer()]
    assert arch._load_hubs(STAGED) == 0


def test_hub_routes_via_prefix_for_cloud_like_writer():
    arch = DataVaultArchitecture(make_config(hubs=[{"business_key": "customer_id"}]))
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    arch._load_hubs(STAGED)
    assert writer.prefix == ""  # restored after flush
    writer.flush.assert_called_once_with()


def test_hub_routes_via_table_name_override_for_db_writer():
    arch = DataVaultArchitecture(make_config(hubs=[{"business_key": "customer_id"}]))
    writer = make_table_writer()
    arch.writers = [writer]
    arch._load_hubs(STAGED)
    writer.flush.assert_called_once_with(table_name="events_hub_customer_id")


# --- Links ---

def test_load_links_one_row_per_unique_pair():
    arch = DataVaultArchitecture(make_config(links=[{"hub_a": "customer_id", "hub_b": "order_id"}]))
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    count = arch._load_links(STAGED)
    assert count == 3  # (C1,O1) (C1,O2) (C2,O3) — all distinct pairs
    for row in writer._written:
        assert "link_hk" in row and "hub_a_hk" in row and "hub_b_hk" in row


def test_load_links_skips_rows_with_missing_hub_value():
    arch = DataVaultArchitecture(make_config(links=[{"hub_a": "customer_id", "hub_b": "missing_field"}]))
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    count = arch._load_links(STAGED)
    assert count == 0


# --- Satellites ---

def test_load_satellites_dedupes_unchanged_consecutive_values():
    arch = DataVaultArchitecture(make_config(satellites=[
        {"business_key": "customer_id", "attribute_columns": ["name", "status"], "source": "customers"}
    ]))
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    count = arch._load_satellites(STAGED)
    # C1 appears twice with IDENTICAL name/status -> only 1 satellite row for C1, 1 for C2
    assert count == 2
    hub_hks = {r["hub_hk"] for r in writer._written}
    assert hub_hks == {md5_hash("C1"), md5_hash("C2")}


def test_load_satellites_inserts_new_row_when_attributes_change():
    arch = DataVaultArchitecture(make_config(satellites=[
        {"business_key": "customer_id", "attribute_columns": ["status"]}
    ]))
    staged = [
        {"customer_id": "C1", "status": "active"},
        {"customer_id": "C1", "status": "cancelled"},  # changed -> new satellite row
    ]
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    count = arch._load_satellites(staged)
    assert count == 2
    hash_diffs = [r["hash_diff"] for r in writer._written]
    assert len(set(hash_diffs)) == 2  # genuinely different hash_diff values


def test_load_satellites_skips_config_missing_fields():
    arch = DataVaultArchitecture(make_config(satellites=[{"business_key": "customer_id"}]))  # no attribute_columns
    arch.writers = [make_cloud_like_writer()]
    assert arch._load_satellites(STAGED) == 0


def test_satellite_routes_via_table_name_override():
    arch = DataVaultArchitecture(make_config(satellites=[
        {"business_key": "customer_id", "attribute_columns": ["name"]}
    ]))
    writer = make_table_writer()
    arch.writers = [writer]
    arch._load_satellites(STAGED)
    writer.flush.assert_called_once_with(table_name="events_satellite_customer_id")


# --- Full execute() ---

def test_execute_loads_hub_link_satellite_in_order():
    arch = DataVaultArchitecture(make_config(
        hubs=[{"business_key": "customer_id"}],
        links=[{"hub_a": "customer_id", "hub_b": "order_id"}],
        satellites=[{"business_key": "customer_id", "attribute_columns": ["name", "status"]}],
    ))
    writer = make_cloud_like_writer()
    arch.writers = [writer]
    counts = arch.execute(STAGED)
    assert counts == {"hub_rows": 2, "link_rows": 3, "satellite_rows": 2}


def test_write_entity_noop_with_no_writers():
    arch = DataVaultArchitecture(make_config())
    arch.writers = []
    arch._write_entity("hub_x", [{"a": 1}])  # should not raise


def test_write_entity_warns_on_empty_records(caplog):
    import logging
    arch = DataVaultArchitecture(make_config())
    arch.writers = [make_cloud_like_writer()]
    with caplog.at_level(logging.WARNING, logger="architecture.data_vault_architecture.pipeline"):
        arch._write_entity("hub_x", [])
    assert any("no hub_x record(s)" in m for m in caplog.messages)


# --- Pipeline wrapper ---

def test_pipeline_no_records_returns_warning():
    class _EmptyReader:
        def connect(self): pass
        def read(self): return iter([])
        def close(self): pass

    pipeline = Pipeline(make_config(), readers=[_EmptyReader()], writers=[])
    result = pipeline.execute()
    assert result == {"status": "warning", "records": 0}
