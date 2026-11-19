"""
Unit tests for JSONParser (Requirements 12.1, 12.2).
"""
import pytest
from parser.json_parser import JSONParser


@pytest.fixture
def parser():
    return JSONParser(config={})


# ── Requirement 12.1 — valid JSON ────────────────────────────────────────────

def test_valid_json_object_returns_metadata_block(parser):
    content = b'{"key": "value"}'
    blocks = parser.parse("file.json", content)
    assert blocks == [{"type": "metadata", "properties": {"data": {"key": "value"}}}]


def test_valid_json_array_returns_metadata_block(parser):
    content = b'[1, 2, 3]'
    blocks = parser.parse("file.json", content)
    assert blocks == [{"type": "metadata", "properties": {"data": [1, 2, 3]}}]


def test_valid_json_scalar_string(parser):
    content = b'"hello"'
    blocks = parser.parse("file.json", content)
    assert blocks == [{"type": "metadata", "properties": {"data": "hello"}}]


def test_valid_json_nested_object(parser):
    content = b'{"a": {"b": [1, 2]}}'
    blocks = parser.parse("file.json", content)
    assert blocks[0]["properties"]["data"] == {"a": {"b": [1, 2]}}


def test_returns_single_block(parser):
    blocks = parser.parse("file.json", b'{"x": 1}')
    assert len(blocks) == 1


def test_block_type_is_metadata(parser):
    blocks = parser.parse("file.json", b'{}')
    assert blocks[0]["type"] == "metadata"


def test_block_has_properties_key(parser):
    blocks = parser.parse("file.json", b'{}')
    assert "properties" in blocks[0]


def test_block_properties_has_data_key(parser):
    blocks = parser.parse("file.json", b'{"foo": "bar"}')
    assert "data" in blocks[0]["properties"]


# ── Requirement 12.2 — invalid JSON ──────────────────────────────────────────

def test_invalid_json_returns_empty_list(parser):
    blocks = parser.parse("file.json", b'not valid json')
    assert blocks == []


def test_empty_bytes_returns_empty_list(parser):
    blocks = parser.parse("file.json", b'')
    assert blocks == []


def test_truncated_json_returns_empty_list(parser):
    blocks = parser.parse("file.json", b'{"key": ')
    assert blocks == []


def test_invalid_json_logs_error(parser, caplog):
    import logging
    with caplog.at_level(logging.ERROR, logger="parser.json_parser"):
        parser.parse("bad.json", b'not json')
    assert "bad.json" in caplog.text
