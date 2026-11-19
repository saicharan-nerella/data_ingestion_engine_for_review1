"""
Unit tests for XMLParser (Requirements 11.1, 11.2).
"""
import logging
import pytest
from parser.xml_parser import XMLParser


@pytest.fixture
def parser():
    return XMLParser(config={})


def test_valid_xml_returns_single_metadata_block(parser):
    content = b"<root><child>text</child></root>"
    blocks = parser.parse("file.xml", content)
    assert len(blocks) == 1
    assert blocks[0]["type"] == "metadata"


def test_valid_xml_block_has_root_tag(parser):
    content = b"<root/>"
    blocks = parser.parse("file.xml", content)
    assert blocks[0]["properties"]["root_tag"] == "root"


def test_valid_xml_block_has_data_key(parser):
    content = b"<root/>"
    blocks = parser.parse("file.xml", content)
    assert "data" in blocks[0]["properties"]


def test_root_tag_reflects_actual_tag(parser):
    content = b"<catalog><item>x</item></catalog>"
    blocks = parser.parse("file.xml", content)
    assert blocks[0]["properties"]["root_tag"] == "catalog"


def test_nested_xml_converted_to_dict(parser):
    content = b"<root><child attr='x'>text</child></root>"
    blocks = parser.parse("file.xml", content)
    data = blocks[0]["properties"]["data"]
    assert "child" in data
    assert data["child"]["@attr"] == "x"
    assert data["child"]["#text"] == "text"


def test_attributes_prefixed_with_at(parser):
    content = b'<root id="42"/>'
    blocks = parser.parse("file.xml", content)
    data = blocks[0]["properties"]["data"]
    assert data["@id"] == "42"


def test_leaf_text_stored_under_hash_text(parser):
    content = b"<root><name>Alice</name></root>"
    blocks = parser.parse("file.xml", content)
    data = blocks[0]["properties"]["data"]
    assert data["name"]["#text"] == "Alice"


def test_deeply_nested_structure(parser):
    content = b"<a><b><c>deep</c></b></a>"
    blocks = parser.parse("file.xml", content)
    data = blocks[0]["properties"]["data"]
    assert data["b"]["c"]["#text"] == "deep"


def test_malformed_xml_returns_empty_list(parser):
    blocks = parser.parse("bad.xml", b"<root><unclosed>")
    assert blocks == []


def test_malformed_xml_logs_error_with_object_key(parser, caplog):
    with caplog.at_level(logging.ERROR, logger="parser.xml_parser"):
        parser.parse("bad.xml", b"<root><unclosed>")
    assert "bad.xml" in caplog.text


def test_malformed_xml_logs_error_message(parser, caplog):
    with caplog.at_level(logging.ERROR, logger="parser.xml_parser"):
        parser.parse("bad.xml", b"not xml at all <<<")
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"


def test_empty_bytes_returns_empty_list(parser):
    blocks = parser.parse("empty.xml", b"")
    assert blocks == []
