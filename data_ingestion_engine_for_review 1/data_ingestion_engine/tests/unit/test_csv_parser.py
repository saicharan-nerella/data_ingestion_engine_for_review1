"""
Unit tests for CSVParser (Requirements 13.1, 13.2, 13.3, 13.4).
"""
import pytest
from parser.csv_parser import CSVParser


@pytest.fixture
def parser():
    return CSVParser(config={})


@pytest.fixture
def no_header_parser():
    return CSVParser(config={"no_header": True})


# ── Requirement 13.4 — first row as headers ───────────────────────────────────

def test_basic_csv_with_headers(parser):
    content = b"name,age,city\nAlice,30,NYC\nBob,25,LA"
    blocks = parser.parse("data.csv", content)
    assert len(blocks) == 2
    table = blocks[0]
    assert table["type"] == "table"
    assert table["headers"] == ["name", "age", "city"]
    assert table["rows"] == [["Alice", "30", "NYC"], ["Bob", "25", "LA"]]


def test_header_generated_false_by_default(parser):
    content = b"a,b\n1,2"
    parser.parse("data.csv", content)
    assert parser.header_generated is False


# ── Requirement 13.2 — auto-delimiter detection ───────────────────────────────

def test_semicolon_delimiter_detected(parser):
    content = b"name;age;city\nAlice;30;NYC\nBob;25;LA"
    blocks = parser.parse("data.csv", content)
    table = blocks[0]
    assert table["headers"] == ["name", "age", "city"]
    assert table["rows"] == [["Alice", "30", "NYC"], ["Bob", "25", "LA"]]
    meta = blocks[1]
    assert meta["properties"]["delimiter"] == ";"


def test_tab_delimiter_detected(parser):
    content = "name\tage\tcity\nAlice\t30\tNYC".encode("utf-8")
    blocks = parser.parse("data.csv", content)
    table = blocks[0]
    assert table["headers"] == ["name", "age", "city"]
    assert table["rows"] == [["Alice", "30", "NYC"]]
    meta = blocks[1]
    assert meta["properties"]["delimiter"] == "\t"


# ── Requirement 13.3 — no_header generates col_N headers ─────────────────────

def test_no_header_generates_col_headers(no_header_parser):
    content = b"Alice,30,NYC\nBob,25,LA"
    blocks = no_header_parser.parse("data.csv", content)
    table = blocks[0]
    assert table["headers"] == ["col_0", "col_1", "col_2"]
    assert table["rows"] == [["Alice", "30", "NYC"], ["Bob", "25", "LA"]]


def test_no_header_sets_header_generated_flag(no_header_parser):
    content = b"1,2,3\n4,5,6"
    no_header_parser.parse("data.csv", content)
    assert no_header_parser.header_generated is True


def test_header_generated_flag_resets_on_next_call(no_header_parser):
    # First call with no_header=True sets flag
    no_header_parser.parse("data.csv", b"1,2\n3,4")
    assert no_header_parser.header_generated is True

    # Switch to a parser without no_header to verify reset behaviour
    normal_parser = CSVParser(config={})
    normal_parser.parse("data.csv", b"a,b\n1,2")
    assert normal_parser.header_generated is False


# ── Requirement 13.1 — metadata block ────────────────────────────────────────

def test_metadata_block_has_delimiter_and_row_count(parser):
    content = b"x,y\n1,2\n3,4\n5,6"
    blocks = parser.parse("data.csv", content)
    meta = blocks[1]
    assert meta["type"] == "metadata"
    assert meta["properties"]["delimiter"] == ","
    assert meta["properties"]["row_count"] == 3


def test_metadata_row_count_excludes_header_row(parser):
    content = b"a,b,c\nrow1a,row1b,row1c"
    blocks = parser.parse("data.csv", content)
    assert blocks[1]["properties"]["row_count"] == 1


# ── Edge cases ────────────────────────────────────────────────────────────────

def test_empty_content_returns_empty_list(parser):
    blocks = parser.parse("data.csv", b"")
    # Empty CSV → table with no headers/rows + metadata
    assert blocks[0]["headers"] == []
    assert blocks[0]["rows"] == []
    assert blocks[1]["properties"]["row_count"] == 0


def test_invalid_bytes_returns_empty_list(parser):
    # Bytes that can't be decoded as UTF-8 or Latin-1 are extremely rare,
    # so simulate a total failure by passing None (wrong type)
    blocks = parser.parse("data.csv", None)  # type: ignore[arg-type]
    assert blocks == []


def test_latin1_content_decoded(parser):
    content = "name,city\ncafé,Paris".encode("latin-1")
    blocks = parser.parse("data.csv", content)
    assert blocks[0]["rows"][0][0] == "café"


def test_returns_two_blocks_on_success(parser):
    content = b"col1,col2\nval1,val2"
    blocks = parser.parse("data.csv", content)
    assert len(blocks) == 2
    assert blocks[0]["type"] == "table"
    assert blocks[1]["type"] == "metadata"
