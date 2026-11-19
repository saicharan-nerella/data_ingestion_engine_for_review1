"""
Unit tests for HTMLParser (Requirements 10.1–10.7).
"""
import pytest
from parser.html_parser import HTMLParser


@pytest.fixture
def parser():
    return HTMLParser(config={})


# ---------------------------------------------------------------------------
# Heading blocks (Req 10.1)
# ---------------------------------------------------------------------------

def test_h1_emits_heading_block(parser):
    blocks = parser.parse("page.html", b"<h1>Title</h1>")
    headings = [b for b in blocks if b["type"] == "heading"]
    assert len(headings) == 1
    assert headings[0]["content"] == "Title"
    assert headings[0]["level"] == 1


def test_heading_levels_h1_to_h6(parser):
    html = b"<h1>A</h1><h2>B</h2><h3>C</h3><h4>D</h4><h5>E</h5><h6>F</h6>"
    blocks = parser.parse("page.html", html)
    headings = [b for b in blocks if b["type"] == "heading"]
    assert [h["level"] for h in headings] == [1, 2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# Text blocks (Req 10.2)
# ---------------------------------------------------------------------------

def test_paragraph_emits_text_block(parser):
    blocks = parser.parse("page.html", b"<p>Hello world</p>")
    texts = [b for b in blocks if b["type"] == "text"]
    assert len(texts) == 1
    assert texts[0]["content"] == "Hello world"


def test_multiple_paragraphs(parser):
    html = b"<p>First</p><p>Second</p>"
    blocks = parser.parse("page.html", html)
    texts = [b for b in blocks if b["type"] == "text"]
    assert len(texts) == 2
    assert texts[0]["content"] == "First"
    assert texts[1]["content"] == "Second"


# ---------------------------------------------------------------------------
# Table blocks (Req 10.3)
# ---------------------------------------------------------------------------

def test_table_emits_table_block(parser):
    html = b"""
    <table>
      <tr><th>Name</th><th>Age</th></tr>
      <tr><td>Alice</td><td>30</td></tr>
    </table>
    """
    blocks = parser.parse("page.html", html)
    tables = [b for b in blocks if b["type"] == "table"]
    assert len(tables) == 1


def test_table_headers_extracted(parser):
    html = b"<table><tr><th>Col1</th><th>Col2</th></tr></table>"
    blocks = parser.parse("page.html", html)
    table = next(b for b in blocks if b["type"] == "table")
    assert table["headers"] == ["Col1", "Col2"]


def test_table_rows_extracted(parser):
    html = b"""
    <table>
      <tr><th>Name</th></tr>
      <tr><td>Alice</td></tr>
      <tr><td>Bob</td></tr>
    </table>
    """
    blocks = parser.parse("page.html", html)
    table = next(b for b in blocks if b["type"] == "table")
    assert table["rows"] == [["Alice"], ["Bob"]]


# ---------------------------------------------------------------------------
# List blocks (Req 10.4)
# ---------------------------------------------------------------------------

def test_ul_emits_list_block_unordered(parser):
    html = b"<ul><li>Item 1</li><li>Item 2</li></ul>"
    blocks = parser.parse("page.html", html)
    lists = [b for b in blocks if b["type"] == "list"]
    assert len(lists) == 1
    assert lists[0]["ordered"] is False
    assert lists[0]["items"] == ["Item 1", "Item 2"]


def test_ol_emits_list_block_ordered(parser):
    html = b"<ol><li>First</li><li>Second</li></ol>"
    blocks = parser.parse("page.html", html)
    lists = [b for b in blocks if b["type"] == "list"]
    assert len(lists) == 1
    assert lists[0]["ordered"] is True
    assert lists[0]["items"] == ["First", "Second"]


# ---------------------------------------------------------------------------
# Image blocks (Req 10.5)
# ---------------------------------------------------------------------------

def test_img_with_src_emits_image_block(parser):
    html = b'<img src="photo.jpg" alt="A photo">'
    blocks = parser.parse("page.html", html)
    images = [b for b in blocks if b["type"] == "image"]
    assert len(images) == 1
    assert images[0]["caption"] == "A photo"
    assert images[0]["data_base64"] == ""
    assert images[0]["mime_type"] == ""


def test_img_without_src_not_emitted(parser):
    html = b'<img alt="no src">'
    blocks = parser.parse("page.html", html)
    images = [b for b in blocks if b["type"] == "image"]
    assert len(images) == 0


def test_img_without_alt_has_empty_caption(parser):
    html = b'<img src="pic.png">'
    blocks = parser.parse("page.html", html)
    images = [b for b in blocks if b["type"] == "image"]
    assert images[0]["caption"] == ""


# ---------------------------------------------------------------------------
# Metadata block (Req 10.6)
# ---------------------------------------------------------------------------

def test_metadata_block_always_present(parser):
    blocks = parser.parse("page.html", b"<html></html>")
    meta_blocks = [b for b in blocks if b["type"] == "metadata"]
    assert len(meta_blocks) == 1


def test_metadata_title_extracted(parser):
    html = b"<html><head><title>My Page</title></head></html>"
    blocks = parser.parse("page.html", html)
    meta = next(b for b in blocks if b["type"] == "metadata")
    assert meta["properties"]["title"] == "My Page"


def test_metadata_title_empty_when_missing(parser):
    blocks = parser.parse("page.html", b"<html></html>")
    meta = next(b for b in blocks if b["type"] == "metadata")
    assert meta["properties"]["title"] == ""


def test_metadata_meta_tags_extracted(parser):
    html = b'<html><head><meta name="description" content="A site"></head></html>'
    blocks = parser.parse("page.html", html)
    meta = next(b for b in blocks if b["type"] == "metadata")
    assert {"name": "description", "content": "A site"} in meta["properties"]["meta_tags"]


def test_metadata_meta_tags_without_name_excluded(parser):
    html = b'<html><head><meta charset="utf-8"></head></html>'
    blocks = parser.parse("page.html", html)
    meta = next(b for b in blocks if b["type"] == "metadata")
    assert meta["properties"]["meta_tags"] == []


def test_metadata_links_extracted(parser):
    html = b'<html><body><a href="https://example.com">Link</a></body></html>'
    blocks = parser.parse("page.html", html)
    meta = next(b for b in blocks if b["type"] == "metadata")
    assert "https://example.com" in meta["properties"]["links"]


def test_metadata_links_without_href_excluded(parser):
    html = b'<html><body><a name="anchor">No href</a></body></html>'
    blocks = parser.parse("page.html", html)
    meta = next(b for b in blocks if b["type"] == "metadata")
    assert meta["properties"]["links"] == []


# ---------------------------------------------------------------------------
# Malformed HTML / best-effort (Req 10.7)
# ---------------------------------------------------------------------------

def test_malformed_html_sets_parse_warning(parser):
    # BeautifulSoup is very lenient; force a scenario where warning is set
    # by monkeypatching _extract_blocks to raise on first call
    original = parser._extract_blocks

    call_count = {"n": 0}

    def patched(soup):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise ValueError("simulated parse error")
        return original(soup)

    parser._extract_blocks = patched
    blocks = parser.parse("bad.html", b"<p>text</p>")
    assert parser.parse_warning == "malformed_html"


def test_total_failure_returns_empty_list(parser):
    def always_fail(soup):
        raise RuntimeError("total failure")

    parser._extract_blocks = always_fail
    blocks = parser.parse("bad.html", b"<p>text</p>")
    assert blocks == []


def test_empty_bytes_returns_metadata_block(parser):
    blocks = parser.parse("empty.html", b"")
    assert any(b["type"] == "metadata" for b in blocks)


def test_parse_warning_none_on_success(parser):
    parser.parse("ok.html", b"<p>Hello</p>")
    assert parser.parse_warning is None
