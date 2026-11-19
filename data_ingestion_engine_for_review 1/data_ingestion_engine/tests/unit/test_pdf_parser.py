"""
Unit tests for PDFParser (Requirements 5.1–5.7).
"""
from unittest.mock import MagicMock, patch

import pytest

from parser.pdf_parser import PDFParser


@pytest.fixture
def parser():
    return PDFParser(config={})


def _make_mock_pdf(metadata=None, pages=None):
    mock_pdf = MagicMock()
    mock_pdf.metadata = metadata or {}
    mock_pdf.pages = []

    for page_spec in (pages or []):
        page = MagicMock()
        page.extract_text.return_value = page_spec.get("text", "")
        page.extract_tables.return_value = page_spec.get("tables", [])
        page.images = page_spec.get("images", [])
        mock_pdf.pages.append(page)

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=mock_pdf)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


def test_valid_pdf_first_block_is_metadata(parser):
    cm = _make_mock_pdf(
        metadata={"Title": "My Doc", "Author": "Alice", "CreationDate": "2024-01-01"},
        pages=[{"text": "Hello world"}],
    )
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    assert blocks[0]["type"] == "metadata"


def test_metadata_block_has_required_fields(parser):
    cm = _make_mock_pdf(
        metadata={"Title": "Report", "Author": "Bob", "CreationDate": "D:20240101"},
        pages=[{"text": "Content"}],
    )
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    props = blocks[0]["properties"]
    assert props["title"] == "Report"
    assert props["author"] == "Bob"
    assert props["creation_date"] == "D:20240101"
    assert props["page_count"] == 1


def test_metadata_page_count_reflects_pages(parser):
    cm = _make_mock_pdf(pages=[{"text": "Page 1"}, {"text": "Page 2"}, {"text": "Page 3"}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    assert blocks[0]["properties"]["page_count"] == 3


def test_metadata_missing_fields_default_to_empty_string(parser):
    cm = _make_mock_pdf(metadata={}, pages=[{"text": "Hi"}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    props = blocks[0]["properties"]
    assert props["title"] == ""
    assert props["author"] == ""
    assert props["creation_date"] == ""


def test_text_page_emits_text_blocks_with_page_number(parser):
    cm = _make_mock_pdf(pages=[{"text": "Hello world\n\nSecond paragraph"}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    text_blocks = [b for b in blocks if b["type"] == "text"]
    assert all(b["page"] == 1 for b in text_blocks)


def test_text_blocks_split_by_double_newline(parser):
    text = "First paragraph sentence here.\n\nSecond paragraph sentence here.\n\nThird paragraph sentence here."
    cm = _make_mock_pdf(pages=[{"text": text}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    text_blocks = [b for b in blocks if b["type"] == "text"]
    contents = [b["content"] for b in text_blocks]
    assert "First paragraph sentence here." in contents
    assert "Second paragraph sentence here." in contents
    assert "Third paragraph sentence here." in contents


def test_empty_text_page_sets_has_ocr_required(parser):
    cm = _make_mock_pdf(pages=[{"text": ""}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        parser.parse("doc.pdf", b"fake")
    assert parser.has_ocr_required is True


def test_non_empty_page_does_not_set_has_ocr_required(parser):
    cm = _make_mock_pdf(pages=[{"text": "Some text here"}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        parser.parse("doc.pdf", b"fake")
    assert parser.has_ocr_required is False


def test_empty_text_page_emits_empty_text_block(parser):
    cm = _make_mock_pdf(pages=[{"text": ""}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    text_blocks = [b for b in blocks if b["type"] == "text"]
    assert any(b["content"] == "" and b["page"] == 1 for b in text_blocks)


def test_short_line_without_period_emits_heading(parser):
    cm = _make_mock_pdf(pages=[{"text": "Introduction\n\nThis is a longer paragraph sentence."}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    headings = [b for b in blocks if b["type"] == "heading"]
    assert any(b["content"] == "Introduction" for b in headings)


def test_table_emits_table_block(parser):
    table_data = [["Name", "Age"], ["Alice", "30"], ["Bob", "25"]]
    cm = _make_mock_pdf(pages=[{"text": "Some text", "tables": [table_data]}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    table_blocks = [b for b in blocks if b["type"] == "table"]
    assert len(table_blocks) == 1


def test_table_headers_and_rows_extracted(parser):
    table_data = [["Name", "Age"], ["Alice", "30"], ["Bob", "25"]]
    cm = _make_mock_pdf(pages=[{"text": "Some text", "tables": [table_data]}])
    with patch("parser.pdf_parser.pdfplumber.open", return_value=cm):
        blocks = parser.parse("doc.pdf", b"fake")
    table = next(b for b in blocks if b["type"] == "table")
    assert table["headers"] == ["Name", "Age"]
    assert table["rows"] == [["Alice", "30"], ["Bob", "25"]]


def test_invalid_pdf_returns_empty_list(parser):
    with patch("parser.pdf_parser.pdfplumber.open", side_effect=Exception("corrupt PDF")):
        blocks = parser.parse("bad.pdf", b"not a pdf")
    assert blocks == []


def test_pdf_syntax_error_returns_empty_list(parser):
    with patch("parser.pdf_parser.pdfplumber.open", side_effect=ValueError("PDFSyntaxError")):
        blocks = parser.parse("bad.pdf", b"garbage")
    assert blocks == []
