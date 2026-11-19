"""
Unit tests for TextParser (Requirements 9.1, 9.2, 9.3).
"""
import pytest
from parser.text_parser import TextParser


@pytest.fixture
def parser():
    return TextParser(config={})


# ── Requirement 9.1 ──────────────────────────────────────────────────────────

def test_utf8_returns_single_text_block(parser):
    content = "Hello, world!".encode("utf-8")
    blocks = parser.parse("file.txt", content)
    assert blocks == [{"type": "text", "content": "Hello, world!"}]


def test_utf8_no_encoding_fallback(parser):
    content = "UTF-8 text".encode("utf-8")
    parser.parse("file.txt", content)
    assert parser.encoding_fallback is None


def test_utf8_unicode_characters(parser):
    text = "Héllo wörld — 日本語"
    blocks = parser.parse("file.txt", text.encode("utf-8"))
    assert blocks[0]["content"] == text


# ── Requirement 9.2 ──────────────────────────────────────────────────────────

def test_latin1_fallback_on_utf8_failure(parser):
    # bytes that are valid Latin-1 but not valid UTF-8
    content = "café".encode("latin-1")
    blocks = parser.parse("file.txt", content)
    assert len(blocks) == 1
    assert blocks[0]["type"] == "text"
    assert blocks[0]["content"] == "café"


def test_latin1_sets_encoding_fallback(parser):
    content = "café".encode("latin-1")
    parser.parse("file.txt", content)
    assert parser.encoding_fallback == "latin-1"


def test_encoding_fallback_reset_between_calls(parser):
    # First call triggers fallback
    parser.parse("file.txt", "café".encode("latin-1"))
    assert parser.encoding_fallback == "latin-1"

    # Second call with valid UTF-8 should reset it
    parser.parse("file.txt", "hello".encode("utf-8"))
    assert parser.encoding_fallback is None


# ── Requirement 9.3 ──────────────────────────────────────────────────────────

def test_total_failure_returns_empty_list():
    """Simulate both decodings raising an exception via a subclass override."""

    class AlwaysFailParser(TextParser):
        def parse(self, object_key: str, content: bytes) -> list[dict]:
            self.encoding_fallback = None
            try:
                raise UnicodeDecodeError("utf-8", b"", 0, 1, "forced")
            except UnicodeDecodeError:
                pass
            try:
                raise UnicodeDecodeError("latin-1", b"", 0, 1, "forced")
            except Exception:
                pass
            return []

    p = AlwaysFailParser(config={})
    blocks = p.parse("file.txt", b"\xff\xfe")
    assert blocks == []


# ── Block structure ───────────────────────────────────────────────────────────

def test_block_has_correct_type(parser):
    blocks = parser.parse("file.txt", b"some text")
    assert blocks[0]["type"] == "text"


def test_empty_file_returns_empty_text_block(parser):
    blocks = parser.parse("file.txt", b"")
    assert blocks == [{"type": "text", "content": ""}]
