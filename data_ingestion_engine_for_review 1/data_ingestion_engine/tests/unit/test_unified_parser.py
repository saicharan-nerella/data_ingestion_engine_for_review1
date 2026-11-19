"""
Unit tests for parser/unified_parser.py (UnifiedParser).

Uses unittest.mock.patch to inject a fake 'unstructured_reader_plugin' layer
into REGISTRY so no real plugin classes need to be importable.
"""
import pytest
from unittest.mock import patch, MagicMock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_PLUGIN_CLASS_PATH = "tests.unit.test_unified_parser._FakePlugin"

# A minimal plugin class that UnifiedParser can instantiate via importlib.
class _FakePlugin:
    def __init__(self, config: dict):
        self.config = config

    def extract(self, object_key: str, content: bytes) -> dict:
        return {
            "source_uri":   f"s3://{object_key}",
            "format":       "unstructured_text",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "content":      [{"type": "text", "content": "hello"}],
        }


FAKE_REGISTRY_LAYER = {
    "unstructured_text":  "tests.unit.test_unified_parser._FakePlugin",
    "unstructured_pdf":   "tests.unit.test_unified_parser._FakePlugin",
    "unstructured_image": "tests.unit.test_unified_parser._FakePlugin",
}

PATCHED_REGISTRY = {"unstructured_reader_plugin": FAKE_REGISTRY_LAYER}


def _make_parser(enabled_readers: list):
    """Instantiate UnifiedParser with a patched REGISTRY."""
    with patch("parser.unified_parser.REGISTRY", PATCHED_REGISTRY):
        from parser.unified_parser import UnifiedParser
        return UnifiedParser({"enabled_readers": enabled_readers})


# ---------------------------------------------------------------------------
# Plugin loading
# ---------------------------------------------------------------------------

class TestPluginLoading:
    def test_unknown_reader_id_raises_key_error(self):
        """Req 3a.1 — unknown identifier must raise KeyError immediately."""
        with patch("parser.unified_parser.REGISTRY", PATCHED_REGISTRY):
            from parser.unified_parser import UnifiedParser
            with pytest.raises(KeyError, match="nonexistent_reader"):
                UnifiedParser({"enabled_readers": ["nonexistent_reader"]})

    def test_plugins_keys_equal_enabled_readers(self):
        """Req 3a.2 — _plugins must contain exactly the enabled readers."""
        enabled = ["unstructured_text", "unstructured_pdf"]
        parser = _make_parser(enabled)
        assert set(parser._plugins.keys()) == set(enabled)

    def test_empty_enabled_readers_loads_no_plugins(self):
        """Edge case — empty list results in no plugins loaded."""
        parser = _make_parser([])
        assert parser._plugins == {}

    def test_single_enabled_reader_loaded(self):
        """Single reader in enabled list is loaded correctly."""
        parser = _make_parser(["unstructured_image"])
        assert "unstructured_image" in parser._plugins

    def test_all_three_readers_loaded(self):
        """All three fake readers can be loaded simultaneously."""
        enabled = ["unstructured_text", "unstructured_pdf", "unstructured_image"]
        parser = _make_parser(enabled)
        assert len(parser._plugins) == 3


# ---------------------------------------------------------------------------
# extract() — error paths
# ---------------------------------------------------------------------------

class TestExtractErrorPaths:
    def test_unknown_format_returns_error_record(self):
        """Req 3a.4 — undetectable format returns error record."""
        parser = _make_parser(["unstructured_text"])
        # .xyz has no extension mapping and no magic bytes → 'unknown'
        record = parser.extract("file.xyz", b"\x00\x01\x02\x03")
        assert record["content"] is None
        assert "error" in record
        assert record["error"]  # non-empty string

    def test_error_record_has_all_top_level_keys(self):
        """Req 3a.5 — error record must contain source_uri, format, extracted_at, content."""
        parser = _make_parser(["unstructured_text"])
        record = parser.extract("mystery.xyz", b"\xde\xad\xbe\xef")
        for key in ("source_uri", "format", "extracted_at", "content"):
            assert key in record, f"Missing key: {key}"

    def test_no_matching_enabled_reader_returns_error_record(self):
        """Req 3a.6 — format detected but reader not in enabled list → error record."""
        # Only text is enabled; feed a PDF magic-byte file
        parser = _make_parser(["unstructured_text"])
        record = parser.extract("doc.pdf", b"%PDF-1.4 rest of content here")
        # FormatDetector returns 'unstructured_pdf', which is not enabled
        assert record["content"] is None
        assert "error" in record
        assert "unstructured_pdf" in record["error"]

    def test_error_record_source_uri_contains_object_key(self):
        """source_uri in error record must reference the object key."""
        parser = _make_parser([])
        record = parser.extract("bucket/path/file.xyz", b"")
        assert "bucket/path/file.xyz" in record["source_uri"]


# ---------------------------------------------------------------------------
# extract() — success path
# ---------------------------------------------------------------------------

class TestExtractSuccessPath:
    def test_delegates_to_correct_plugin(self):
        """Req 3a.3 — extract() delegates to the plugin matching the detected format."""
        parser = _make_parser(["unstructured_text"])
        mock_plugin = MagicMock()
        mock_plugin.extract.return_value = {
            "source_uri":   "s3://notes.txt",
            "format":       "unstructured_text",
            "extracted_at": "2024-01-01T00:00:00+00:00",
            "content":      [{"type": "text", "content": "hi"}],
        }
        parser._plugins["unstructured_text"] = mock_plugin

        parser.extract("notes.txt", b"hello world")

        mock_plugin.extract.assert_called_once_with("notes.txt", b"hello world")

    def test_returned_record_has_all_top_level_keys(self):
        """Req 3a.5 — successful record must contain source_uri, format, extracted_at, content."""
        parser = _make_parser(["unstructured_text"])
        record = parser.extract("notes.txt", b"hello world")
        for key in ("source_uri", "format", "extracted_at", "content"):
            assert key in record, f"Missing key: {key}"

    def test_top_level_fields_merged_when_missing(self):
        """Req 3a.5 — UnifiedParser fills in missing top-level fields via setdefault."""
        parser = _make_parser(["unstructured_text"])
        # Plugin returns a record without the top-level fields
        mock_plugin = MagicMock()
        mock_plugin.extract.return_value = {"content": []}
        parser._plugins["unstructured_text"] = mock_plugin

        record = parser.extract("notes.txt", b"data")
        assert "source_uri" in record
        assert "format" in record
        assert "extracted_at" in record

    def test_plugin_provided_fields_not_overwritten(self):
        """setdefault must not overwrite values already set by the plugin."""
        parser = _make_parser(["unstructured_text"])
        mock_plugin = MagicMock()
        mock_plugin.extract.return_value = {
            "source_uri":   "s3://custom-uri",
            "format":       "custom_format",
            "extracted_at": "2000-01-01T00:00:00+00:00",
            "content":      [],
        }
        parser._plugins["unstructured_text"] = mock_plugin

        record = parser.extract("notes.txt", b"data")
        assert record["source_uri"] == "s3://custom-uri"
        assert record["format"] == "custom_format"
        assert record["extracted_at"] == "2000-01-01T00:00:00+00:00"


# ---------------------------------------------------------------------------
# No-op methods
# ---------------------------------------------------------------------------

class TestNoOps:
    def test_connect_is_noop(self):
        parser = _make_parser([])
        assert parser.connect() is None

    def test_close_is_noop(self):
        parser = _make_parser([])
        assert parser.close() is None

    def test_read_is_noop(self):
        parser = _make_parser([])
        assert parser.read() is None
