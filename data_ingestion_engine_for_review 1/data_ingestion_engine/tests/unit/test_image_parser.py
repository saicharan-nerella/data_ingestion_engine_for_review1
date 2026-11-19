"""
Unit tests for ImageParser.
"""
import base64
import io

import pytest
from PIL import Image

from parser.image_parser import ImageParser


def make_png_bytes(width=10, height=10, mode="RGB"):
    buf = io.BytesIO()
    Image.new(mode, (width, height), color=(255, 0, 0)).save(buf, format="PNG")
    return buf.getvalue()


def make_jpeg_bytes(width=8, height=8, mode="RGB"):
    buf = io.BytesIO()
    Image.new(mode, (width, height), color=(0, 128, 255)).save(buf, format="JPEG")
    return buf.getvalue()


@pytest.fixture
def parser():
    return ImageParser(config={})


class TestImageParserValidPNG:
    def test_returns_two_blocks(self, parser):
        blocks = parser.parse("test.png", make_png_bytes())
        assert len(blocks) == 2

    def test_first_block_is_image_type(self, parser):
        blocks = parser.parse("test.png", make_png_bytes())
        assert blocks[0]["type"] == "image"

    def test_second_block_is_metadata_type(self, parser):
        blocks = parser.parse("test.png", make_png_bytes())
        assert blocks[1]["type"] == "metadata"

    def test_image_block_has_data_base64(self, parser):
        content = make_png_bytes()
        blocks = parser.parse("test.png", content)
        assert "data_base64" in blocks[0]
        assert blocks[0]["data_base64"] == base64.b64encode(content).decode("ascii")

    def test_image_block_has_mime_type(self, parser):
        blocks = parser.parse("test.png", make_png_bytes())
        assert blocks[0]["mime_type"] == "image/png"

    def test_metadata_block_has_width_and_height(self, parser):
        blocks = parser.parse("test.png", make_png_bytes(width=20, height=15))
        props = blocks[1]["properties"]
        assert props["width"] == 20
        assert props["height"] == 15

    def test_metadata_block_has_color_mode(self, parser):
        blocks = parser.parse("test.png", make_png_bytes(mode="RGB"))
        assert blocks[1]["properties"]["color_mode"] == "RGB"

    def test_metadata_block_has_format_name(self, parser):
        blocks = parser.parse("test.png", make_png_bytes())
        assert blocks[1]["properties"]["format_name"] == "PNG"


class TestImageParserValidJPEG:
    def test_returns_two_blocks(self, parser):
        blocks = parser.parse("test.jpg", make_jpeg_bytes())
        assert len(blocks) == 2

    def test_mime_type_is_jpeg(self, parser):
        blocks = parser.parse("test.jpg", make_jpeg_bytes())
        assert blocks[0]["mime_type"] == "image/jpeg"

    def test_format_name_is_jpeg(self, parser):
        blocks = parser.parse("test.jpg", make_jpeg_bytes())
        assert blocks[1]["properties"]["format_name"] == "JPEG"


class TestImageParserInvalidInput:
    def test_invalid_bytes_returns_empty_list(self, parser):
        assert parser.parse("bad.png", b"not an image") == []

    def test_empty_bytes_returns_empty_list(self, parser):
        assert parser.parse("empty.png", b"") == []

    def test_truncated_bytes_returns_empty_list(self, parser):
        truncated = make_png_bytes()[:20]
        assert parser.parse("truncated.png", truncated) == []
