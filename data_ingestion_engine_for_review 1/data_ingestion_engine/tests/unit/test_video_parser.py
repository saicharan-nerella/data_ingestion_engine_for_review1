"""
Unit tests for VideoParser (Requirements 8.1, 8.2, 8.3).
"""
from unittest.mock import patch, MagicMock

import pytest

from parser.video_parser import VideoParser


def make_mock_media_info(duration_ms=5000, frame_rate=25.0, width=1920, height=1080, codec="avc1"):
    mock_track = MagicMock()
    mock_track.track_type = "Video"
    mock_track.duration = duration_ms
    mock_track.frame_rate = str(frame_rate)
    mock_track.width = width
    mock_track.height = height
    mock_track.codec_id = codec
    mock_track.format = codec
    mock_info = MagicMock()
    mock_info.tracks = [mock_track]
    return mock_info


@pytest.fixture
def parser():
    return VideoParser(config={})


class TestVideoParserSuccess:
    def test_valid_video_returns_single_metadata_block(self, parser):
        mock_info = make_mock_media_info()
        with patch("parser.video_parser.pymediainfo.MediaInfo.parse", return_value=mock_info):
            result = parser.parse("test.mp4", b"fake-video-bytes")
        assert len(result) == 1
        assert result[0]["type"] == "metadata"

    def test_metadata_block_has_all_required_properties(self, parser):
        mock_info = make_mock_media_info(
            duration_ms=10000, frame_rate=30.0, width=1280, height=720, codec="hev1"
        )
        with patch("parser.video_parser.pymediainfo.MediaInfo.parse", return_value=mock_info):
            result = parser.parse("test.mkv", b"fake-video-bytes")
        props = result[0]["properties"]
        assert props["duration_seconds"] == pytest.approx(10.0)
        assert props["frame_rate"] == pytest.approx(30.0)
        assert props["width"] == 1280
        assert props["height"] == 720
        assert props["codec"] == "hev1"

    def test_transcript_is_none_and_transcription_required_is_true(self, parser):
        mock_info = make_mock_media_info()
        with patch("parser.video_parser.pymediainfo.MediaInfo.parse", return_value=mock_info):
            result = parser.parse("test.mp4", b"fake-video-bytes")
        props = result[0]["properties"]
        assert props["transcript"] is None
        assert props["transcription_required"] is True


class TestVideoParserFailure:
    def test_no_video_track_returns_empty_list(self, parser):
        mock_info = MagicMock()
        mock_info.tracks = []
        with patch("parser.video_parser.pymediainfo.MediaInfo.parse", return_value=mock_info):
            result = parser.parse("audio_only.mp4", b"fake-bytes")
        assert result == []

    def test_exception_during_parse_returns_empty_list(self, parser):
        with patch(
            "parser.video_parser.pymediainfo.MediaInfo.parse",
            side_effect=Exception("decode error"),
        ):
            result = parser.parse("corrupt.mp4", b"corrupt-bytes")
        assert result == []
