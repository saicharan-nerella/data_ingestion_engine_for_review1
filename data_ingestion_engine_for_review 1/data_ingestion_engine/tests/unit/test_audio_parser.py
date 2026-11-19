"""
Unit tests for AudioParser (Requirements 7.1, 7.2, 7.3).
"""
from unittest.mock import patch, MagicMock

import pytest

from parser.audio_parser import AudioParser


def make_mock_audio(length=120.5, sample_rate=44100, channels=2, codec_class="MP3"):
    mock_info = MagicMock()
    mock_info.length = length
    mock_info.sample_rate = sample_rate
    mock_info.channels = channels
    mock_audio = MagicMock()
    mock_audio.info = mock_info
    mock_audio.__class__.__name__ = codec_class
    return mock_audio


@pytest.fixture
def parser():
    return AudioParser(config={})


class TestAudioParserSuccess:
    def test_valid_audio_returns_single_metadata_block(self, parser):
        mock_audio = make_mock_audio()
        with patch("parser.audio_parser.mutagen.File", return_value=mock_audio):
            result = parser.parse("test.mp3", b"fake-audio-bytes")
        assert len(result) == 1
        assert result[0]["type"] == "metadata"

    def test_metadata_block_has_all_required_properties(self, parser):
        mock_audio = make_mock_audio(length=60.0, sample_rate=48000, channels=1, codec_class="FLAC")
        with patch("parser.audio_parser.mutagen.File", return_value=mock_audio):
            result = parser.parse("test.flac", b"fake-audio-bytes")
        props = result[0]["properties"]
        assert props["duration_seconds"] == 60.0
        assert props["sample_rate"] == 48000
        assert props["channels"] == 1
        assert props["codec"] == "FLAC"

    def test_transcript_is_none_and_transcription_required_is_true(self, parser):
        mock_audio = make_mock_audio()
        with patch("parser.audio_parser.mutagen.File", return_value=mock_audio):
            result = parser.parse("test.mp3", b"fake-audio-bytes")
        props = result[0]["properties"]
        assert props["transcript"] is None
        assert props["transcription_required"] is True


class TestAudioParserFailure:
    def test_mutagen_returns_none_yields_empty_list(self, parser):
        with patch("parser.audio_parser.mutagen.File", return_value=None):
            result = parser.parse("unknown.bin", b"not-audio")
        assert result == []

    def test_mutagen_raises_exception_yields_empty_list(self, parser):
        with patch("parser.audio_parser.mutagen.File", side_effect=Exception("decode error")):
            result = parser.parse("corrupt.mp3", b"corrupt-bytes")
        assert result == []
