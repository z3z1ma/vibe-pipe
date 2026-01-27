"""Basic tests to verify pytest setup."""

from vibe_piper import __version__


def test_version_exists() -> None:
    """Test that version is defined."""
    assert __version__ == "0.1.0"
