from services.time_utils import (
    DEFAULT_DISPLAY_FORMAT,
    DEFAULT_DISPLAY_TIMEZONE,
    format_cell_value,
    format_timestamp_for_display,
    load_time_settings,
    normalize_storage_timestamp,
)


def test_normalize_storage_timestamp_uses_canonical_utc_seconds():
    assert normalize_storage_timestamp("2026-04-05T12:34:56.999999+00:00") == "2026-04-05T12:34:56Z"


def test_format_timestamp_for_berlin_display_includes_seconds():
    settings = load_time_settings(
        {
            "time": {
                "display_timezone": "Europe/Berlin",
                "display_format": "%d.%m.%Y %H:%M:%S",
            }
        }
    )
    assert format_timestamp_for_display("2026-01-05T12:34:56Z", settings) == "05.01.2026 13:34:56"


def test_format_timestamp_detail_can_include_utc():
    settings = load_time_settings({"time": {}})
    rendered = format_timestamp_for_display("2026-01-05T12:34:56Z", settings, include_utc=True)
    assert "05.01.2026 13:34:56" in rendered
    assert "2026-01-05 12:34:56 UTC" in rendered


def test_load_time_settings_defaults_when_missing():
    settings = load_time_settings({})
    assert settings.display_timezone == DEFAULT_DISPLAY_TIMEZONE
    assert settings.display_format == DEFAULT_DISPLAY_FORMAT
    assert settings.ntp_enabled is False


def test_load_time_settings_partial_ntp_config():
    settings = load_time_settings({"time": {"ntp": {"enabled": True}}})
    assert settings.ntp_enabled is True
    assert settings.ntp_servers == []
    assert settings.ntp_timeout_seconds == 2.0


def test_format_cell_value_formats_timestamp_columns():
    settings = load_time_settings({})
    rendered = format_cell_value("timestamp_utc", "2026-01-05T12:34:56Z", settings)
    assert "05.01.2026 13:34:56" in rendered
