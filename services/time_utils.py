from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_DISPLAY_TIMEZONE = "Europe/Berlin"
DEFAULT_DISPLAY_FORMAT = "%d.%m.%Y %H:%M:%S"


@dataclass(slots=True)
class TimeSettings:
    display_timezone: str = DEFAULT_DISPLAY_TIMEZONE
    display_format: str = DEFAULT_DISPLAY_FORMAT
    ntp_enabled: bool = False
    ntp_servers: list[str] | None = None
    ntp_timeout_seconds: float = 2.0
    drift_warning_seconds: float = 2.0


def utc_now_storage() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_storage_timestamp(value: str | datetime | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt_value = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    parsed = parse_utc_timestamp(value)
    if parsed is None:
        return value
    return parsed.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_time_settings(config: dict[str, Any]) -> TimeSettings:
    time_config = config.get("time", {}) or {}
    ntp_config = time_config.get("ntp", {}) or {}
    servers = ntp_config.get("servers", []) or []
    return TimeSettings(
        display_timezone=str(time_config.get("display_timezone", DEFAULT_DISPLAY_TIMEZONE)),
        display_format=str(time_config.get("display_format", DEFAULT_DISPLAY_FORMAT)),
        ntp_enabled=bool(ntp_config.get("enabled", False)),
        ntp_servers=[str(server) for server in servers],
        ntp_timeout_seconds=float(ntp_config.get("timeout_seconds", 2)),
        drift_warning_seconds=float(ntp_config.get("drift_warning_seconds", 2)),
    )


def format_timestamp_for_display(
    value: str | None,
    settings: TimeSettings,
    include_utc: bool = False,
) -> str:
    parsed = parse_utc_timestamp(value)
    if parsed is None:
        return value or "-"
    local_dt = convert_to_display_timezone(parsed, settings.display_timezone)
    local_value = local_dt.strftime(settings.display_format)
    if include_utc:
        utc_value = parsed.strftime("%Y-%m-%d %H:%M:%S UTC")
        return f"{local_value} ({utc_value})"
    return local_value


def format_cell_value(key: str, value: Any, settings: TimeSettings) -> str:
    if value is None:
        return "-"
    if "timestamp" in key.lower():
        return format_timestamp_for_display(str(value), settings=settings, include_utc=True)
    return str(value)


def _resolve_timezone(name: str):
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return timezone.utc  # type: ignore[return-value]


def convert_to_display_timezone(parsed: datetime, timezone_name: str) -> datetime:
    try:
        return parsed.astimezone(_resolve_timezone(timezone_name))
    except Exception:
        if timezone_name == "Europe/Berlin":
            return parsed.astimezone(_berlin_fallback_offset(parsed))
        return parsed.astimezone(timezone.utc)


def _berlin_fallback_offset(parsed: datetime) -> timezone:
    year = parsed.year
    dst_start = _last_sunday(year, 3).replace(hour=1, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    dst_end = _last_sunday(year, 10).replace(hour=1, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    offset_hours = 2 if dst_start <= parsed < dst_end else 1
    return timezone(timedelta(hours=offset_hours), name="Europe/Berlin")


def _last_sunday(year: int, month: int) -> datetime:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    return last_day - timedelta(days=(last_day.weekday() + 1) % 7)
