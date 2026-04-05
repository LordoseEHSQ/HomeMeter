from __future__ import annotations

import json
import logging
from urllib.parse import parse_qsl, urljoin
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from collectors.base import BaseCollector, MeasurementRecord

LOGGER = logging.getLogger(__name__)


class CfosCollector(BaseCollector):
    source_type = "cfos_http"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._last_request_meta: dict[str, Any] = {"attempted_paths": [], "selected_path": None, "selected_url": None}

    def perform_request(self) -> requests.Response:
        base_url = str(self.config.get("base_url", "")).rstrip("/")
        if not base_url:
            raise ValueError("cFos collector missing base_url in config.")

        auth_config = self.config.get("auth", {}) or {}
        auth_type = str(auth_config.get("type", "none")).lower()
        headers: dict[str, str] = {"Accept": "application/json, text/plain;q=0.9, */*;q=0.5"}
        auth = None
        if auth_type == "basic":
            auth = HTTPBasicAuth(
                str(auth_config.get("username", "")),
                str(auth_config.get("password", "")),
            )
        elif auth_type == "bearer" and auth_config.get("token"):
            headers["Authorization"] = f"Bearer {auth_config['token']}"

        session = self.get_session()
        attempted_paths: list[str] = []
        last_http_error: requests.exceptions.HTTPError | None = None
        last_response: requests.Response | None = None

        for status_path in self._candidate_paths():
            attempted_paths.append(status_path)
            url = self._build_url(base_url, status_path)
            response = session.get(url, timeout=self.build_timeout(), headers=headers, auth=auth)
            last_response = response
            if response.status_code in {401, 403}:
                response.raise_for_status()
            if response.status_code == 404:
                continue
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as exc:
                last_http_error = exc
                continue

            self._last_request_meta = {
                "attempted_paths": attempted_paths,
                "selected_path": status_path,
                "selected_url": url,
            }
            return response

        self._last_request_meta = {
            "attempted_paths": attempted_paths,
            "selected_path": None,
            "selected_url": None,
        }
        if last_http_error is not None:
            raise last_http_error
        if last_response is not None:
            last_response.raise_for_status()
        raise requests.exceptions.ConnectionError("No cFos HTTP endpoint returned a usable response.")

    def parse_payload(self, raw_payload: str, response: requests.Response | None) -> Any:
        content_type = response.headers.get("Content-Type", "") if response is not None else ""
        if "json" in content_type.lower() or raw_payload.strip().startswith(("{", "[")):
            try:
                parsed = json.loads(raw_payload)
                return self._normalize_parsed_container(parsed, payload_format="json")
            except json.JSONDecodeError as exc:
                raise ValueError(f"cFos JSON parse failure: {exc}") from exc
        querystring_payload = self._parse_querystring_payload(raw_payload)
        if querystring_payload:
            return self._normalize_parsed_container(querystring_payload, payload_format="querystring")
        line_payload = self._parse_line_payload(raw_payload)
        if line_payload:
            return self._normalize_parsed_container(line_payload, payload_format="line_pairs")
        LOGGER.warning("cFos returned non-JSON payload; storing raw payload only.")
        return {
            "_raw_text": raw_payload,
            "_todo": "Unknown cFos response format",
            "_payload_format": "raw_text",
        }

    def normalize_payload(
        self, parsed_payload: Any, raw_payload: str
    ) -> tuple[list[MeasurementRecord], dict[str, Any]]:
        measurements: list[MeasurementRecord] = []
        details: dict[str, Any] = {
            "mapping_status": "partial",
            "selected_path": self._last_request_meta.get("selected_path"),
            "selected_url": self._last_request_meta.get("selected_url"),
            "attempted_paths": self._last_request_meta.get("attempted_paths", []),
        }

        if not isinstance(parsed_payload, dict):
            raise ValueError("cFos response could not be normalized; expected JSON object.")

        flat_payload = self._flatten_payload(parsed_payload)
        numeric_pairs = {key: value for key, value in flat_payload.items() if isinstance(value, (int, float))}
        details["numeric_field_count"] = len(numeric_pairs)
        details["payload_format"] = str(parsed_payload.get("_payload_format", "json"))
        details["visible_sections"] = self._visible_sections(flat_payload)
        details["settings_keys_found"] = self._settings_keys(flat_payload)

        for metric_name, field_names in self._known_candidates().items():
            value = self._extract_number(flat_payload, field_names)
            if value is not None:
                measurements.append(
                    MeasurementRecord(
                        metric_name=metric_name,
                        metric_value=value,
                        unit=self._default_unit(metric_name),
                        raw_payload=raw_payload,
                    )
                )

        details["unmapped_numeric_fields_preview"] = [
            {"field": key, "value": value}
            for key, value in list(numeric_pairs.items())[:12]
            if not any(measurement.metric_name == f"raw::{key}" for measurement in measurements)
        ]
        details["todo"] = (
            "cFos HTTP parsing and normalization are still best-effort. Confirm the real endpoint and "
            "field semantics against your device before treating settings or wallbox metrics as fully trusted."
        )
        details["settings_visibility"] = "partial" if details["settings_keys_found"] else "unknown"
        details["measurement_visibility"] = "partial" if measurements else "raw_only"
        if not measurements and numeric_pairs:
            for key, value in list(numeric_pairs.items())[:12]:
                measurements.append(
                    MeasurementRecord(
                        metric_name=f"raw::{key}",
                        metric_value=value,
                        unit=None,
                        source_type="raw_numeric",
                        raw_payload=raw_payload,
                    )
                )
        return measurements, details

    def _candidate_paths(self) -> list[str]:
        configured = self.config.get("candidate_status_paths", []) or []
        candidates: list[str] = []
        status_path = str(self.config.get("status_path", "/") or "/")
        for path in [status_path, *configured]:
            normalized = self._normalize_path(path)
            if normalized not in candidates:
                candidates.append(normalized)
        return candidates or ["/"]

    def _build_url(self, base_url: str, path: str) -> str:
        normalized_path = self._normalize_path(path)
        return urljoin(f"{base_url}/", normalized_path.lstrip("/"))

    def _normalize_path(self, path: str) -> str:
        path = (path or "/").strip()
        if not path.startswith("/"):
            path = f"/{path}"
        return path

    def _normalize_parsed_container(self, parsed: Any, payload_format: str) -> dict[str, Any]:
        if isinstance(parsed, dict):
            enriched = dict(parsed)
            enriched.setdefault("_payload_format", payload_format)
            return enriched
        if isinstance(parsed, list):
            return {"items": parsed, "_payload_format": payload_format}
        raise ValueError("cFos response could not be normalized; expected a JSON object or list.")

    def _parse_querystring_payload(self, raw_payload: str) -> dict[str, Any]:
        parsed_pairs = parse_qsl(raw_payload, keep_blank_values=False)
        if not parsed_pairs:
            return {}
        result: dict[str, Any] = {}
        for key, value in parsed_pairs:
            result[key] = self._coerce_scalar(value)
        return result

    def _parse_line_payload(self, raw_payload: str) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for line in raw_payload.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith(("#", "//")):
                continue
            delimiter = ":" if ":" in stripped else "=" if "=" in stripped else None
            if delimiter is None:
                continue
            key, value = stripped.split(delimiter, 1)
            key = key.strip()
            if not key:
                continue
            result[key] = self._coerce_scalar(value.strip())
        return result

    def _coerce_scalar(self, value: Any) -> Any:
        if isinstance(value, str):
            lower = value.strip().lower()
            if lower in {"true", "on", "enabled"}:
                return 1.0
            if lower in {"false", "off", "disabled"}:
                return 0.0
            try:
                return float(value)
            except ValueError:
                return value
        return value

    def _known_candidates(self) -> dict[str, list[str]]:
        return {
            "grid_power_w": [
                "grid_power",
                "gridPower",
                "power_grid",
                "powerGrid",
                "grid.power",
                "measurements.grid_power",
            ],
            "house_power_w": [
                "house_power",
                "housePower",
                "home_power",
                "load_power",
                "house.power",
                "measurements.house_power",
            ],
            "wallbox_power_w": [
                "charging_power",
                "charger_power",
                "wallbox_power",
                "evse.power",
                "charger.power",
                "measurements.charging_power",
                "power",
            ],
            "pv_power_w": ["pv_power", "pvPower", "solar_power", "pv.power", "measurements.pv_power"],
            "current_a": ["current", "amps", "current_a", "charger.current", "evse.current"],
            "voltage_v": ["voltage", "voltage_v", "charger.voltage", "evse.voltage"],
            "energy_wh": ["energy", "energy_wh", "charged_energy", "session.energy_wh"],
            "max_current_a": ["max_current", "maxCurrent", "settings.max_current", "config.max_current"],
            "min_current_a": ["min_current", "minCurrent", "settings.min_current", "config.min_current"],
        }

    def _extract_number(self, payload: dict[str, Any], candidates: list[str]) -> float | None:
        normalized = {self._normalize_key(key): value for key, value in payload.items()}
        for candidate in candidates:
            candidate_key = self._normalize_key(candidate)
            if candidate_key in normalized:
                try:
                    return float(normalized[candidate_key])
                except (TypeError, ValueError):
                    LOGGER.warning("cFos field %s exists but is not numeric.", candidate)
            suffix_match = next(
                (value for key, value in normalized.items() if key.endswith(f".{candidate_key}") or key.endswith(f"::{candidate_key}")),
                None,
            )
            if suffix_match is not None:
                try:
                    return float(suffix_match)
                except (TypeError, ValueError):
                    LOGGER.warning("cFos field %s exists but is not numeric.", candidate)
        return None

    def _flatten_payload(self, value: Any, prefix: str = "") -> dict[str, Any]:
        flattened: dict[str, Any] = {}
        if isinstance(value, dict):
            for key, item in value.items():
                if key.startswith("_"):
                    continue
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                flattened.update(self._flatten_payload(item, next_prefix))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                next_prefix = f"{prefix}[{index}]"
                flattened.update(self._flatten_payload(item, next_prefix))
        elif prefix:
            flattened[prefix] = value
        return flattened

    def _visible_sections(self, flat_payload: dict[str, Any]) -> list[str]:
        sections = []
        for key in flat_payload:
            root = key.split(".", 1)[0].split("[", 1)[0]
            if root and root not in sections:
                sections.append(root)
        return sections[:12]

    def _settings_keys(self, flat_payload: dict[str, Any]) -> list[str]:
        hints = ("setting", "config", "limit", "max_current", "min_current", "phase", "mode")
        keys = [key for key in flat_payload if any(hint in key.lower() for hint in hints)]
        return keys[:12]

    def _normalize_key(self, key: str) -> str:
        return key.replace("[", ".").replace("]", "").replace(":", ".").lower()

    def _default_unit(self, metric_name: str) -> str | None:
        if metric_name.endswith("_w"):
            return "W"
        if metric_name.endswith("_a"):
            return "A"
        if metric_name.endswith("_v"):
            return "V"
        if metric_name.endswith("_wh"):
            return "Wh"
        return None
