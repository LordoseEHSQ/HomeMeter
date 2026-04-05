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
        self._last_request_meta: dict[str, Any] = {
            "attempted_paths": [],
            "selected_path": None,
            "selected_url": None,
            "attempted_auth_variants": [],
            "auth_variant_used": None,
            "credentials_source": "unknown",
            "auth_tested": False,
            "auth_test_result": "not_tested",
            "security_warning": None,
        }

    def perform_request(self) -> requests.Response:
        base_url = str(self.config.get("base_url", "")).rstrip("/")
        if not base_url:
            raise ValueError("cFos collector missing base_url in config.")

        auth_config = self.config.get("auth", {}) or {}
        auth_type = str(auth_config.get("type", "none")).lower()
        headers: dict[str, str] = {"Accept": "application/json, text/plain;q=0.9, */*;q=0.5"}

        session = self.get_session()
        attempted_paths: list[str] = []
        attempted_auth_variants: list[str] = []
        last_http_error: requests.exceptions.HTTPError | None = None
        last_response: requests.Response | None = None
        html_only_paths: list[str] = []
        auth_variants = self._auth_variants(auth_config, auth_type)

        for variant in auth_variants:
            attempted_auth_variants.append(str(variant["label"]))
            variant_headers = dict(headers)
            auth = None
            if variant["type"] == "basic":
                auth = HTTPBasicAuth(str(variant["username"]), str(variant["password"]))
            elif variant["type"] == "bearer" and variant.get("token"):
                variant_headers["Authorization"] = f"Bearer {variant['token']}"

            for status_path in self._candidate_paths():
                attempted_paths.append(f"{variant['label']}::{status_path}")
                url = self._build_url(base_url, status_path)
                response = session.get(url, timeout=self.build_timeout(), headers=variant_headers, auth=auth)
                last_response = response
                if response.status_code in {401, 403}:
                    last_http_error = requests.exceptions.HTTPError(
                        f"{response.status_code} auth error",
                        response=self._response_to_requests_response(response),
                    )
                    break
                if response.status_code == 404:
                    continue
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as exc:
                    last_http_error = exc
                    continue
                if self._looks_like_html_ui(response):
                    html_only_paths.append(status_path)
                    continue

                self._last_request_meta = {
                    "attempted_paths": attempted_paths,
                    "selected_path": status_path,
                    "selected_url": url,
                    "attempted_auth_variants": attempted_auth_variants,
                    "auth_variant_used": variant["label"],
                    "credentials_source": variant["source"],
                    "auth_tested": variant["type"] != "none",
                    "auth_test_result": "succeeded" if variant["type"] != "none" else "not_required_or_unknown",
                    "security_warning": (
                        "Documented cFos default credentials still work. Change them to custom credentials."
                        if variant["source"] == "default"
                        else None
                    ),
                    "html_only_paths": html_only_paths,
                }
                return response

        self._last_request_meta = {
            "attempted_paths": attempted_paths,
            "selected_path": None,
            "selected_url": None,
            "attempted_auth_variants": attempted_auth_variants,
            "auth_variant_used": None,
            "credentials_source": auth_config.get("credential_source", "unknown"),
            "auth_tested": auth_type in {"basic", "bearer"},
            "auth_test_result": "failed" if auth_type in {"basic", "bearer"} else "not_required_or_unknown",
            "security_warning": None,
            "html_only_paths": html_only_paths,
        }
        if last_http_error is not None:
            raise last_http_error
        if html_only_paths:
            raise ValueError(
                "cFos returned HTML frontend pages but no usable data endpoint response. "
                f"HTML-only paths seen: {', '.join(html_only_paths)}"
            )
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
            "attempted_auth_variants": self._last_request_meta.get("attempted_auth_variants", []),
            "auth_variant_used": self._last_request_meta.get("auth_variant_used"),
            "credentials_source": self._last_request_meta.get("credentials_source"),
            "auth_tested": self._last_request_meta.get("auth_tested", False),
            "auth_test_result": self._last_request_meta.get("auth_test_result", "not_tested"),
            "security_warning": self._last_request_meta.get("security_warning"),
            "html_only_paths": self._last_request_meta.get("html_only_paths", []),
        }

        if not isinstance(parsed_payload, dict):
            raise ValueError("cFos response could not be normalized; expected JSON object.")

        flat_payload = self._flatten_payload(parsed_payload)
        numeric_pairs = {key: value for key, value in flat_payload.items() if isinstance(value, (int, float))}
        used_keys: set[str] = set()
        details["numeric_field_count"] = len(numeric_pairs)
        details["payload_format"] = str(parsed_payload.get("_payload_format", "json"))
        details["visible_sections"] = self._visible_sections(flat_payload)
        details["settings_keys_found"] = self._settings_keys(flat_payload)
        details["device_info"] = self._device_info(parsed_payload, flat_payload)

        for metric_name, field_names in self._known_candidates().items():
            value, matched_key = self._extract_number(flat_payload, field_names)
            if value is not None:
                if matched_key:
                    used_keys.add(matched_key)
                measurements.append(
                    MeasurementRecord(
                        metric_name=metric_name,
                        metric_value=value,
                        unit=self._default_unit(metric_name),
                        source_type="confirmed_useful",
                        raw_payload=raw_payload,
                    )
                )

        likely_candidates = self._likely_useful_candidates(numeric_pairs, used_keys)
        for key, value in likely_candidates[:12]:
            measurements.append(
                MeasurementRecord(
                    metric_name=f"candidate::{key}",
                    metric_value=value,
                    unit=self._guess_unit(key),
                    source_type="likely_useful_candidate",
                    raw_payload=raw_payload,
                )
            )
            used_keys.add(key)

        unmapped_candidates = [(key, value) for key, value in numeric_pairs.items() if key not in used_keys]
        details["confirmed_metric_names"] = [measurement.metric_name for measurement in measurements if measurement.source_type == "confirmed_useful"]
        details["likely_useful_candidates_preview"] = [
            {"field": key, "value": value, "unit_guess": self._guess_unit(key)}
            for key, value in likely_candidates[:12]
        ]
        details["unmapped_numeric_fields_preview"] = [{"field": key, "value": value} for key, value in unmapped_candidates[:12]]
        details["todo"] = (
            "cFos HTTP parsing and normalization are still best-effort. Confirm the real endpoint and "
            "field semantics against your device before treating settings or wallbox metrics as fully trusted."
        )
        details["settings_visibility"] = "partial" if details["settings_keys_found"] else "unknown"
        details["measurement_visibility"] = "partial" if measurements else "raw_only"
        for key, value in unmapped_candidates[:12]:
            measurements.append(
                MeasurementRecord(
                    metric_name=f"raw::{key}",
                    metric_value=value,
                    unit=None,
                    source_type="unmapped_numeric",
                    raw_payload=raw_payload,
                )
            )
        return measurements, details

    def _candidate_paths(self) -> list[str]:
        configured = self.config.get("candidate_status_paths", []) or []
        candidates: list[str] = []
        status_path = str(self.config.get("status_path", "/") or "/")
        builtin_candidates = [
            "/cnf?cmd=get_dev_info",
            "/cnf?cmd=get_dev_info&fmt=json",
            "/status",
            "/api/status",
        ]
        for path in [status_path, *configured, *builtin_candidates]:
            normalized = self._normalize_path(path)
            if normalized not in candidates:
                candidates.append(normalized)
        return candidates or ["/"]

    def _auth_variants(self, auth_config: dict[str, Any], auth_type: str) -> list[dict[str, Any]]:
        enabled = bool(auth_config.get("enabled", auth_type != "none"))
        if not enabled or auth_type == "none":
            return [{"type": "none", "label": "no_auth", "source": "not_required_or_unknown"}]
        if auth_type == "bearer":
            return [
                {
                    "type": "bearer",
                    "label": "custom_bearer",
                    "source": "custom",
                    "token": str(auth_config.get("token", "") or ""),
                }
            ]

        credential_source = str(auth_config.get("credential_source", "custom")).lower()
        if credential_source == "default_auto":
            default_username = str(auth_config.get("default_username", "admin") or "admin")
            passwords = auth_config.get("default_password_variants", ["", "1234abcd"]) or ["", "1234abcd"]
            variants: list[dict[str, Any]] = []
            for password in passwords:
                password_text = str(password or "")
                label = (
                    "admin + empty password"
                    if default_username == "admin" and password_text == ""
                    else "admin + 1234abcd"
                    if default_username == "admin" and password_text == "1234abcd"
                    else f"{default_username} + custom default variant"
                )
                variants.append(
                    {
                        "type": "basic",
                        "label": label,
                        "source": "default",
                        "username": default_username,
                        "password": password_text,
                    }
                )
            return variants

        return [
            {
                "type": "basic",
                "label": "custom credentials",
                "source": "custom",
                "username": str(auth_config.get("username", "") or ""),
                "password": str(auth_config.get("password", "") or ""),
            }
        ]

    def _response_to_requests_response(self, response: Any) -> requests.Response:
        if isinstance(response, requests.Response):
            return response
        mapped = requests.Response()
        mapped.status_code = int(getattr(response, "status_code", 0) or 0)
        mapped.url = str(getattr(response, "url", ""))
        return mapped

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
            "params.grid_pwr",
        ],
        "house_power_w": [
            "house_power",
            "housePower",
            "home_power",
            "load_power",
            "house.power",
            "measurements.house_power",
            "params.cons_pwr",
        ],
        "wallbox_power_w": [
            "charging_power",
            "charger_power",
            "wallbox_power",
            "evse.power",
            "charger.power",
            "measurements.charging_power",
            "power",
            "params.cons_evse_power",
            "devices[0].power_w",
        ],
        "available_evse_power_w": ["params.avail_evse_power"],
        "surplus_power_w": ["params.surplus_power"],
        "error_power_w": ["params.error_pwr"],
        "pv_power_w": ["pv_power", "pvPower", "solar_power", "pv.power", "measurements.pv_power"],
        "current_a": ["current", "amps", "current_a", "charger.current", "evse.current"],
        "voltage_v": ["voltage", "voltage_v", "charger.voltage", "evse.voltage"],
            "energy_wh": ["energy", "energy_wh", "charged_energy", "session.energy_wh"],
            "max_current_a": ["max_current", "maxCurrent", "settings.max_current", "config.max_current"],
            "min_current_a": ["min_current", "minCurrent", "settings.min_current", "config.min_current"],
        }

    def _extract_number(self, payload: dict[str, Any], candidates: list[str]) -> tuple[float | None, str | None]:
        normalized = {self._normalize_key(key): value for key, value in payload.items()}
        for candidate in candidates:
            candidate_key = self._normalize_key(candidate)
            if candidate_key in normalized:
                try:
                    return float(normalized[candidate_key]), candidate_key
                except (TypeError, ValueError):
                    LOGGER.warning("cFos field %s exists but is not numeric.", candidate)
            suffix_match = next(
                (
                    (key, value)
                    for key, value in normalized.items()
                    if key.endswith(f".{candidate_key}") or key.endswith(f"::{candidate_key}") or key.endswith(candidate_key)
                ),
                None,
            )
            if suffix_match is not None:
                try:
                    return float(suffix_match[1]), suffix_match[0]
                except (TypeError, ValueError):
                    LOGGER.warning("cFos field %s exists but is not numeric.", candidate)
        return None, None

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

    def _device_info(self, parsed_payload: dict[str, Any], flat_payload: dict[str, Any]) -> dict[str, Any]:
        info_keys = ("name", "model", "serial", "firmware", "version", "dev_info")
        info: dict[str, Any] = {}
        for key, value in parsed_payload.items():
            if key.startswith("_"):
                continue
            if isinstance(value, str) and any(hint in key.lower() for hint in info_keys):
                info[key] = value
        for key, value in flat_payload.items():
            if isinstance(value, str) and any(hint in key.lower() for hint in info_keys):
                info[key] = value
        return info

    def _likely_useful_candidates(self, numeric_pairs: dict[str, float], used_keys: set[str]) -> list[tuple[str, float]]:
        hints = ("power", "energy", "current", "curr", "amp", "volt", "meter", "charge", "evse", "charger")
        candidates = []
        for key, value in numeric_pairs.items():
            if key in used_keys:
                continue
            if any(hint in key.lower() for hint in hints):
                candidates.append((key, value))
        return candidates

    def _guess_unit(self, key: str) -> str | None:
        lowered = key.lower()
        if "power" in lowered:
            return "W"
        if "energy" in lowered:
            return "Wh"
        if any(token in lowered for token in ("current", "curr", "amp")):
            return "A"
        if "volt" in lowered:
            return "V"
        return None

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

    def _looks_like_html_ui(self, response: requests.Response) -> bool:
        content_type = response.headers.get("Content-Type", "").lower()
        text = response.text.lstrip()[:200].lower()
        return "text/html" in content_type or text.startswith("<!doctype html") or text.startswith("<html")
