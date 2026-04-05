from __future__ import annotations

import requests

from collectors.cfos import CfosCollector


class FakeResponse:
    def __init__(self, status_code: int, text: str, headers: dict[str, str] | None = None, url: str = "") -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {"Content-Type": "application/json"}
        self.url = url

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            response.url = self.url
            raise requests.exceptions.HTTPError(f"{self.status_code} error", response=response)


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = responses
        self.requested_urls: list[str] = []
        self.auth_variants: list[tuple[str | None, str | None]] = []

    def get(self, url: str, timeout=None, headers=None, auth=None):  # noqa: ANN001
        self.requested_urls.append(url)
        if auth is not None:
            self.auth_variants.append((getattr(auth, "username", None), getattr(auth, "password", None)))
        else:
            self.auth_variants.append((None, None))
        response = self._responses.pop(0)
        response.url = url
        return response


def build_collector() -> CfosCollector:
    return CfosCollector(
        device_name="cfos",
        config={
            "base_url": "http://cfos.local",
            "status_path": "/",
            "candidate_status_paths": ["/", "/status", "/api/status"],
            "auth": {
                "enabled": True,
                "type": "basic",
                "credential_source": "custom",
                "default_username": "admin",
                "default_password_variants": ["", "1234abcd"],
                "username": "admin",
                "password": "secret",
            },
            "timeout_seconds": 5,
        },
        default_connect_timeout=2,
        default_read_timeout=4,
    )


def test_cfos_collector_tries_candidate_paths_until_success(monkeypatch):
    collector = build_collector()
    fake_session = FakeSession(
        [
            FakeResponse(404, "not found", {"Content-Type": "text/plain"}),
            FakeResponse(200, '{"charger":{"power":7200}}'),
        ]
    )
    monkeypatch.setattr(collector, "get_session", lambda: fake_session)

    response = collector.perform_request()

    assert response.status_code == 200
    assert fake_session.requested_urls == ["http://cfos.local/", "http://cfos.local/status"]
    assert collector._last_request_meta["selected_path"] == "/status"
    assert collector._last_request_meta["auth_variant_used"] == "custom credentials"


def test_cfos_collector_skips_html_ui_and_uses_data_endpoint(monkeypatch):
    collector = build_collector()
    fake_session = FakeSession(
        [
            FakeResponse(200, "<!DOCTYPE html><html></html>", {"Content-Type": "text/html"}),
            FakeResponse(404, "", {"Content-Type": "text/plain"}),
            FakeResponse(404, "", {"Content-Type": "text/plain"}),
            FakeResponse(200, '{"params":{"grid_pwr":2355,"cons_evse_power":0}}'),
        ]
    )
    monkeypatch.setattr(collector, "get_session", lambda: fake_session)

    response = collector.perform_request()

    assert response.status_code == 200
    assert collector._last_request_meta["selected_path"] == "/cnf?cmd=get_dev_info"
    assert "/" in collector._last_request_meta["html_only_paths"]


def test_cfos_collector_parses_querystring_payload():
    collector = build_collector()

    parsed = collector.parse_payload(
        "charging_power=7000&grid_power=-2300&max_current=16",
        FakeResponse(200, "", {"Content-Type": "text/plain"}),
    )
    measurements, details = collector.normalize_payload(parsed, "charging_power=7000")

    metric_names = {measurement.metric_name for measurement in measurements}
    assert "wallbox_power_w" in metric_names
    assert "grid_power_w" in metric_names
    assert "max_current_a" in metric_names
    assert details["payload_format"] == "querystring"


def test_cfos_collector_maps_get_dev_info_power_fields():
    collector = build_collector()
    raw_payload = '{"params":{"grid_pwr":2355,"cons_pwr":1980,"cons_evse_power":7200,"avail_evse_power":19481,"surplus_power":0}}'
    parsed = collector.parse_payload(raw_payload, FakeResponse(200, raw_payload))

    measurements, _details = collector.normalize_payload(parsed, raw_payload)

    values = {measurement.metric_name: measurement.metric_value for measurement in measurements}
    assert values["grid_power_w"] == 2355
    assert values["house_power_w"] == 1980
    assert values["wallbox_power_w"] == 7200
    assert values["available_evse_power_w"] == 19481


def test_cfos_collector_parses_line_pairs_and_nested_settings():
    collector = build_collector()
    raw_payload = '{"charger": {"power": 11000, "current": 16}, "settings": {"max_current": 20}}'
    parsed = collector.parse_payload(raw_payload, FakeResponse(200, raw_payload))
    measurements, details = collector.normalize_payload(parsed, raw_payload)

    values = {measurement.metric_name: measurement.metric_value for measurement in measurements}
    assert values["wallbox_power_w"] == 11000
    assert values["current_a"] == 16
    assert values["max_current_a"] == 20
    assert details["settings_visibility"] == "partial"
    assert "wallbox_power_w" in details["confirmed_metric_names"]


def test_cfos_collector_keeps_raw_numeric_fallback_when_mapping_unknown():
    collector = build_collector()
    parsed = collector.parse_payload('{"foo": {"bar": 12.5}}', FakeResponse(200, '{"foo":{"bar":12.5}}'))

    measurements, details = collector.normalize_payload(parsed, '{"foo":{"bar":12.5}}')

    assert any(measurement.metric_name == "raw::foo.bar" for measurement in measurements)
    assert details["measurement_visibility"] == "raw_only"


def test_cfos_collector_categorizes_likely_useful_and_unmapped_numeric_fields():
    collector = build_collector()
    raw_payload = '{"meter":{"charge_rate_pct":87,"mystery_value":17},"status":{"state":2}}'
    parsed = collector.parse_payload(raw_payload, FakeResponse(200, raw_payload))

    measurements, details = collector.normalize_payload(parsed, raw_payload)

    assert any(measurement.metric_name == "candidate::meter.charge_rate_pct" for measurement in measurements)
    assert any(measurement.metric_name == "candidate::meter.mystery_value" for measurement in measurements)
    assert any(measurement.metric_name == "raw::status.state" for measurement in measurements)
    assert details["likely_useful_candidates_preview"][0]["field"] == "meter.charge_rate_pct"
    assert any(item["field"] == "meter.mystery_value" for item in details["likely_useful_candidates_preview"])
    assert any(item["field"] == "status.state" for item in details["unmapped_numeric_fields_preview"])


def test_cfos_collector_can_try_documented_default_credentials(monkeypatch):
    collector = build_collector()
    collector.config["auth"]["credential_source"] = "default_auto"
    collector.config["auth"]["username"] = ""
    collector.config["auth"]["password"] = ""
    fake_session = FakeSession(
        [
            FakeResponse(401, "unauthorized", {"Content-Type": "text/plain"}),
            FakeResponse(200, '{"charger":{"power":3600}}'),
        ]
    )
    monkeypatch.setattr(collector, "get_session", lambda: fake_session)

    response = collector.perform_request()

    assert response.status_code == 200
    assert fake_session.auth_variants == [("admin", ""), ("admin", "1234abcd")]
    assert collector._last_request_meta["auth_variant_used"] == "admin + 1234abcd"
    assert collector._last_request_meta["credentials_source"] == "default"
    assert collector._last_request_meta["security_warning"] is not None
