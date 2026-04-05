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

    def get(self, url: str, timeout=None, headers=None, auth=None):  # noqa: ANN001
        self.requested_urls.append(url)
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
            "auth": {"type": "basic", "username": "admin", "password": "secret"},
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


def test_cfos_collector_keeps_raw_numeric_fallback_when_mapping_unknown():
    collector = build_collector()
    parsed = collector.parse_payload('{"foo": {"bar": 12.5}}', FakeResponse(200, '{"foo":{"bar":12.5}}'))

    measurements, details = collector.normalize_payload(parsed, '{"foo":{"bar":12.5}}')

    assert any(measurement.metric_name == "raw::foo.bar" for measurement in measurements)
    assert details["measurement_visibility"] == "raw_only"
