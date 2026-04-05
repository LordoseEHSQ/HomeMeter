from pathlib import Path

import yaml

from services.config_editor import save_config, update_config_from_form


def test_update_config_from_form_updates_cfos_auth_and_paths(sample_config_dict):
    updated = update_config_from_form(
        sample_config_dict,
        scope="cfos",
        form={
            "base_url": "http://192.168.50.139",
            "status_path": "/cnf?cmd=get_dev_info",
            "timeout_seconds": "7",
            "auth_enabled": "true",
            "auth_type": "basic",
            "credential_source": "default_auto",
            "default_username": "admin",
            "default_password_variants": "\n1234abcd",
            "candidate_status_paths": "/status\n/api/status",
        },
    )

    cfos = updated["devices"]["cfos"]
    assert cfos["base_url"] == "http://192.168.50.139"
    assert cfos["status_path"] == "/cnf?cmd=get_dev_info"
    assert cfos["timeout_seconds"] == 7
    assert cfos["candidate_status_paths"] == ["/status", "/api/status"]
    assert cfos["auth"]["credential_source"] == "default_auto"
    assert cfos["auth"]["password"] == ""
    assert cfos["auth"]["default_password_variants"] == ["", "1234abcd"]


def test_update_config_from_form_updates_kostal_role_aware_settings(sample_config_dict):
    updated = update_config_from_form(
        sample_config_dict,
        scope="kostal",
        form={
            "host": "192.168.1.17",
            "port": "1502",
            "protocol": "sunspec_tcp",
            "unit_id": "71",
            "modbus_byte_order": "CDAB",
            "sunspec_byte_order": "ABCD",
            "timeout_seconds": "8",
            "auth_enabled": "true",
            "role": "installer",
            "web_access_enabled": "true",
            "installer_service_code": "svc-code",
            "transport_uses_auth": "true",
            "transport_username": "modbus-user",
            "transport_password": "modbus-pass",
        },
    )

    kostal = updated["devices"]["kostal"]
    assert kostal["host"] == "192.168.1.17"
    assert kostal["protocol"] == "sunspec_tcp"
    assert kostal["timeout_seconds"] == 8
    assert kostal["auth"]["role"] == "installer"
    assert kostal["auth"]["web_access"]["installer_service_code"] == "svc-code"
    assert kostal["auth"]["transport"]["uses_auth"] is True
    assert kostal["auth"]["transport"]["username"] == "modbus-user"


def test_save_config_writes_yaml(tmp_path: Path, sample_config_dict):
    target = tmp_path / "config.yaml"
    save_config(str(target), sample_config_dict)

    loaded = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert loaded["devices"]["cfos"]["auth"]["username"] == "admin"


def test_update_config_from_form_updates_analytics_settings(sample_config_dict):
    updated = update_config_from_form(
        sample_config_dict,
        scope="analytics",
        form={"default_window": "7d", "chart_refresh_seconds": "45", "rollup_retention_days": "365"},
    )
    assert updated["analytics"]["default_window"] == "7d"
    assert updated["analytics"]["chart_refresh_seconds"] == 45
    assert updated["analytics"]["rollup_retention_days"] == 365
