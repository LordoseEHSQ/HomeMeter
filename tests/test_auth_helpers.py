from app import sanitize_config
from services.auth import mask_secret, summarize_cfos_auth, summarize_kostal_auth


class DummyFinding:
    def __init__(self, message: str, key_path: str) -> None:
        self.message = message
        self.key_path = key_path


def test_mask_secret_masks_non_empty_values():
    assert mask_secret("secret") == "***masked***"
    assert mask_secret("") == "not configured"


def test_summarize_cfos_auth_reports_success_without_leaking_password():
    summary = summarize_cfos_auth(
        {"enabled": True, "type": "basic", "credential_source": "custom", "username": "admin", "password": "secret"},
        "healthy",
        [DummyFinding("Basic auth selected", "devices.cfos.auth")],
    )
    assert summary["state"] == "succeeded"
    assert summary["masked"]["password"] == "***masked***"


def test_summarize_cfos_auth_reports_default_variant_and_security_warning():
    summary = summarize_cfos_auth(
        {
            "enabled": True,
            "type": "basic",
            "credential_source": "default_auto",
            "default_username": "admin",
            "default_password_variants": ["", "1234abcd"],
        },
        "healthy",
        [],
        {
            "auth_test_result": "succeeded",
            "auth_variant_used": "admin + 1234abcd",
            "credentials_source": "default",
            "security_warning": "Documented cFos default credentials still work. Change them to custom credentials.",
        },
    )
    assert summary["credentials_source"] == "default"
    assert summary["worked_variant"] == "admin + 1234abcd"
    assert summary["security_warning"] is not None


def test_summarize_kostal_auth_distinguishes_web_context_from_transport():
    summary = summarize_kostal_auth(
        {
            "enabled": True,
            "role": "installer",
            "web_access": {"enabled": True, "installer_service_code": "123456"},
            "transport": {"uses_auth": False},
        },
        "mapping_incomplete",
        "modbus_tcp",
    )
    assert summary["role"] == "installer"
    assert summary["state"] == "possibly_only_relevant_for_web_settings_access"
    assert summary["transport_auth_state"] == "not_used_by_current_protocol"
    assert summary["masked"]["installer_service_code"] == "***masked***"


def test_sanitize_config_redacts_role_specific_secrets():
    sanitized = sanitize_config(
        {
            "auth": {
                "default_password_variants": ["", "1234abcd"],
                "plant_owner_password": "owner-secret",
                "installer_service_code": "service-secret",
                "installer_master_key": "master-secret",
            }
        }
    )
    assert sanitized["auth"]["default_password_variants"] == "***masked***"
    assert sanitized["auth"]["plant_owner_password"] == "***masked***"
    assert sanitized["auth"]["installer_service_code"] == "***masked***"
    assert sanitized["auth"]["installer_master_key"] == "***masked***"
