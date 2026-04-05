from __future__ import annotations

from typing import Any


SECRET_KEYS = {
    "password",
    "token",
    "default_password_variants",
    "plant_owner_password",
    "installer_service_code",
    "installer_master_key",
    "master_key",
    "service_code",
}


def mask_secret(value: Any) -> str:
    if not value:
        return "not configured"
    return "***masked***"


def summarize_cfos_auth(
    auth: dict[str, Any],
    collector_status: str,
    findings: list[Any],
    poll_details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    poll_details = poll_details or {}
    enabled = bool(auth.get("enabled", str(auth.get("type", "none")).lower() != "none"))
    auth_type = str(auth.get("type", "none")).lower()
    credential_source = str(auth.get("credential_source", "custom")).lower()
    username = str(auth.get("username", "") or "")
    password = str(auth.get("password", "") or "")
    token = str(auth.get("token", "") or "")
    configured = (
        (auth_type == "basic" and credential_source == "custom" and bool(username and password is not None))
        or (auth_type == "basic" and credential_source == "default_auto")
        or (auth_type == "bearer" and bool(token))
    )
    partial = (auth_type == "basic" and bool(username or password) and not bool(username and password)) or (
        auth_type == "bearer" and not token and enabled
    )
    if not enabled or auth_type == "none":
        state = "not_required_or_unknown"
    elif collector_status == "auth_failed":
        state = "failed"
    elif collector_status in {"healthy", "reachable"} and configured:
        state = "succeeded"
    elif partial:
        state = "partial"
    elif configured:
        state = "configured"
    else:
        state = "missing"
    if configured and collector_status == "never_polled":
        state = "untested"
    auth_test_result = str(poll_details.get("auth_test_result", "untested" if configured else "not_tested"))
    worked_variant = poll_details.get("auth_variant_used")
    worked_source = poll_details.get("credentials_source", credential_source)
    security_warning = poll_details.get("security_warning")
    return {
        "enabled": enabled,
        "auth_type": auth_type,
        "configured": configured,
        "partial": partial,
        "config_state": "configured" if configured else "partial" if partial else "missing" if enabled else "not_required_or_unknown",
        "state": state,
        "credentials_source": worked_source if worked_variant else credential_source,
        "auth_test_result": auth_test_result,
        "worked_variant": worked_variant,
        "security_warning": security_warning,
        "masked": {
            "username": (
                str(auth.get("default_username", "admin") or "admin")
                if credential_source == "default_auto"
                else username or "not configured"
            ),
            "password": mask_secret(password if credential_source == "custom" else "default variants configured"),
            "token": mask_secret(token),
        },
        "notes": [finding.message for finding in findings if "auth" in finding.key_path or "credential" in finding.message.lower()],
    }


def summarize_kostal_auth(auth: dict[str, Any], collector_status: str, protocol: str) -> dict[str, Any]:
    enabled = bool(auth.get("enabled", False))
    role = str(auth.get("role", "plant_owner")).lower()
    web_access = auth.get("web_access", {}) or {}
    transport = auth.get("transport", {}) or {}
    web_enabled = bool(web_access.get("enabled", enabled))
    transport_uses_auth = bool(transport.get("uses_auth", False))
    plant_owner_password = str(web_access.get("plant_owner_password", "") or "")
    installer_service_code = str(web_access.get("installer_service_code", "") or "")
    installer_master_key = str(web_access.get("installer_master_key", "") or "")
    web_username = str(web_access.get("username", "") or "")
    web_password = str(web_access.get("password", "") or "")
    transport_username = str(transport.get("username", "") or "")
    transport_password = str(transport.get("password", "") or "")

    if role == "installer":
        web_configured = bool(installer_service_code or installer_master_key or (web_username and web_password))
        web_partial = bool(installer_service_code or installer_master_key or web_username or web_password) and not web_configured
    else:
        web_configured = bool(plant_owner_password or (web_username and web_password))
        web_partial = bool(plant_owner_password or web_username or web_password) and not web_configured

    transport_configured = bool(transport_username and transport_password) if transport_uses_auth else False
    transport_partial = transport_uses_auth and bool(transport_username or transport_password) and not transport_configured

    if not enabled:
        overall_state = "not_used_by_current_protocol"
    elif transport_uses_auth and collector_status == "auth_failed":
        overall_state = "failed"
    elif transport_uses_auth and collector_status in {"healthy", "reachable"} and transport_configured:
        overall_state = "succeeded"
    elif transport_uses_auth and transport_partial:
        overall_state = "partial"
    elif transport_uses_auth and transport_configured and collector_status == "never_polled":
        overall_state = "untested"
    elif transport_uses_auth and not transport_configured:
        overall_state = "missing"
    elif web_enabled and web_configured:
        overall_state = "possibly_only_relevant_for_web_settings_access"
    elif web_enabled and web_partial:
        overall_state = "partial"
    elif web_enabled and not web_configured:
        overall_state = "missing"
    else:
        overall_state = "not_used_by_current_protocol"

    return {
        "enabled": enabled,
        "role": role,
        "protocol": protocol,
        "web_access_enabled": web_enabled,
        "transport_uses_auth": transport_uses_auth,
        "config_state": "configured" if (web_configured or transport_configured) else "partial" if (web_partial or transport_partial) else "missing" if enabled else "not_used_by_current_protocol",
        "web_access_state": "configured" if web_configured else "partial" if web_partial else "missing" if web_enabled else "disabled",
        "transport_auth_state": (
            "configured"
            if transport_configured
            else "partial"
            if transport_partial
            else "missing"
            if transport_uses_auth
            else "not_used_by_current_protocol"
        ),
        "state": overall_state,
        "role_context_note": (
            "Current Modbus/SunSpec transport path does not automatically imply KOSTAL web/settings login usage."
        ),
        "masked": {
            "plant_owner_password": mask_secret(plant_owner_password),
            "installer_service_code": mask_secret(installer_service_code),
            "installer_master_key": mask_secret(installer_master_key),
            "web_username": web_username or "not configured",
            "web_password": mask_secret(web_password),
            "transport_username": transport_username or "not configured",
            "transport_password": mask_secret(transport_password),
        },
    }
