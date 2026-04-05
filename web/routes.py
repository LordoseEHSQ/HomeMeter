from __future__ import annotations

from flask import Blueprint, abort, current_app, flash, redirect, render_template, request, session, url_for

from services.i18n import resolve_language

from services.config_editor import update_config_from_form
from services.gap_detection import build_integration_gaps


def register_routes(app) -> None:
    blueprint = Blueprint("web", __name__)

    @blueprint.route("/")
    def dashboard():
        store = current_app.config["STORE"]
        current_language = resolve_language(session.get("ui_language"))
        validation_result = current_app.config["BUILD_VALIDATION_RESULT"]()
        device_specs = current_app.config["BUILD_DEVICE_SPECS"]()
        device_operations = current_app.config["BUILD_DEVICE_OPERATIONS"](validation_result, device_specs)
        database_report = current_app.config["DB_INSPECTOR"].build_report()
        system_health = current_app.config["HEALTH_SERVICE"].build_summary(
            device_operations,
            validation_result,
            database_report,
        )
        dashboard_data = store.get_dashboard_data(current_app.config["DEVICE_NAMES"])
        integration_gaps = build_integration_gaps(store, device_operations, device_specs)
        return render_template(
            "dashboard.html",
            dashboard=dashboard_data,
            system_health=system_health,
            device_operations=device_operations,
            integration_gaps=integration_gaps,
            analytics=current_app.config["ANALYTICS_SERVICE"].build_dashboard(
                current_app.config["ANALYTICS_SETTINGS"].get("default_window", "24h"),
                language=current_language,
                time_settings=current_app.config["TIME_SETTINGS"],
            ),
            time_settings=current_app.config["TIME_SETTINGS"],
        )

    @blueprint.route("/analytics")
    def analytics():
        current_language = resolve_language(session.get("ui_language"))
        selected_window = request.args.get("window", current_app.config["ANALYTICS_SETTINGS"].get("default_window", "24h"))
        analytics_view = current_app.config["ANALYTICS_SERVICE"].build_dashboard(
            selected_window,
            language=current_language,
            time_settings=current_app.config["TIME_SETTINGS"],
        )
        return render_template(
            "analytics.html",
            analytics=analytics_view,
            time_settings=current_app.config["TIME_SETTINGS"],
        )

    @blueprint.route("/analytics/partial")
    def analytics_partial():
        current_language = resolve_language(session.get("ui_language"))
        selected_window = request.args.get("window", current_app.config["ANALYTICS_SETTINGS"].get("default_window", "24h"))
        analytics_view = current_app.config["ANALYTICS_SERVICE"].build_dashboard(
            selected_window,
            language=current_language,
            time_settings=current_app.config["TIME_SETTINGS"],
        )
        return render_template(
            "partials/analytics_live.html",
            analytics=analytics_view,
            time_settings=current_app.config["TIME_SETTINGS"],
        )

    @blueprint.route("/devices/<device_name>")
    def device_detail(device_name: str):
        store = current_app.config["STORE"]
        known_devices = {"cfos", "easee", "kostal"}
        if device_name not in known_devices:
            abort(404)
        detail = store.get_device_detail(device_name)
        validation_result = current_app.config["BUILD_VALIDATION_RESULT"]()
        device_specs = current_app.config["BUILD_DEVICE_SPECS"]()
        operations = current_app.config["BUILD_DEVICE_OPERATIONS"](validation_result, device_specs)
        operation = next(item for item in operations if item["name"] == device_name)
        return render_template(
            "device_detail.html",
            device=detail,
            operation=operation,
            device_specs=device_specs.get(device_name, {}),
            cfos_protocol_statuses=(
                current_app.config["CFOS_PROTOCOL_DIAGNOSTICS"].describe(
                    (current_app.config["CONFIG"].get("devices", {}) or {}).get("cfos", {}) or {},
                    store,
                )
                if device_name == "cfos"
                else []
            ),
            time_settings=current_app.config["TIME_SETTINGS"],
        )

    @blueprint.route("/alerts")
    def alerts():
        store = current_app.config["STORE"]
        alert_rows = store.get_recent_alerts(limit=100)
        return render_template("alerts.html", alerts=alert_rows)

    @blueprint.route("/settings")
    def settings():
        store = current_app.config["STORE"]
        validation_result = current_app.config["BUILD_VALIDATION_RESULT"]()
        device_specs = current_app.config["BUILD_DEVICE_SPECS"]()
        device_operations = current_app.config["BUILD_DEVICE_OPERATIONS"](validation_result, device_specs)
        integration_gaps = build_integration_gaps(store, device_operations, device_specs)
        config_data = current_app.config["SANITIZE_CONFIG"](current_app.config["CONFIG"])
        return render_template(
            "settings.html",
            device_operations=device_operations,
            config_validation=validation_result,
            config_data=config_data,
            time_settings=current_app.config["TIME_SETTINGS"],
            time_status=current_app.config["TIME_MONITOR"].get_status(),
            device_specs=device_specs,
            cfos_protocol_statuses=current_app.config["CFOS_PROTOCOL_DIAGNOSTICS"].describe(
                (current_app.config["CONFIG"].get("devices", {}) or {}).get("cfos", {}) or {},
                current_app.config["STORE"],
            ),
            integration_gaps=integration_gaps,
        )

    @blueprint.route("/system")
    def system_status():
        store = current_app.config["STORE"]
        validation_result = current_app.config["BUILD_VALIDATION_RESULT"]()
        device_specs = current_app.config["BUILD_DEVICE_SPECS"]()
        device_operations = current_app.config["BUILD_DEVICE_OPERATIONS"](validation_result, device_specs)
        database_report = current_app.config["DB_INSPECTOR"].build_report()
        system_health = current_app.config["HEALTH_SERVICE"].build_summary(
            device_operations,
            validation_result,
            database_report,
        )
        integration_gaps = build_integration_gaps(store, device_operations, device_specs)
        return render_template(
            "system_status.html",
            system_health=system_health,
            device_operations=device_operations,
            database_report=database_report,
            config_validation=validation_result,
            time_settings=current_app.config["TIME_SETTINGS"],
            time_status=current_app.config["TIME_MONITOR"].get_status(),
            integration_gaps=integration_gaps,
        )

    @blueprint.route("/config-health")
    def config_health():
        validation_result = current_app.config["BUILD_VALIDATION_RESULT"]()
        return render_template(
            "config_health.html",
            config_validation=validation_result,
            config_data=current_app.config["SANITIZE_CONFIG"](current_app.config["CONFIG"]),
            time_settings=current_app.config["TIME_SETTINGS"],
        )

    @blueprint.route("/database")
    def database_inspection():
        database_report = current_app.config["DB_INSPECTOR"].build_report()
        return render_template(
            "database.html",
            database_report=database_report,
            time_settings=current_app.config["TIME_SETTINGS"],
        )

    @blueprint.route("/settings/devices")
    def settings_devices():
        return settings()

    @blueprint.route("/settings/database")
    def settings_database():
        return database_inspection()

    @blueprint.route("/settings/config-health")
    def settings_config_health():
        return config_health()

    @blueprint.route("/settings/time")
    def settings_time():
        validation_result = current_app.config["BUILD_VALIDATION_RESULT"]()
        return render_template(
            "time_settings.html",
            time_settings=current_app.config["TIME_SETTINGS"],
            time_status=current_app.config["TIME_MONITOR"].get_status(),
            config_validation=validation_result,
        )

    @blueprint.post("/actions/reload-config")
    def reload_config():
        try:
            current_app.config["REFRESH_RUNTIME_CONFIG"]()
            flash("Configuration reloaded from config.yaml.", "success")
        except Exception as exc:
            flash(f"Failed to reload config: {exc}", "error")
        return redirect(request.referrer or url_for("web.settings"))

    @blueprint.post("/actions/save-settings/<scope>")
    def save_settings(scope: str):
        try:
            updated = update_config_from_form(
                current_app.config["CONFIG"],
                scope=scope,
                form={key: value for key, value in request.form.items()},
            )
            current_app.config["PERSIST_RUNTIME_CONFIG"](updated)
            flash(f"Saved {scope} settings to config.yaml.", "success")
        except Exception as exc:
            flash(f"Failed to save {scope} settings: {exc}", "error")
        return redirect(request.referrer or url_for("web.settings"))

    @blueprint.post("/actions/run-diagnostics")
    def run_diagnostics():
        polling_manager = current_app.config["POLLING_MANAGER"]
        alerts = polling_manager.run_cycle_once()
        flash(f"Diagnostics cycle executed. Alerts generated: {len(alerts)}.", "success")
        return redirect(request.referrer or url_for("web.system_status"))

    @blueprint.post("/actions/test-connection/<device_name>")
    def test_connection(device_name: str):
        polling_manager = current_app.config["POLLING_MANAGER"]
        result = polling_manager.test_device(device_name)
        if result is None:
            abort(404)
        flash(
            f"Tested {device_name}: {result.status.value}"
            + (f" ({result.error_message})" if result.error_message else ""),
            "success" if result.success else "warning",
        )
        return redirect(request.referrer or url_for("web.settings"))

    @blueprint.post("/actions/check-time")
    def check_time():
        status = current_app.config["TIME_MONITOR"].run_check()
        flash(
            f"Reference time check: {status['status']}"
            + (f" ({status['last_error']})" if status.get("last_error") else ""),
            "success" if status["status"] in {"healthy", "warning"} else "warning",
        )
        return redirect(request.referrer or url_for("web.system_status"))

    @blueprint.post("/actions/set-language/<language>")
    def set_language(language: str):
        session["ui_language"] = resolve_language(language)
        flash(f"UI language set to {session['ui_language']}.", "success")
        return redirect(request.referrer or url_for("web.dashboard"))

    @blueprint.post("/actions/test-protocol/<device_name>/<surface>")
    def test_protocol(device_name: str, surface: str):
        if device_name != "cfos":
            abort(404)
        cfos_config = (current_app.config["CONFIG"].get("devices", {}) or {}).get("cfos", {}) or {}
        status = current_app.config["CFOS_PROTOCOL_DIAGNOSTICS"].test_surface(
            surface=surface,
            cfos_config=cfos_config,
            store=current_app.config["STORE"],
        )
        flash(
            f"cFos {surface} diagnostics: "
            f"configured={status['configured']}, reachable={status['reachable']}, implementation={status['implementation_state']}"
            + (f" ({status['last_error']})" if status.get("last_error") else ""),
            "success" if status.get("reachable") else "warning",
        )
        return redirect(request.referrer or url_for("web.device_detail", device_name="cfos"))

    app.register_blueprint(blueprint)
