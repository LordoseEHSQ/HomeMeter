def test_dashboard_route_renders(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b"Operational health at a glance" in response.data
    assert b"Current priorities and next checks" in response.data
    assert b"Timing summary only" in response.data
    assert b"Short device summary" in response.data
    assert b"Deutsch" in response.data
    assert b"English" in response.data


def test_settings_route_renders(client):
    response = client.get("/settings")
    assert response.status_code == 200
    assert b"Bearbeitbare Einstellungen" in response.data
    assert b"Normale Operatoren sollen Werte andern konnen" in response.data
    assert b"cFos bearbeiten" in response.data
    assert b"Live-Refresh und Polling" in response.data
    assert b"Aufbewahrung Rohdaten" in response.data
    assert b"Cleanup aktiv" in response.data
    assert b"Raw Config" in response.data


def test_settings_devices_route_renders_clean_device_blocks(client):
    response = client.get("/settings/devices")
    assert response.status_code == 200
    assert b"settings-device-card" in response.data
    assert b"cFos-Protokollflachen" in response.data
    assert b"Read-only-Betriebszustand" in response.data


def test_database_route_renders(client):
    response = client.get("/database")
    assert response.status_code == 200
    assert b"Database inspection" in response.data
    assert b"cleanup_runs" in response.data


def test_system_route_exposes_timing_behavior(client):
    response = client.get("/system")
    assert response.status_code == 200
    assert b"Polling- und Persistenzverhalten" in response.data
    assert b"Letzte KPI-Zusammenfassung" in response.data
    assert b"Letzter erfolgreicher Cleanup" in response.data
    assert b"Retention cleanup history" in response.data
    assert b"Local startup and duplicate-process notes" in response.data


def test_analytics_route_renders(client):
    response = client.get("/analytics")
    assert response.status_code == 200
    assert b"Energie- und KPI-Dashboard" in response.data
    assert b"data-refresh-seconds=\"30\"" in response.data
    assert b"Live-Signale" in response.data


def test_analytics_partial_route_renders(client):
    response = client.get("/analytics/partial")
    assert response.status_code == 200
    assert b"Energieuberblick" in response.data
    assert b"Zeitreihen und Verlauf" in response.data


def test_run_diagnostics_action_redirects(client):
    response = client.post("/actions/reload-config", follow_redirects=False)
    assert response.status_code == 302


def test_settings_time_route_renders(client):
    response = client.get("/settings/time")
    assert response.status_code == 200
    assert b"Time settings and diagnostics" in response.data
    assert b"Polling- und Persistenzverhalten" in response.data
    assert b"Recent cleanup runs" in response.data


def test_device_detail_route_renders(client):
    response = client.get("/devices/cfos")
    assert response.status_code == 200
    assert b"Current device state" in response.data
    assert b"Latest stored values" in response.data
    assert b"cFos Protocol Surface Diagnostics" in response.data
    assert b"Raw payloads and deep technical detail" in response.data


def test_save_settings_action_redirects(client):
    response = client.post(
        "/actions/save-settings/cfos",
        data={"base_url": "http://192.168.50.139", "status_path": "/status"},
        follow_redirects=False,
    )
    assert response.status_code == 302


def test_save_settings_workflow_persists_without_touching_json(client):
    response = client.post(
        "/actions/save-settings/time",
        data={"display_timezone": "UTC", "display_format": "%Y-%m-%d %H:%M:%S"},
        follow_redirects=True,
    )
    assert response.status_code == 200
    assert b"Saved time settings to config.yaml." in response.data
    follow_up = client.get("/settings")
    assert b'value="UTC"' in follow_up.data


def test_save_timing_settings_action_redirects(client):
    response = client.post(
        "/actions/save-settings/timing",
        data={
            "analytics_refresh_interval_seconds": "20",
            "poll_interval_seconds": "11",
            "raw_write_interval_seconds": "30",
            "derived_write_interval_seconds": "40",
            "rollup_interval_seconds": "60",
            "retention_days_raw": "14",
            "retention_days_rollup": "90",
            "persistence_enabled__present": "1",
            "persistence_enabled": "true",
            "live_refresh_enabled__present": "1",
            "cleanup_enabled__present": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302


def test_language_switch_route_changes_visible_navigation_language(client):
    response = client.post("/actions/set-language/en", follow_redirects=True)
    assert response.status_code == 200
    assert b"Language" in response.data
    assert b"Settings / Devices" in response.data


def test_analytics_route_switches_to_english(client):
    client.post("/actions/set-language/en", follow_redirects=False)
    response = client.get("/analytics")
    assert response.status_code == 200
    assert b"Energy and KPI dashboard" in response.data
    assert b"Time series and trends" in response.data
