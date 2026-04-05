def test_dashboard_route_renders(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b"Operational health at a glance" in response.data
    assert b"Deutsch" in response.data
    assert b"English" in response.data


def test_settings_route_renders(client):
    response = client.get("/settings")
    assert response.status_code == 200
    assert b"Einstellungen und Geraetebetrieb" in response.data
    assert b"Devices and operational state" not in response.data
    assert b"cFos bearbeiten" in response.data


def test_settings_devices_route_renders_clean_device_blocks(client):
    response = client.get("/settings/devices")
    assert response.status_code == 200
    assert b"settings-device-card" in response.data
    assert b"cFos-Protokollflachen" in response.data


def test_database_route_renders(client):
    response = client.get("/database")
    assert response.status_code == 200
    assert b"Database inspection" in response.data


def test_analytics_route_renders(client):
    response = client.get("/analytics")
    assert response.status_code == 200
    assert b"Energie- und KPI-Dashboard" in response.data
    assert b"data-refresh-seconds=\"30\"" in response.data


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


def test_device_detail_route_renders(client):
    response = client.get("/devices/cfos")
    assert response.status_code == 200
    assert b"cFos Protocol Surface Diagnostics" in response.data
    assert b"Auth And Recording Context" in response.data


def test_save_settings_action_redirects(client):
    response = client.post(
        "/actions/save-settings/cfos",
        data={"base_url": "http://192.168.50.139", "status_path": "/status"},
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
