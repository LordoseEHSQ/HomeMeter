def test_dashboard_route_renders(client):
    response = client.get("/")
    assert response.status_code == 200
    assert b"Operational health at a glance" in response.data


def test_settings_route_renders(client):
    response = client.get("/settings")
    assert response.status_code == 200
    assert b"Settings and device operations" in response.data


def test_database_route_renders(client):
    response = client.get("/database")
    assert response.status_code == 200
    assert b"Database inspection" in response.data


def test_run_diagnostics_action_redirects(client):
    response = client.post("/actions/reload-config", follow_redirects=False)
    assert response.status_code == 302


def test_settings_time_route_renders(client):
    response = client.get("/settings/time")
    assert response.status_code == 200
    assert b"Time settings and diagnostics" in response.data
