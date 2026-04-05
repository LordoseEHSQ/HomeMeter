from services.database_stats import DatabaseInspector

from tests.conftest import insert_poll_result


def test_database_initialization_creates_empty_tables(store):
    report = DatabaseInspector(store).build_report()
    assert report["file_exists"] is True
    assert report["tables"]["measurements"]["row_count"] == 0
    assert report["tables"]["poll_events"]["row_count"] == 0
    assert report["tables"]["alerts"]["row_count"] == 0


def test_database_report_reflects_stored_rows(store):
    insert_poll_result(store, device_name="cfos")
    store.save_alert(
        timestamp_utc="2026-04-05T12:00:01+00:00",
        device_name="cfos",
        severity="medium",
        rule_name="test_rule",
        message="Test alert",
        context={"ok": False},
    )
    report = DatabaseInspector(store).build_report()
    assert report["tables"]["measurements"]["row_count"] == 1
    assert report["tables"]["poll_events"]["row_count"] == 1
    assert report["tables"]["alerts"]["row_count"] == 1
    assert report["storage_activity"]["status"] == "healthy"
