from services.config_validation import ConfigValidator


def test_config_validator_accepts_valid_config(sample_config_dict):
    result = ConfigValidator().validate(sample_config_dict)
    assert result.is_valid is True
    assert result.error_count == 0


def test_config_validator_flags_missing_host_and_bad_interval(sample_config_dict):
    sample_config_dict["polling"]["interval_seconds"] = 0
    sample_config_dict["devices"]["kostal"]["host"] = ""
    result = ConfigValidator().validate(sample_config_dict)
    assert result.is_valid is False
    messages = [finding.message for finding in result.findings]
    assert any("Polling interval" in message for message in messages)
    assert any("missing host" in message.lower() for message in messages)


def test_config_validator_flags_missing_basic_auth_password(sample_config_dict):
    sample_config_dict["devices"]["cfos"]["auth"]["password"] = ""
    result = ConfigValidator().validate(sample_config_dict)
    assert result.warning_count >= 1
    assert any("Basic auth selected" in finding.message for finding in result.findings)


def test_config_validator_flags_enabled_ntp_without_servers(sample_config_dict):
    sample_config_dict["time"]["ntp"]["enabled"] = True
    sample_config_dict["time"]["ntp"]["servers"] = []
    result = ConfigValidator().validate(sample_config_dict)
    assert any("no NTP servers are configured" in finding.message for finding in result.findings)


def test_config_validator_flags_cfos_enabled_protocol_without_host(sample_config_dict):
    sample_config_dict["devices"]["cfos"]["protocols"]["mqtt"]["enabled"] = True
    sample_config_dict["devices"]["cfos"]["protocols"]["mqtt"]["host"] = ""
    result = ConfigValidator().validate(sample_config_dict)
    assert any("cFos mqtt diagnostics are enabled but host is missing" in finding.message for finding in result.findings)


def test_config_validator_flags_invalid_kostal_protocol(sample_config_dict):
    sample_config_dict["devices"]["kostal"]["protocol"] = "http"
    result = ConfigValidator().validate(sample_config_dict)
    assert any("KOSTAL protocol must be modbus_tcp or sunspec_tcp" in finding.message for finding in result.findings)
