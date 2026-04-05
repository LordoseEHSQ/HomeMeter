# HomeMeter Diagnostics

HomeMeter Diagnostics is a local Flask application for validating home-energy measurements before you trust them for charging logic or control optimization. It is diagnostics-first: the app stores poll attempts, normalized values, raw payloads, alerts, and operational findings in SQLite, and it keeps uncertainty visible instead of hiding it.

## What is already implemented

- Local Flask web UI with dashboard, device detail, alerts, system status, config health, database inspection, device settings, and time diagnostics pages
- SQLite persistence for measurements, poll events, and alerts
- Background polling plus on-demand diagnostics actions
- UTC storage with centralized local-time display formatting
- Config validation and system-health summaries
- cFos HTTP collector with configurable credentials, candidate endpoint paths, raw-payload retention, and best-effort normalization
- cFos multi-surface diagnostics model for HTTP, MQTT, Modbus, and SunSpec visibility
- KOSTAL connectivity-first collector with protocol, unit ID, and byte-order specs modeled
- Gap detection for missing metrics, mapping gaps, partial integrations, and stale/implausible values
- pytest-based automated tests

## What remains partial

- Full real cFos protocol support is still incomplete. HTTP is the best-developed surface; MQTT, Modbus, and SunSpec remain preparatory only.
- cFos settings visibility is partial. The collector can surface likely setting-like fields from HTTP payloads, but it does not claim complete settings coverage.
- cFos wallbox / Wallbox Booster power mapping is still best-effort until real payload semantics are confirmed.
- KOSTAL register mapping is still incomplete. The app models connectivity, byte order, and decoding helpers, but it does not yet claim verified live register addresses or sign conventions.
- Easee remains a transparent skeleton integration.

## Current pages

- `/`: dashboard
- `/system`: combined health, storage, gap, and time summary
- `/settings`: device operations, protocol surfaces, config snapshot, and time diagnostics
- `/settings/devices`: alias for device operations
- `/settings/database`: alias for database inspection
- `/settings/config-health`: alias for config health
- `/settings/time`: dedicated time settings and reference-time diagnostics
- `/config-health`: validation findings
- `/database`: storage observability and recent rows
- `/alerts`: recent alerts
- `/devices/<device_name>`: detailed per-device diagnostics

## Core structure

- `main.py`: Flask entrypoint
- `app.py`: app factory, config loading, collector wiring, polling manager, filters
- `collectors/`: collector implementations and shared result contract
- `storage/sqlite_store.py`: SQLite schema and query helpers
- `analysis/plausibility.py`: plausibility rules and alert generation
- `services/`: config validation, device specs, cFos protocol diagnostics, time handling, KOSTAL decoding helpers, health summaries, and gap detection
- `templates/` and `static/`: Jinja templates and CSS
- `tests/`: pytest suite

## Time and timestamp handling

- Canonical storage is UTC, formatted as `YYYY-MM-DDTHH:MM:SSZ`
- UI display is localized centrally via a shared filter/helper
- Default display timezone is `Europe/Berlin`
- Default display format is `DD.MM.YYYY HH:MM:SS`
- Important views include seconds consistently
- Optional reference-time checks query configured NTP servers and estimate drift
- The app does not set the operating-system clock

## Configuration

Top-level sections expected in `config.yaml`:

- `app`
- `polling`
- `storage`
- `time`
- `devices`

Configured devices:

- `cfos`
- `easee`
- `kostal`

### cFos config state

The current cFos integration is HTTP-first and configurable:

- `base_url`
- `status_path`
- `candidate_status_paths`
- `auth.type`, `auth.username`, `auth.password`, `auth.token`
- `preferred_protocols`
- `protocols.http`
- `protocols.mqtt`
- `protocols.modbus`
- `protocols.sunspec`

The collector will try the configured HTTP path first, then any configured candidate paths, and it records raw payloads even when full mapping is not available.

### KOSTAL config state

The current KOSTAL integration models:

- `host`
- `port`
- `protocol` (`modbus_tcp` or `sunspec_tcp`)
- `unit_id`
- `modbus_byte_order`
- `sunspec_byte_order`

Connectivity is real. Register mapping is still explicitly partial.

## Windows setup

```powershell
py -3.13 -m pip install -r requirements.txt
Copy-Item config.example.yaml config.yaml
python main.py
```

Then open `http://127.0.0.1:5000`.

## Running tests

```powershell
py -3.13 -m pytest -q tests -p no:cacheprovider --basetemp=.pytest-tmp-run
```

The custom `--basetemp` path is recommended on this Windows setup because default temporary directories can hit restrictive permission behavior.

## Diagnostics states shown in the UI

- healthy
- reachable
- timeout
- unreachable
- auth missing
- auth failed
- parsing failed
- mapping incomplete
- unsupported response
- empty payload
- disabled
- config error

## cFos current integration truth

What is implemented now:

- configurable HTTP auth
- configurable candidate HTTP status paths
- JSON, query-string-style, and line-pair payload parsing
- best-effort normalization for likely power/current/energy/current-limit fields
- raw numeric fallback when fields are not yet confidently mapped
- visibility of cFos protocol surfaces in the UI

What is still not fully solved:

- full settings coverage
- confirmed wallbox-specific metric semantics for every field
- full MQTT / Modbus / SunSpec implementation
- guaranteed endpoint completeness across all cFos firmware/config variants

## KOSTAL current integration truth

What is implemented now:

- protocol/spec modeling for `modbus_tcp` and `sunspec_tcp`
- connectivity checks against the configured TCP endpoint
- explicit mapping-profile visibility in diagnostics
- byte-order-aware decoding helpers for future verified register reads

What is still open:

- verified live register map
- trusted power sign conventions
- full SunSpec model discovery and value extraction

## Database observability

The database page shows:

- SQLite path
- file size
- row counts per table
- oldest and newest timestamps per table
- latest rows from `measurements`
- latest rows from `poll_events`
- latest rows from `alerts`
- whether cFos or KOSTAL measurements exist
- whether raw payloads are being stored
- storage activity summary

## Known limitations

- The app is designed to stay runnable even when collectors are partial.
- Raw payload visibility is considered a success criterion for diagnostics, not proof of correct semantic mapping.
- cFos and KOSTAL are intentionally honest about mapping gaps.
- Time reference checking is a drift/check feature only, not clock synchronization.

## Troubleshooting

### Hanging behavior

- Long request hangs are not expected.
- Earlier iterations hit a real hanging issue caused by an old stuck `python.exe main.py` process on port `5000`.
- Current routes were kept lighter to avoid rebuilding heavy global snapshots on every request.

### Port 5000 already in use

- Check whether another Flask or Python process is already bound to `127.0.0.1:5000`.
- If needed, stop the old process before restarting the app.

### Stuck Python processes

- On Windows, inspect running Python processes and stop stale `main.py` instances if the UI becomes unresponsive.
- Multiple old processes can make it look like new code is running when an older server is actually serving the requests.

### Device unreachable

- Verify IP, host, port, subnet routing, and local firewall rules.
- Your setup spans `192.168.50.x` and `192.168.1.x`; cross-subnet routing problems are expected to matter.

### Auth failure

- Review configured `auth.type`, username, password, or token.
- cFos credentials remain configurable; they are not hardcoded in collector logic.

### Partial mapping

- Open the device detail page and inspect the latest raw payload plus recent measurements.
- A partial mapping state is explicit and intentional. It does not mean the collector is fully implemented.

### Empty measurements

- Open `/database` and confirm whether poll events exist without normalized measurements.
- If raw payloads exist but normalized metrics do not, check the device detail page for mapping gaps.

### Config issues

- Open `/config-health` or `/settings`.
- The validator reports missing sections, missing hosts, bad ports, missing credentials, suspicious cFos protocol blocks, invalid KOSTAL protocol selections, and time-config issues.

### Parse failure

- Inspect the latest raw payload on the device page.
- Extend or confirm the collector mapping only after the real payload format is known.
