# HomeMeter Diagnostics

HomeMeter Diagnostics is a local Flask application for checking whether your home energy measurements are reachable, internally consistent and plausible before you trust them for charging or automation logic. It is diagnostics-first: it stores poll attempts, normalized values, raw payloads and alerts in SQLite, and it makes uncertainty visible instead of masking it.

## What the app is for

- Verify whether devices are reachable
- See whether failures come from network, timeout, auth, parsing, config or incomplete mapping
- Confirm that data is really being stored
- Inspect which collectors are healthy, reachable-but-partial, failing or disabled
- Review config health before trusting collector output
- Inspect database activity and recent rows directly in the UI

## Current pages

- `/`: dashboard
- `/system`: combined system health summary
- `/settings`: device operations and configuration overview
- `/config-health`: config validation findings
- `/database`: database inspection and storage observability
- `/alerts`: recent alerts
- `/devices/<device_name>`: detailed device diagnostics

## Core structure

- `main.py`: Flask entrypoint
- `app.py`: app factory, config loading, collector setup and polling manager
- `collectors/`: collector modules and collector result contract
- `storage/sqlite_store.py`: SQLite schema and data-access helpers
- `analysis/plausibility.py`: plausibility rules
- `services/`: config validation, health summary, diagnostics formatting and database inspection
- `web/routes.py`: Flask routes and operation actions
- `templates/` and `static/`: Jinja UI and CSS
- `tests/`: pytest suite

## Windows setup

```powershell
py -3.13 -m pip install -r requirements.txt
Copy-Item config.example.yaml config.yaml
python main.py
```

Then open `http://127.0.0.1:5000`.

If you prefer a virtual environment, you can still create one, but the current environment may hit Windows permission problems around `ensurepip`. The app itself works with the installed local Python as long as the required packages are present.

## Configuration

Top-level sections expected in `config.yaml`:

- `app`
- `polling`
- `storage`
- `devices`

Configured devices:

- `cfos`
- `easee`
- `kostal`

The UI now validates and reports configuration issues such as:

- missing sections
- invalid polling interval
- invalid timeout values
- enabled device without host/base URL
- missing credentials for selected auth type
- suspicious or incomplete collector setup

## Collector truthfulness

This project does not pretend that incomplete device mappings are complete.

- `cFos`: real HTTP collector with best-effort field mapping and raw fallback
- `Easee`: adapter skeleton with transparent raw payload capture and explicit mapping incompleteness
- `KOSTAL`: connectivity-first collector with clear distinction between reachability and mapping completeness

Collector and operations states shown in the UI include:

- healthy
- reachable
- timeout
- unreachable
- auth missing
- auth failed
- parsing failed
- mapping incomplete
- unsupported response
- disabled
- config error

## Database observability

The database page shows:

- SQLite file path
- file size
- row counts per table
- newest and oldest record timestamps
- latest rows from `measurements`
- latest rows from `poll_events`
- latest rows from `alerts`
- storage activity summary

## Operations actions

The UI includes these local actions:

- reload config
- run diagnostics now
- test connection per device

These actions are implemented server-side and update stored poll events where appropriate.

## Running tests

Install test dependencies through `requirements.txt`, then run:

```powershell
py -3.13 -m pytest -q tests -p no:cacheprovider --basetemp=.pytest-tmp-run
```

The custom `--basetemp` value is recommended on this Windows setup because the default temp directory may have restrictive permissions.

## Implemented and intentionally partial

Implemented:

- local Flask UI
- SQLite persistence
- background polling
- config validation
- system health summary
- database inspection
- device operations overview
- pytest-based automated tests

Intentionally partial:

- exact real-device API mappings for Easee
- exact real-device API/register mappings for KOSTAL
- full semantic certainty for cFos field names until your payload is confirmed

## Troubleshooting

### Device unreachable

- Verify IP, host, port and subnet routing.
- Your setup spans `192.168.50.x` and `192.168.1.x`; cross-subnet routing problems are expected to matter.

### Timeout

- Increase timeout values in `config.yaml`.
- Check firewall, routing and Wi-Fi stability.

### Auth failure

- Review `auth.type`, username, password or token.
- The UI distinguishes missing credentials from actual auth failures when possible.

### Parse failure

- Open the device detail page and inspect the latest raw payload.
- Extend the collector only after verifying the real response shape.

### Data not stored

- Open `/database`.
- Check poll-event counts, measurement counts and the latest rows in each table.

### Mapping incomplete

- This is an explicit, honest state for partially implemented collectors.
- It is not a silent error and not a success state.
