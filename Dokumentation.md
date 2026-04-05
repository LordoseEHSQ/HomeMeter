# Projektdokumentation

## Dauerhafte Projektregel: sichtbare Sprachumschaltung

- Eine Sprachumschaltung gilt nur dann als vorhanden, wenn sie in der realen UI sichtbar und bedienbar ist.
- Eine Konfigurationsoption allein reicht nicht aus.
- Vor Aussagen wie "mehrsprachig" oder "Sprachwechsel vorhanden" muss die laufende UI geprüft werden.
- Wenn der Nutzer keinen Sprachschalter sieht, ist das als echter Produktmangel zu behandeln und nicht als Bedienfehler.
- Änderungen an Sprachumschaltung oder Lokalisierung müssen immer in der laufenden UI verifiziert werden, nicht nur im Template oder Code.
- Wenn die Übersetzung nur teilweise umgesetzt ist, muss das offen dokumentiert werden.

## Produktstruktur und primäre Informations-Heimat

Die App trennt Informationen jetzt bewusster nach Zweck. Nicht jede Seite soll alles zeigen.

### Dashboard

- primärer Zweck: schneller operativer Überblick
- zeigt nur:
  - aktuelle Systemgesundheit
  - knappe Prioritäten/Warnungen
  - KPI-Snapshot
  - kurze Gerätezusammenfassung
  - kompakten Timing-Hinweis
- zeigt bewusst nicht mehr als Hauptinhalt:
  - Rohpayloads
  - tiefes Mapping-Detail
  - vollständige Cleanup-Historie
  - vollständige Integrationslücken-Tabellen

### Analytics

- primärer Zweck: Trends, KPIs und Energieverständnis über Zeit
- zeigt:
  - KPI-Karten
  - Zeitreihen
  - Abdeckung/Konfidenz
  - Interpretationshilfen
- zeigt bewusst nicht als Hauptinhalt:
  - Auth-Fehler
  - Token-/Transport-Details
  - Runtime-/Prozesshygiene

### Geräte-Detailseiten

- primärer Zweck: technische Wahrheit pro Gerät
- zeigt:
  - Verbindungs- und Auth-Zustand
  - Mess- und Recording-Zustand
  - Protokoll-/Mapping-Status
  - letzte Messwerte
  - Poll-Historie und Fehler
- Advanced/Debug auf derselben Seite, aber bewusst sekundär:
  - Rohpayloads
  - Specs-Dumps
  - unmappte Kandidatenfelder
  - tiefe technische Mapping-Details

### Einstellungen

- primärer Zweck: bearbeitbare Konfiguration
- zeigt:
  - echte Formulare
  - gruppierte Settings
  - helper text
  - Save-Workflow
- zeigt bewusst nicht als Hauptinteraktion:
  - Raw Config
  - Geräte-Diagnose
  - Debug-Dumps

### Einstellungen / Geräte

- primärer Zweck: read-only Betriebs- und Diagnoseübersicht pro Gerät
- zeigt:
  - Status
  - Auth-Zusammenfassung
  - Recording
  - Fähigkeiten und Lücken
- Bearbeitung gehört in den Settings-Hub, nicht hierhin.

### Systemstatus

- primärer Zweck: Runtime-, Timing-, Cleanup- und Prozessgesundheit
- ist die primäre Heimat für:
  - Polling-/Persistenz-Timing
  - Cleanup-Historie
  - Runtime-Hygiene
  - systemweite Integrationslücken

### Datenbankseite

- primärer Zweck: Speicher- und Tabelleninspektion
- zeigt:
  - Tabellenvolumen
  - letzte Zeilen
  - Recording-Zustand
  - Cleanup-Historie aus Datenbanksicht
- sie ist nicht die primäre Seite für allgemeine Runtime-Erklärung.

## Live-Refresh, Polling und Persistenz

Die App trennt jetzt bewusst vier unterschiedliche Laufzeit-Ebenen:

1. `analytics_refresh_interval_seconds`
   - steuert nur, wie oft Analytics-Teilansichten in der UI nachgeladen werden
   - erzeugt keinen impliziten Datenbank-Write

2. `poll_interval_seconds`
   - steuert, wie oft Collector gegen Geräte laufen
   - jeder Poll schreibt weiterhin `poll_events`, damit Diagnosehistorie sichtbar bleibt

3. `raw_write_interval_seconds`
   - steuert, wie oft Rohmessungen und Rohpayloads wirklich in SQLite persistiert werden
   - Polling kann häufiger stattfinden als Rohdaten-Write

4. `derived_write_interval_seconds`
   - steuert, wie oft semantische/abgeleitete Werte geschrieben werden

5. `rollup_interval_seconds`
   - steuert, wie oft Minuten-Rollups und KPI-Summaries aktualisiert werden

Wichtig:

- Live-Refresh ist nicht gleich Polling.
- Polling ist nicht gleich Persistenz.
- Persistenz ist nicht gleich Rollup-Bildung.
- Diese Trennung ist absichtlich eingebaut, damit die App operativ transparent bleibt und SQLite nicht unnötig belastet wird.

## Timing-Einstellungen

Die wichtigsten Laufzeiteinstellungen liegen in `config.yaml` unter `scheduling` und sind in der UI unter `Einstellungen` bearbeitbar:

- `analytics_refresh_interval_seconds`
- `poll_interval_seconds`
- `raw_write_interval_seconds`
- `derived_write_interval_seconds`
- `rollup_interval_seconds`
- `retention_days_raw`
- `retention_days_rollup`
- `persistence_enabled`
- `live_refresh_enabled`
- `cleanup_enabled`

Die UI zeigt zusätzlich die letzten erfolgreichen Zeitpunkte für:

- Polling
- Rohdaten-Write
- Derived-Write
- Rollup
- KPI-Summary
- Cleanup

## Retention-Verhalten

Retention ist jetzt ebenfalls Teil der Runtime-Konfiguration.

- `retention_days_raw`
  - gilt für alte Rohdaten in `measurements`
  - gilt für alte `poll_events`, weil dort Rohpayloads und Poll-Historie liegen
  - gilt auch für `alerts`, damit operative Ereignisse nicht unbegrenzt wachsen

- `retention_days_rollup`
  - gilt für `semantic_metrics`
  - gilt für `minute_rollups`
  - gilt für `kpi_summaries`

- `cleanup_enabled`
  - aktiviert oder deaktiviert den vereinfachten Cleanup-Lauf

## Cleanup-Verhalten

Der Cleanup ist absichtlich leicht gehalten:

- kein eigener Worker
- kein komplexer Scheduler
- keine aggressive Datenmigration

Stattdessen läuft ein einfacher SQLite-freundlicher Cleanup nur während aktiver Polling-Zyklen und höchstens ungefähr stündlich:

- löscht alte Zeilen aus `measurements`
- löscht alte Zeilen aus `poll_events`
- löscht alte Zeilen aus `alerts`
- löscht alte Zeilen aus `semantic_metrics`
- löscht alte Zeilen aus `minute_rollups`
- löscht alte Zeilen aus `kpi_summaries`

Jeder Cleanup-Lauf wird jetzt persistent in `cleanup_runs` protokolliert:

- Zeitstempel
- Erfolg/Misserfolg
- Scope bzw. Tabelle
- Anzahl gelöschter Zeilen
- Dauer
- Fehlermeldung

Damit ist nachvollziehbar, wann Cleanup lief und was tatsächlich bereinigt wurde.

## Aktuelle Grenzen

- Cleanup läuft derzeit nur innerhalb des aktiven Polling-Prozesses.
  - Wenn die App nicht pollt, läuft auch kein Cleanup.
- Cleanup ist bewusst einfach gehalten und noch keine vollständige Datenlebenszyklus-Verwaltung mit mehreren Policies pro Gerät oder Metrikgruppe.
- Teile der UI sind weiterhin nur teilweise lokalisiert; Analytics und zentrale Settings sind weiter als einige Detailseiten.

## Startup- und Runtime-Hygiene

HomeMeter versucht lokale Laufzeitfehler besser sichtbar zu machen, löst aber keine OS-Prozessverwaltung für dich.

- Beim Start prüft `main.py` leichtgewichtig, ob der konfigurierte Port bereits belegt erscheint.
- Wenn der Port schon belegt ist, startet die App nicht einfach still daneben, sondern bricht mit einer Warnung ab.
- Diese Prüfung ist nur ein Port-Check.
  - Sie identifiziert nicht sicher den Fremdprozess.
  - Sie beendet keine Prozesse automatisch.

## Troubleshooting

### Doppelter Python-Prozess / Port 5000 schon belegt

- Prüfe, ob noch ein alter `python main.py` oder ein anderer lokaler Flask-Prozess läuft.
- Wenn Port `5000` bereits belegt ist, startet HomeMeter absichtlich nicht einfach ein zweites Mal.
- Beende alte lokale Instanzen und starte danach neu.

### Stale lokale Server-Instanz

- Wenn die UI veraltet aussieht oder neue Routen fehlen, läuft oft noch eine alte Python-Instanz.
- Dann hilft ein sauberer Neustart der alten lokalen Prozesse mehr als nur ein Browser-Reload.

### Cleanup läuft nicht

- Cleanup hängt an aktiven Polling-Zyklen.
- Wenn Polling pausiert oder die App nicht läuft, wird auch kein Cleanup ausgeführt.
- Prüfe in Systemstatus oder Zeit-Einstellungen die letzte erfolgreiche Cleanup-Zeit und die Tabelle `cleanup_runs`.

## Manuelle Browser-QA-Checkliste

Wenn keine echte Browser-Automation verfügbar ist, nutze diese Liste für die finale visuelle Abnahme:

### Dashboard

- kein horizontaler Overflow
- Timing-Block sichtbar
- letzte Poll-/Write-/Rollup-Zeit lesbar
- keine abgeschnittenen Tabellen oder Karten

### Analytics

- Charts zeigen Zeitbezug und Einheit
- Refresh-Hinweis sichtbar
- keine kaputten Container
- keine gemischten Sprachreste an zentralen Analytics-Stellen

### Einstellungen / Geräte

- breite Geräteblöcke bleiben lesbar
- keine überlaufenden Felder
- Auth-, Recording- und Capability-Bereiche klar getrennt

### Geräte-Detailseiten

- Rohpayload-Vorschau bleibt im Container
- Status- und Mapping-Bereiche sind lesbar

### Einstellungen / Zeit

- Timing-Intervalle sichtbar
- Cleanup-Historie sichtbar
- keine überbreiten Tabellen

### Systemstatus

- Timing-Panel sichtbar
- Cleanup-Historie sichtbar
- Runtime-Hygiene-Hinweise sichtbar

### Datenbankseite

- `cleanup_runs` erscheint in Tabellenübersicht und Latest Rows
- keine überlaufenden Datenbanktabellen
- Zeitstempel bleiben lesbar

## Aktueller Stand

- Die Sidebar enthält einen sichtbaren Sprachschalter für `Deutsch` und `English`.
- Navigation, Sidebar-Aktionen und zentrale Analytics-/Settings-Bereiche reagieren auf die Umschaltung.
- Die App trennt jetzt Live-Refresh, Polling, Rohdaten-Persistenz, Derived-Persistenz und Rollup-Bildung sauber.
- Ein kompakter Timing-Block im Dashboard macht die operative Taktung direkt sichtbar.
- Retention und Cleanup sind konfigurierbar und als einfacher Hintergrundmechanismus in den Polling-Ablauf eingebunden.
