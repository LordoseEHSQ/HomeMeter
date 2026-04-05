# Projektdokumentation

## Dauerhafte Projektregel: sichtbare Sprachumschaltung

- Eine Sprachumschaltung gilt nur dann als vorhanden, wenn sie in der realen UI sichtbar und bedienbar ist.
- Eine Konfigurationsoption allein reicht nicht aus.
- Vor Aussagen wie "mehrsprachig" oder "Sprachwechsel vorhanden" muss die laufende UI geprüft werden.
- Wenn der Nutzer keinen Sprachschalter sieht, ist das als echter Produktmangel zu behandeln und nicht als Bedienfehler.
- Änderungen an Sprachumschaltung oder Lokalisierung müssen immer in der laufenden UI verifiziert werden, nicht nur im Template oder Code.
- Wenn die Übersetzung nur teilweise umgesetzt ist, muss das offen dokumentiert werden.

## Aktueller Stand

- Die Sidebar enthält jetzt einen sichtbaren Sprachschalter für `Deutsch` und `English`.
- Die Navigation, die Sidebar-Aktionen und der App-Untertitel reagieren bereits auf die Umschaltung.
- Der Rest der UI ist noch nicht vollständig zweisprachig und bleibt deshalb aktuell nur teilweise lokalisiert.
