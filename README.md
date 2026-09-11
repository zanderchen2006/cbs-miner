# MQTT-Vorratsdatenspeicherung

Ein Programm: MQTT-Mitschnitt von `cbs_koblenz/#` nach SQLite plus Live-Panel (HTML/CSS/JS).

```bash
cp .env.example .env
uv sync
uv run python src/app.py
```

Danach im Browser: [http://127.0.0.1:8081](http://127.0.0.1:8081)

Beenden mit Ctrl+C.

Das Programm abonniert den gesamten Topic-Baum `cbs_koblenz/#` (inkl. Retain und Untertopics), speichert jede Nachricht und zeigt sie live an.

## Konfiguration

Siehe `.env.example`. Echte Umgebungswerte überschreiben Einträge aus `.env`.

| Variable | Default | Bedeutung |
|---|---|---|
| `MQTT_HOST` | `broker.emqx.io` | Broker |
| `MQTT_PORT` | `8883` | Port (8883 = TLS, 1883 = unverschlüsselt) |
| `MQTT_TOPIC` | `cbs_koblenz/#` | Subscribe-Filter |
| `MQTT_CLIENT_ID` | `cbs-mqtt-<hostname>` | Eindeutige Client-ID |
| `SQLITE_PATH` | `data/mqtt.db` | Datenbankdatei |
| `PANEL_HOST` | `127.0.0.1` | Web-Oberfläche |
| `PANEL_PORT` | `8081` | Port der Oberfläche |

## Schema

Tabelle `messages`: `received_at` (UTC ISO-8601), `topic`, `payload` (roh), `qos`, `retain`, plus `building`, `floor`, `room`, `device`, `metric`, `value_num` (Zahl oder leer bei `null`).
