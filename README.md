# MQTT-Vorratsdatenspeicherung

Ein Programm: MQTT-Mitschnitt mehrerer Broker und Topics nach SQLite plus Live-Panel (HTML/CSS/JS).

```bash
cp .env.example .env
uv sync
uv run python src/app.py
```

Danach im Browser: [http://127.0.0.1:8081](http://127.0.0.1:8081)

Beenden mit Ctrl+C.

Beim ersten Start wird der Broker aus `.env` einmalig als Server übernommen. Danach legst du weitere Server und Topics in der Seitenleiste an; Änderungen gelten ohne Neustart.

## Konfiguration

Siehe `.env.example`. Echte Umgebungswerte überschreiben Einträge aus `.env`.

| Variable | Default | Bedeutung |
|---|---|---|
| `MQTT_HOST` | `broker.emqx.io` | Erst-Seed: Broker (danach UI) |
| `MQTT_PORT` | `8883` | Erst-Seed: Port (8883 = TLS, 1883 = unverschlüsselt) |
| `MQTT_TOPIC` | `cbs_koblenz/#` | Erst-Seed: Subscribe-Filter |
| `MQTT_CLIENT_ID` | `cbs-mqtt-<hostname>-<id>` | Erst-Seed: Client-ID, sonst automatisch |
| `SQLITE_PATH` | `data/mqtt.db` | Datenbankdatei |
| `PANEL_HOST` | `127.0.0.1` | Web-Oberfläche |
| `PANEL_PORT` | `8081` | Port der Oberfläche |

Server, Topics, TLS und optionale Anmeldung werden in der Oberfläche gespeichert (SQLite) und überschreiben die `MQTT_*`-Werte nach dem ersten Start.

## Schema

Tabelle `messages`: `received_at` (UTC ISO-8601), `server_id`, `topic`, `payload` (roh), `qos`, `retain`, plus `building`, `floor`, `room`, `device`, `metric`, `value_num` (Zahl oder leer bei `null`).

Tabellen `servers` und `subscriptions` halten die Broker- und Topic-Konfiguration.
