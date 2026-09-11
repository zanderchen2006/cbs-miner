"""CBS MQTT: live subscribe, SQLite store, and HTML panel."""

from __future__ import annotations

import json
import socket
import sqlite3
import sys
import threading
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import paho.mqtt.client as mqtt

from config import env, load_dotenv, sqlite_path
from store import Store
from topics import decode_payload, item_from

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
ASSETS = ROOT / "assets"
FILTER_COLS = ("building", "floor", "room", "device", "metric")


class LiveTree:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.topics: dict[str, dict] = {}
        self.connected = False
        self.mqtt_topic = "cbs_koblenz/#"

    def upsert(self, item: dict) -> None:
        with self._lock:
            self.topics[item["topic"]] = item

    def snapshot(self) -> list[dict]:
        with self._lock:
            return [dict(item) for item in self.topics.values()]

    def set_connected(self, connected: bool) -> None:
        with self._lock:
            self.connected = connected

    def status(self) -> dict:
        with self._lock:
            return {
                "connected": self.connected,
                "topic": self.mqtt_topic,
                "live_topics": len(self.topics),
            }


LIVE = LiveTree()
STORE: Store | None = None


def open_db() -> sqlite3.Connection | None:
    path = sqlite_path()
    if not path.is_file() or path.stat().st_size == 0:
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("SELECT 1 FROM messages LIMIT 1")
    except sqlite3.OperationalError:
        conn.close()
        return None
    return conn


def columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(messages)")}


def as_dict(row: sqlite3.Row, cols: set[str]) -> dict:
    payload = row["payload"]
    parsed = item_from(
        row["topic"] or "",
        payload,
        row["qos"],
        bool(row["retain"]),
        row["received_at"],
    )
    for key in FILTER_COLS:
        if key in cols and row[key] not in (None, ""):
            parsed[key] = row[key]
    if "value_num" in cols and row["value_num"] is not None:
        parsed["value_num"] = row["value_num"]
    return parsed


def load_db_latest(conn: sqlite3.Connection) -> dict[str, dict]:
    cols = columns(conn)
    select = "id, received_at, topic, payload, qos, retain"
    for extra in (*FILTER_COLS, "value_num"):
        if extra in cols:
            select += f", {extra}"
    rows = conn.execute(
        f"""
        SELECT {select}
        FROM messages
        WHERE id IN (SELECT MAX(id) FROM messages GROUP BY topic)
        """
    ).fetchall()
    return {row["topic"]: as_dict(row, cols) for row in rows}


def distinct_from(items: list[dict], column: str) -> list[str]:
    values = sorted({item[column] for item in items if item.get(column)})
    return values


def matches(item: dict, query: dict[str, list[str]]) -> bool:
    one = lambda key: (query.get(key) or [""])[0].strip()
    for column in FILTER_COLS:
        value = one(column)
        if value and item.get(column) != value:
            return False
    search = one("search").lower()
    if search:
        hay = f"{item.get('topic') or ''} {item.get('payload') or ''}".lower()
        if search not in hay:
            return False
    return True


def load_state(query: dict[str, list[str]]) -> dict:
    path = sqlite_path()
    merged: dict[str, dict] = {}
    stats = {"n": 0, "topics": 0, "first_at": None, "last_at": None}
    conn = open_db()
    if conn is not None:
        try:
            merged.update(load_db_latest(conn))
            row = conn.execute(
                """
                SELECT
                  COUNT(*) AS n,
                  COUNT(DISTINCT topic) AS topics,
                  MIN(received_at) AS first_at,
                  MAX(received_at) AS last_at
                FROM messages
                """
            ).fetchone()
            stats = {
                "n": row["n"],
                "topics": row["topics"],
                "first_at": row["first_at"],
                "last_at": row["last_at"],
            }
            one = lambda key: (query.get(key) or [""])[0].strip()
            try:
                limit = max(1, min(int(one("limit") or "200"), 2000))
            except ValueError:
                limit = 200
            cols = columns(conn)
            select = "id, received_at, topic, payload, qos, retain"
            for extra in (*FILTER_COLS, "value_num"):
                if extra in cols:
                    select += f", {extra}"
            clauses: list[str] = []
            params: list[object] = []
            for column in FILTER_COLS:
                value = one(column)
                if value and column in cols:
                    clauses.append(f"{column} = ?")
                    params.append(value)
            search = one("search")
            if search:
                clauses.append("(IFNULL(payload, '') LIKE ? OR topic LIKE ?)")
                params.extend([f"%{search}%", f"%{search}%"])
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            history_rows = conn.execute(
                f"""
                SELECT {select}
                FROM messages
                {where}
                ORDER BY received_at DESC, id DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
            history = [as_dict(row, cols) for row in history_rows]
        finally:
            conn.close()
    else:
        history = []

    for item in LIVE.snapshot():
        merged[item["topic"]] = item

    live = sorted(
        (item for item in merged.values() if matches(item, query)),
        key=lambda item: item["topic"] or "",
    )
    mqtt = LIVE.status()
    ok = bool(live) or stats["n"] > 0 or mqtt["connected"]
    return {
        "ok": ok,
        "db_path": str(path),
        "mqtt": mqtt,
        "stats": {
            **stats,
            "live_topics": len(merged),
        },
        "filters": {column: distinct_from(list(merged.values()), column) for column in FILTER_COLS},
        "live": live,
        "messages": history,
    }


def start_mqtt() -> mqtt.Client:
    load_dotenv()
    host = env("MQTT_HOST", "broker.emqx.io")
    port = int(env("MQTT_PORT", "8883"))
    topic = env("MQTT_TOPIC", "cbs_koblenz/#")
    client_id = env("MQTT_CLIENT_ID", f"cbs-mqtt-{socket.gethostname()}")
    LIVE.mqtt_topic = topic

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )
    client.reconnect_delay_set(min_delay=1, max_delay=120)

    def on_connect(
        _client: mqtt.Client,
        _userdata: object,
        _connect_flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            LIVE.set_connected(False)
            print(f"MQTT connect failed: {reason_code}", file=sys.stderr)
            return
        _client.subscribe(topic, qos=1)
        LIVE.set_connected(True)
        print(f"MQTT connected {host}:{port}, subscribed to {topic}", flush=True)

    def on_disconnect(
        _client: mqtt.Client,
        _userdata: object,
        _disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        _properties: mqtt.Properties | None,
    ) -> None:
        LIVE.set_connected(False)
        if reason_code.is_failure:
            print(f"MQTT disconnected: {reason_code}, reconnecting…", flush=True)

    def on_message(
        _client: mqtt.Client,
        _userdata: object,
        message: mqtt.MQTTMessage,
    ) -> None:
        received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        payload = decode_payload(message.payload)
        item = item_from(
            message.topic,
            payload,
            message.qos,
            bool(message.retain),
            received_at,
        )
        LIVE.upsert(item)
        if STORE is not None:
            STORE.insert(
                received_at=received_at,
                topic=message.topic,
                payload=payload,
                qos=message.qos,
                retain=bool(message.retain),
            )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    if port != 1883:
        client.tls_set()
    client.connect_async(host, port, keepalive=60)
    client.loop_start()
    return client


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(WEB), **kwargs)

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            body = json.dumps(load_state(parse_qs(parsed.query))).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path.startswith("/assets/"):
            self._send_file(ASSETS / Path(unquote(parsed.path)).name)
            return
        super().do_GET()

    def _send_file(self, path: Path) -> None:
        path = path.resolve()
        if ASSETS not in path.parents and path.parent != ASSETS:
            self.send_error(404)
            return
        if not path.is_file():
            self.send_error(404)
            return
        data = path.read_bytes()
        ctype = self.guess_type(str(path))
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def seed_from_db() -> None:
    conn = open_db()
    if conn is None:
        return
    try:
        for item in load_db_latest(conn).values():
            LIVE.upsert(item)
    finally:
        conn.close()


def main() -> None:
    global STORE
    load_dotenv()
    host = env("PANEL_HOST", "127.0.0.1")
    port = int(env("PANEL_PORT", "8081"))
    STORE = Store(sqlite_path())
    seed_from_db()
    client = start_mqtt()
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"CBS MQTT: http://{host}:{port}", flush=True)
    print(f"Live-Topic: {LIVE.mqtt_topic}", flush=True)
    print(f"Datenbank: {sqlite_path().resolve()}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping…", flush=True)
    finally:
        client.loop_stop()
        client.disconnect()
        STORE.close()
        server.server_close()


if __name__ == "__main__":
    main()
