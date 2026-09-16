"""CBS MQTT: live subscribe, SQLite store, and HTML panel."""

from __future__ import annotations

import json
import re
import sqlite3
import ssl
import sys
import threading
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import paho.mqtt.client as mqtt

from config import env, load_dotenv, sqlite_path
from store import Store, default_client_id, public_server
from topics import decode_payload, item_from

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
ASSETS = ROOT / "assets"
FILTER_COLS = ("building", "floor", "room", "device", "metric")
SERVER_PATH = re.compile(r"^/api/servers/(\d+)$")
SERVER_TOPICS_PATH = re.compile(r"^/api/servers/(\d+)/topics$")
SERVER_TOPIC_PATH = re.compile(r"^/api/servers/(\d+)/topics/(\d+)$")


class LiveTree:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.topics: dict[tuple[int | None, str], dict] = {}

    def upsert(self, item: dict) -> None:
        with self._lock:
            self.topics[(item.get("server_id"), item["topic"])] = item

    def snapshot(self) -> list[dict]:
        with self._lock:
            return [dict(item) for item in self.topics.values()]


class MqttManager:
    def __init__(self, store: Store, live: LiveTree) -> None:
        self.store = store
        self.live = live
        self._lock = threading.Lock()
        self._clients: dict[int, mqtt.Client] = {}
        self._connected: dict[int, bool] = {}
        self._errors: dict[int, str] = {}

    def start_all(self) -> None:
        for server in self.store.list_servers():
            if server["enabled"]:
                self._start_server(server)

    def apply_server(self, server_id: int) -> None:
        self._stop_server(server_id)
        server = self.store.get_server(server_id)
        if server is not None and server["enabled"]:
            self._start_server(server)

    def remove_server(self, server_id: int) -> None:
        self._stop_server(server_id)

    def stop_all(self) -> None:
        for server_id in list(self._clients):
            self._stop_server(server_id)

    def status(self) -> list[dict]:
        servers = self.store.list_servers()
        with self._lock:
            connected = dict(self._connected)
            errors = dict(self._errors)
        out = []
        for server in servers:
            public = public_server(server)
            public["connected"] = bool(connected.get(server["id"]))
            public["error"] = errors.get(server["id"]) or None
            out.append(public)
        return out

    def _set_error(self, server_id: int, message: str | None) -> None:
        with self._lock:
            if message:
                self._errors[server_id] = message
            else:
                self._errors.pop(server_id, None)

    def _set_connected(self, server_id: int, connected: bool) -> None:
        with self._lock:
            if server_id in self._clients:
                self._connected[server_id] = connected
            elif not connected:
                self._connected.pop(server_id, None)

    def _start_server(self, server: dict) -> None:
        server_id = int(server["id"])
        host = server["host"]
        port = int(server["port"])
        name = server["name"]
        client_id = server.get("client_id") or default_client_id(server_id)
        topics = [
            (item["topic"], int(item["qos"]))
            for item in server.get("topics") or []
            if item.get("enabled", True)
        ]
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311,
        )
        client.reconnect_delay_set(min_delay=1, max_delay=120)
        if server.get("username"):
            client.username_pw_set(server["username"], server.get("password") or None)

        def on_connect(
            _client: mqtt.Client,
            _userdata: object,
            _connect_flags: mqtt.ConnectFlags,
            reason_code: mqtt.ReasonCode,
            _properties: mqtt.Properties | None,
            sid: int = server_id,
            label: str = name,
            broker: str = host,
            broker_port: int = port,
            subs: list[tuple[str, int]] = topics,
        ) -> None:
            with self._lock:
                if self._clients.get(sid) is not _client:
                    return
            if reason_code.is_failure:
                self._set_connected(sid, False)
                self._set_error(sid, f"Connect fehlgeschlagen: {reason_code}")
                print(f"MQTT connect failed {label} ({broker}:{broker_port}): {reason_code}", file=sys.stderr)
                return
            for topic, qos in subs:
                _client.subscribe(topic, qos=qos)
            self._set_connected(sid, True)
            self._set_error(sid, None)
            joined = ", ".join(topic for topic, _qos in subs) or "(keine Topics)"
            print(f"MQTT connected {label} {broker}:{broker_port}, subscribed to {joined}", flush=True)

        def on_disconnect(
            _client: mqtt.Client,
            _userdata: object,
            _disconnect_flags: mqtt.DisconnectFlags,
            reason_code: mqtt.ReasonCode,
            _properties: mqtt.Properties | None,
            sid: int = server_id,
            label: str = name,
        ) -> None:
            with self._lock:
                if self._clients.get(sid) is not _client:
                    return
            self._set_connected(sid, False)
            if reason_code.is_failure:
                self._set_error(sid, f"Getrennt: {reason_code}")
                print(f"MQTT disconnected {label}: {reason_code}, reconnecting…", flush=True)

        def on_connect_fail(
            _client: mqtt.Client,
            _userdata: object,
            sid: int = server_id,
            label: str = name,
            broker: str = host,
            broker_port: int = port,
        ) -> None:
            self._set_connected(sid, False)
            self._set_error(sid, f"Keine Verbindung zu {broker}:{broker_port} (TLS/Netzwerk)")
            print(f"MQTT connect fail {label} ({broker}:{broker_port})", file=sys.stderr)

        def on_message(
            _client: mqtt.Client,
            _userdata: object,
            message: mqtt.MQTTMessage,
            sid: int = server_id,
            label: str = name,
        ) -> None:
            with self._lock:
                if self._clients.get(sid) is not _client:
                    return
            received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            payload = decode_payload(message.payload)
            item = item_from(
                message.topic,
                payload,
                message.qos,
                bool(message.retain),
                received_at,
                server_id=sid,
                server_name=label,
            )
            self.live.upsert(item)
            if STORE is not None:
                STORE.insert(
                    received_at=received_at,
                    topic=message.topic,
                    payload=payload,
                    qos=message.qos,
                    retain=bool(message.retain),
                    server_id=sid,
                )

        client.on_connect = on_connect
        client.on_connect_fail = on_connect_fail
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        if server.get("tls"):
            # Public test brokers (e.g. test.mosquitto.org:8883) often use a
            # private CA that is not in the system store.
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)
        with self._lock:
            self._clients[server_id] = client
            self._connected[server_id] = False
        client.connect_async(host, port, keepalive=60)
        client.loop_start()

    def _stop_server(self, server_id: int) -> None:
        with self._lock:
            client = self._clients.pop(server_id, None)
            self._connected.pop(server_id, None)
            self._errors.pop(server_id, None)
        if client is None:
            return
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass


LIVE = LiveTree()
STORE: Store | None = None
MANAGER: MqttManager | None = None


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
    server_id = row["server_id"] if "server_id" in cols else None
    server_name = None
    keys = set(row.keys())
    if "server_name" in keys:
        server_name = row["server_name"]
    parsed = item_from(
        row["topic"] or "",
        payload,
        row["qos"],
        bool(row["retain"]),
        row["received_at"],
        server_id=server_id,
        server_name=server_name,
    )
    for key in FILTER_COLS:
        if key in cols and row[key] not in (None, ""):
            parsed[key] = row[key]
    if "value_num" in cols and row["value_num"] is not None:
        parsed["value_num"] = row["value_num"]
    return parsed


def live_key(item: dict) -> tuple[int | None, str]:
    return (item.get("server_id"), item.get("topic") or "")


def attach_default_server(item: dict, servers: list[dict]) -> dict:
    if item.get("server_id") is not None or len(servers) != 1:
        return item
    item = dict(item)
    item["server_id"] = servers[0]["id"]
    item["server_name"] = servers[0]["name"]
    return item


def load_db_latest(conn: sqlite3.Connection) -> dict[tuple[int | None, str], dict]:
    cols = columns(conn)
    select = "messages.id, messages.received_at, messages.topic, messages.payload, messages.qos, messages.retain"
    for extra in (*FILTER_COLS, "value_num", "server_id"):
        if extra in cols:
            select += f", messages.{extra}"
    join = ""
    group = "messages.topic"
    if "server_id" in cols:
        join = "LEFT JOIN servers ON servers.id = messages.server_id"
        select += ", servers.name AS server_name"
        group = "IFNULL(messages.server_id, 0), messages.topic"
    rows = conn.execute(
        f"""
        SELECT {select}
        FROM messages
        {join}
        WHERE messages.id IN (SELECT MAX(id) FROM messages GROUP BY {group})
        """
    ).fetchall()
    latest: dict[tuple[int | None, str], dict] = {}
    for row in rows:
        item = as_dict(row, cols)
        latest[live_key(item)] = item
    return latest


def distinct_from(items: list[dict], column: str) -> list[str]:
    values = sorted({item[column] for item in items if item.get(column)})
    return values


def matches(item: dict, query: dict[str, list[str]]) -> bool:
    one = lambda key: (query.get(key) or [""])[0].strip()
    server = one("server")
    if server:
        if str(item.get("server_id") or "") != server:
            return False
    for column in FILTER_COLS:
        value = one(column)
        if value and item.get(column) != value:
            return False
    search = one("search").lower()
    if search:
        hay = f"{item.get('topic') or ''} {item.get('payload') or ''} {item.get('server_name') or ''}".lower()
        if search not in hay:
            return False
    return True


def load_state(query: dict[str, list[str]]) -> dict:
    path = sqlite_path()
    servers = MANAGER.status() if MANAGER is not None else []
    merged: dict[tuple[int | None, str], dict] = {}
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
            select = "messages.id, messages.received_at, messages.topic, messages.payload, messages.qos, messages.retain"
            for extra in (*FILTER_COLS, "value_num", "server_id"):
                if extra in cols:
                    select += f", messages.{extra}"
            join = ""
            if "server_id" in cols:
                join = "LEFT JOIN servers ON servers.id = messages.server_id"
                select += ", servers.name AS server_name"
            clauses: list[str] = []
            params: list[object] = []
            server = one("server")
            if server and "server_id" in cols:
                try:
                    server_id = int(server)
                    if len(servers) == 1 and servers[0]["id"] == server_id:
                        clauses.append("(messages.server_id = ? OR messages.server_id IS NULL)")
                    else:
                        clauses.append("messages.server_id = ?")
                    params.append(server_id)
                except ValueError:
                    pass
            for column in FILTER_COLS:
                value = one(column)
                if value and column in cols:
                    clauses.append(f"messages.{column} = ?")
                    params.append(value)
            search = one("search")
            if search:
                clauses.append(
                    "(IFNULL(messages.payload, '') LIKE ? OR messages.topic LIKE ?)"
                )
                params.extend([f"%{search}%", f"%{search}%"])
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            history_rows = conn.execute(
                f"""
                SELECT {select}
                FROM messages
                {join}
                {where}
                ORDER BY messages.received_at DESC, messages.id DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()
            history = [attach_default_server(as_dict(row, cols), servers) for row in history_rows]
        finally:
            conn.close()
    else:
        history = []

    merged = {
        live_key(attach_default_server(item, servers)): attach_default_server(item, servers)
        for item in merged.values()
    }
    for item in LIVE.snapshot():
        item = attach_default_server(item, servers)
        merged[live_key(item)] = item

    live = sorted(
        (item for item in merged.values() if matches(item, query)),
        key=lambda item: (item.get("server_name") or "", item.get("topic") or ""),
    )
    any_connected = any(server["connected"] for server in servers)
    topic_labels = [
        item["topic"]
        for server in servers
        for item in server.get("topics") or []
        if item.get("enabled", True)
    ]
    mqtt_status = {
        "connected": any_connected,
        "topic": ", ".join(topic_labels),
        "live_topics": len(merged),
        "servers": servers,
    }
    ok = bool(live) or stats["n"] > 0 or any_connected
    return {
        "ok": ok,
        "db_path": str(path),
        "mqtt": mqtt_status,
        "servers": servers,
        "stats": {
            **stats,
            "live_topics": len(merged),
        },
        "filters": {
            **{column: distinct_from(list(merged.values()), column) for column in FILTER_COLS},
            "server": [{"id": server["id"], "name": server["name"]} for server in servers],
        },
        "live": live,
        "messages": history,
    }


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, directory=str(WEB), **kwargs)

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._send_json(load_state(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/servers":
            servers = MANAGER.status() if MANAGER is not None else []
            self._send_json({"servers": servers})
            return
        if parsed.path.startswith("/assets/"):
            self._send_file(ASSETS / Path(unquote(parsed.path)).name)
            return
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/servers":
            self._create_server()
            return
        match = SERVER_TOPICS_PATH.match(parsed.path)
        if match:
            self._add_topic(int(match.group(1)))
            return
        self.send_error(404)

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        match = SERVER_PATH.match(parsed.path)
        if match:
            self._update_server(int(match.group(1)))
            return
        self.send_error(404)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        match = SERVER_TOPIC_PATH.match(parsed.path)
        if match:
            self._delete_topic(int(match.group(1)), int(match.group(2)))
            return
        match = SERVER_PATH.match(parsed.path)
        if match:
            self._delete_server(int(match.group(1)))
            return
        self.send_error(404)

    def _create_server(self) -> None:
        if STORE is None or MANAGER is None:
            self._send_json({"error": "Store nicht bereit"}, 503)
            return
        try:
            server = STORE.create_server(self._read_json())
        except ValueError as exc:
            self._send_json({"error": str(exc)}, 400)
            return
        MANAGER.apply_server(server["id"])
        public = public_server(server)
        public["connected"] = False
        self._send_json(public, 201)

    def _update_server(self, server_id: int) -> None:
        if STORE is None or MANAGER is None:
            self._send_json({"error": "Store nicht bereit"}, 503)
            return
        try:
            server = STORE.update_server(server_id, self._read_json())
        except ValueError as exc:
            self._send_json({"error": str(exc)}, 400)
            return
        if server is None:
            self._send_json({"error": "Server nicht gefunden"}, 404)
            return
        MANAGER.apply_server(server_id)
        public = public_server(server)
        status = {item["id"]: item["connected"] for item in MANAGER.status()}
        public["connected"] = bool(status.get(server_id))
        self._send_json(public)

    def _delete_server(self, server_id: int) -> None:
        if STORE is None or MANAGER is None:
            self._send_json({"error": "Store nicht bereit"}, 503)
            return
        if not STORE.delete_server(server_id):
            self._send_json({"error": "Server nicht gefunden"}, 404)
            return
        MANAGER.remove_server(server_id)
        self._send_json({"ok": True})

    def _add_topic(self, server_id: int) -> None:
        if STORE is None or MANAGER is None:
            self._send_json({"error": "Store nicht bereit"}, 503)
            return
        try:
            topic = STORE.add_topic(server_id, self._read_json())
        except KeyError:
            self._send_json({"error": "Server nicht gefunden"}, 404)
            return
        except ValueError as exc:
            self._send_json({"error": str(exc)}, 400)
            return
        MANAGER.apply_server(server_id)
        self._send_json(topic, 201)

    def _delete_topic(self, server_id: int, topic_id: int) -> None:
        if STORE is None or MANAGER is None:
            self._send_json({"error": "Store nicht bereit"}, 503)
            return
        if not STORE.delete_topic(server_id, topic_id):
            self._send_json({"error": "Topic nicht gefunden"}, 404)
            return
        MANAGER.apply_server(server_id)
        self._send_json({"ok": True})

    def _read_json(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Ungültiges JSON") from exc
        if not isinstance(data, dict):
            raise ValueError("JSON-Objekt erwartet")
        return data

    def _send_json(self, data: dict, status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
    global STORE, MANAGER
    load_dotenv()
    host = env("PANEL_HOST", "127.0.0.1")
    port = int(env("PANEL_PORT", "8081"))
    STORE = Store(sqlite_path())
    STORE.seed_from_env_if_empty()
    STORE.attach_orphan_messages()
    MANAGER = MqttManager(STORE, LIVE)
    seed_from_db()
    MANAGER.start_all()
    httpd = ThreadingHTTPServer((host, port), Handler)
    servers = MANAGER.status()
    print(f"CBS MQTT: http://{host}:{port}", flush=True)
    if servers:
        for server in servers:
            topics = ", ".join(item["topic"] for item in server["topics"]) or "(keine Topics)"
            print(f"Server {server['name']}: {server['host']}:{server['port']} · {topics}", flush=True)
    else:
        print("Keine MQTT-Server konfiguriert — bitte in der Oberfläche anlegen.", flush=True)
    print(f"Datenbank: {sqlite_path().resolve()}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping…", flush=True)
    finally:
        MANAGER.stop_all()
        STORE.close()
        httpd.server_close()


if __name__ == "__main__":
    main()
