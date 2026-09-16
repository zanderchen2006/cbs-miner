"""SQLite storage for MQTT messages and broker configuration."""

from __future__ import annotations

import socket
import sqlite3
import threading
from pathlib import Path

from config import env
from topics import parse_topic, parse_value

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  received_at TEXT NOT NULL,
  topic TEXT NOT NULL,
  payload TEXT,
  qos INTEGER NOT NULL,
  retain INTEGER NOT NULL,
  building TEXT,
  floor TEXT,
  room TEXT,
  device TEXT,
  metric TEXT,
  value_num REAL
);
CREATE INDEX IF NOT EXISTS idx_messages_topic_time
  ON messages(topic, received_at);

CREATE TABLE IF NOT EXISTS servers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  host TEXT NOT NULL,
  port INTEGER NOT NULL,
  tls INTEGER NOT NULL DEFAULT 0,
  username TEXT,
  password TEXT,
  client_id TEXT,
  enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  server_id INTEGER NOT NULL,
  topic TEXT NOT NULL,
  qos INTEGER NOT NULL DEFAULT 1,
  enabled INTEGER NOT NULL DEFAULT 1,
  FOREIGN KEY (server_id) REFERENCES servers(id) ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_sub_server_topic
  ON subscriptions(server_id, topic);
"""

EXTRA_COLUMNS = (
    ("building", "TEXT"),
    ("floor", "TEXT"),
    ("room", "TEXT"),
    ("device", "TEXT"),
    ("metric", "TEXT"),
    ("value_num", "REAL"),
    ("server_id", "INTEGER"),
)


def _as_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    return default


def _clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_topics(raw: object | None) -> list[dict] | None:
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ValueError("topics muss eine Liste sein")
    out: list[dict] = []
    seen: set[str] = set()
    for item in raw:
        if isinstance(item, str):
            topic = item.strip()
            qos = 1
        elif isinstance(item, dict):
            topic = str(item.get("topic") or "").strip()
            try:
                qos = int(item.get("qos") if item.get("qos") is not None else 1)
            except (TypeError, ValueError) as exc:
                raise ValueError("qos muss eine Zahl sein") from exc
        else:
            raise ValueError("Ungültiger Topic-Eintrag")
        if not topic:
            continue
        if qos not in (0, 1, 2):
            raise ValueError("qos muss 0, 1 oder 2 sein")
        if topic in seen:
            continue
        seen.add(topic)
        out.append({"topic": topic, "qos": qos})
    return out


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        existing = {
            row[1] for row in self.conn.execute("PRAGMA table_info(messages)")
        }
        for name, typedef in EXTRA_COLUMNS:
            if name not in existing:
                self.conn.execute(f"ALTER TABLE messages ADD COLUMN {name} {typedef}")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_place "
            "ON messages(building, floor, room, device, metric)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_server_topic_time "
            "ON messages(server_id, topic, received_at)"
        )
        rows = self.conn.execute(
            """
            SELECT id, topic, payload
            FROM messages
            WHERE building IS NULL AND metric IS NULL AND topic IS NOT NULL
            """
        ).fetchall()
        for row_id, topic, payload in rows:
            parsed = parse_topic(topic)
            self.conn.execute(
                """
                UPDATE messages
                SET building = ?, floor = ?, room = ?, device = ?, metric = ?, value_num = ?
                WHERE id = ?
                """,
                (
                    parsed["building"],
                    parsed["floor"],
                    parsed["room"],
                    parsed["device"],
                    parsed["metric"],
                    parse_value(payload),
                    row_id,
                ),
            )

    def seed_from_env_if_empty(self) -> None:
        with self.lock:
            count = self.conn.execute("SELECT COUNT(*) FROM servers").fetchone()[0]
            if count:
                return
            host = env("MQTT_HOST", "broker.emqx.io")
            port = int(env("MQTT_PORT", "8883"))
            topic = env("MQTT_TOPIC", "cbs_koblenz/#")
            client_id = _clean_text(env("MQTT_CLIENT_ID", ""))
            tls = port != 1883
            name = host
            server_id = self._insert_server_locked(
                {
                    "name": name,
                    "host": host,
                    "port": port,
                    "tls": tls,
                    "username": None,
                    "password": None,
                    "client_id": client_id,
                    "enabled": True,
                    "topics": [{"topic": topic, "qos": 1}] if topic else [],
                }
            )
            self.conn.execute(
                "UPDATE messages SET server_id = ? WHERE server_id IS NULL",
                (server_id,),
            )
            self.conn.commit()

    def attach_orphan_messages(self) -> None:
        with self.lock:
            row = self.conn.execute(
                "SELECT id FROM servers ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return
            self.conn.execute(
                "UPDATE messages SET server_id = ? WHERE server_id IS NULL",
                (row[0],),
            )
            self.conn.commit()

    def insert(
        self,
        received_at: str,
        topic: str,
        payload: str | None,
        qos: int,
        retain: bool,
        server_id: int | None = None,
    ) -> None:
        parsed = parse_topic(topic)
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO messages (
                  received_at, topic, payload, qos, retain,
                  building, floor, room, device, metric, value_num, server_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    received_at,
                    topic,
                    payload,
                    qos,
                    int(retain),
                    parsed["building"],
                    parsed["floor"],
                    parsed["room"],
                    parsed["device"],
                    parsed["metric"],
                    parse_value(payload),
                    server_id,
                ),
            )
            self.conn.commit()

    def list_servers(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM servers ORDER BY id"
            ).fetchall()
            subs = self.conn.execute(
                "SELECT * FROM subscriptions ORDER BY id"
            ).fetchall()
        by_id: dict[int, dict] = {}
        for row in rows:
            server = self._server_from_row(row)
            server["topics"] = []
            by_id[server["id"]] = server
        for row in subs:
            topic = self._topic_from_row(row)
            parent = by_id.get(topic["server_id"])
            if parent is not None:
                parent["topics"].append(topic)
        return list(by_id.values())

    def get_server(self, server_id: int) -> dict | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM servers WHERE id = ?",
                (server_id,),
            ).fetchone()
            if row is None:
                return None
            subs = self.conn.execute(
                "SELECT * FROM subscriptions WHERE server_id = ? ORDER BY id",
                (server_id,),
            ).fetchall()
        server = self._server_from_row(row)
        server["topics"] = [self._topic_from_row(item) for item in subs]
        return server

    def create_server(self, data: dict) -> dict:
        payload = self._parse_server(data, partial=False)
        with self.lock:
            server_id = self._insert_server_locked(payload)
            self.conn.commit()
        server = self.get_server(server_id)
        assert server is not None
        return server

    def update_server(self, server_id: int, data: dict) -> dict | None:
        current = self.get_server(server_id)
        if current is None:
            return None
        payload = self._parse_server(data, partial=True, current=current)
        topics = normalize_topics(data["topics"]) if "topics" in data else None
        with self.lock:
            self.conn.execute(
                """
                UPDATE servers
                SET name = ?, host = ?, port = ?, tls = ?, username = ?,
                    password = ?, client_id = ?, enabled = ?
                WHERE id = ?
                """,
                (
                    payload["name"],
                    payload["host"],
                    payload["port"],
                    int(payload["tls"]),
                    payload["username"],
                    payload["password"],
                    payload["client_id"],
                    int(payload["enabled"]),
                    server_id,
                ),
            )
            if topics is not None:
                self.conn.execute(
                    "DELETE FROM subscriptions WHERE server_id = ?",
                    (server_id,),
                )
                for item in topics:
                    self.conn.execute(
                        """
                        INSERT INTO subscriptions (server_id, topic, qos, enabled)
                        VALUES (?, ?, ?, 1)
                        """,
                        (server_id, item["topic"], item["qos"]),
                    )
            self.conn.commit()
        return self.get_server(server_id)

    def delete_server(self, server_id: int) -> bool:
        with self.lock:
            cur = self.conn.execute("DELETE FROM servers WHERE id = ?", (server_id,))
            self.conn.commit()
            return cur.rowcount > 0

    def add_topic(self, server_id: int, data: dict) -> dict:
        if self.get_server(server_id) is None:
            raise KeyError("Server nicht gefunden")
        topics = normalize_topics([data if not isinstance(data, str) else {"topic": data}])
        if not topics:
            raise ValueError("Topic darf nicht leer sein")
        item = topics[0]
        with self.lock:
            try:
                cur = self.conn.execute(
                    """
                    INSERT INTO subscriptions (server_id, topic, qos, enabled)
                    VALUES (?, ?, ?, 1)
                    """,
                    (server_id, item["topic"], item["qos"]),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Topic ist bereits vorhanden") from exc
            self.conn.commit()
            topic_id = cur.lastrowid
        topic = self._get_topic(server_id, int(topic_id or 0))
        assert topic is not None
        return topic

    def delete_topic(self, server_id: int, topic_id: int) -> bool:
        with self.lock:
            cur = self.conn.execute(
                "DELETE FROM subscriptions WHERE id = ? AND server_id = ?",
                (topic_id, server_id),
            )
            self.conn.commit()
            return cur.rowcount > 0

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def _get_topic(self, server_id: int, topic_id: int) -> dict | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM subscriptions WHERE id = ? AND server_id = ?",
                (topic_id, server_id),
            ).fetchone()
        return None if row is None else self._topic_from_row(row)

    def _insert_server_locked(self, payload: dict) -> int:
        cur = self.conn.execute(
            """
            INSERT INTO servers (
              name, host, port, tls, username, password, client_id, enabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["name"],
                payload["host"],
                payload["port"],
                int(payload["tls"]),
                payload["username"],
                payload["password"],
                payload["client_id"],
                int(payload["enabled"]),
            ),
        )
        server_id = int(cur.lastrowid or 0)
        for item in payload.get("topics") or []:
            self.conn.execute(
                """
                INSERT INTO subscriptions (server_id, topic, qos, enabled)
                VALUES (?, ?, ?, 1)
                """,
                (server_id, item["topic"], item["qos"]),
            )
        return server_id

    def _parse_server(
        self,
        data: dict,
        *,
        partial: bool,
        current: dict | None = None,
    ) -> dict:
        host = _clean_text(data.get("host"))
        if host is None and not partial:
            raise ValueError("Host ist erforderlich")
        if host is None and current is not None:
            host = current["host"]
        if not host:
            raise ValueError("Host ist erforderlich")

        if "port" in data and data["port"] not in (None, ""):
            try:
                port = int(data["port"])
            except (TypeError, ValueError) as exc:
                raise ValueError("Port muss eine Zahl sein") from exc
        elif current is not None:
            port = int(current["port"])
        else:
            port = 8883
        if port < 1 or port > 65535:
            raise ValueError("Port muss zwischen 1 und 65535 liegen")

        if "tls" in data:
            tls = _as_bool(data.get("tls"), port != 1883)
        elif current is not None:
            tls = bool(current["tls"])
        else:
            tls = port != 1883

        name = _clean_text(data.get("name"))
        if name is None and current is not None and "name" not in data:
            name = current["name"]
        if name is None:
            name = host

        username = data.get("username") if "username" in data else (
            current["username"] if current else None
        )
        username = _clean_text(username)

        if "password" in data and _clean_text(data.get("password")):
            password = _clean_text(data.get("password"))
        elif current is not None:
            password = current.get("password")
        else:
            password = None

        client_id = data.get("client_id") if "client_id" in data else (
            current.get("client_id") if current else None
        )
        client_id = _clean_text(client_id)

        if "enabled" in data:
            enabled = _as_bool(data.get("enabled"), True)
        elif current is not None:
            enabled = bool(current["enabled"])
        else:
            enabled = True

        topics: list[dict] | None
        if "topics" in data:
            topics = normalize_topics(data.get("topics")) or []
        elif current is not None:
            topics = None
        else:
            topics = []

        payload = {
            "name": name,
            "host": host,
            "port": port,
            "tls": tls,
            "username": username,
            "password": password,
            "client_id": client_id,
            "enabled": enabled,
        }
        if topics is not None:
            payload["topics"] = topics
        return payload

    @staticmethod
    def _server_from_row(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"],
            "name": row["name"],
            "host": row["host"],
            "port": row["port"],
            "tls": bool(row["tls"]),
            "username": row["username"],
            "password": row["password"],
            "client_id": row["client_id"],
            "enabled": bool(row["enabled"]),
        }

    @staticmethod
    def _topic_from_row(row: sqlite3.Row) -> dict:
        return {
            "id": row["id"],
            "server_id": row["server_id"],
            "topic": row["topic"],
            "qos": row["qos"],
            "enabled": bool(row["enabled"]),
        }


def public_server(server: dict) -> dict:
    topics = [
        {
            "id": item["id"],
            "server_id": item["server_id"],
            "topic": item["topic"],
            "qos": item["qos"],
            "enabled": bool(item.get("enabled", True)),
        }
        for item in server.get("topics") or []
    ]
    return {
        "id": server["id"],
        "name": server["name"],
        "host": server["host"],
        "port": server["port"],
        "tls": bool(server["tls"]),
        "username": server.get("username"),
        "has_password": bool(server.get("password")),
        "client_id": server.get("client_id"),
        "enabled": bool(server.get("enabled", True)),
        "topics": topics,
        "connected": bool(server.get("connected", False)),
    }


def default_client_id(server_id: int) -> str:
    return f"cbs-mqtt-{socket.gethostname()}-{server_id}"
