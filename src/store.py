"""SQLite storage for MQTT messages."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

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
"""

EXTRA_COLUMNS = (
    ("building", "TEXT"),
    ("floor", "TEXT"),
    ("room", "TEXT"),
    ("device", "TEXT"),
    ("metric", "TEXT"),
    ("value_num", "REAL"),
)


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
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

    def insert(
        self,
        received_at: str,
        topic: str,
        payload: str | None,
        qos: int,
        retain: bool,
    ) -> None:
        parsed = parse_topic(topic)
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO messages (
                  received_at, topic, payload, qos, retain,
                  building, floor, room, device, metric, value_num
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()
