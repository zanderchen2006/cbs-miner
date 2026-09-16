"""Parse CBS Koblenz MQTT topics and sensor payloads."""

from __future__ import annotations

NAMED_SEGMENTS = ("building", "floor", "room", "device")
TOPIC_PREFIX = "cbs_koblenz"


def parse_topic(topic: str) -> dict[str, str | None]:
    """Split cbs_koblenz/building/A/floor/3/room/11/device/esp12f-01/humidity."""
    out: dict[str, str | None] = {
        "building": None,
        "floor": None,
        "room": None,
        "device": None,
        "metric": None,
    }
    parts = [part for part in str(topic).split("/") if part]
    if parts and parts[0] == TOPIC_PREFIX:
        parts = parts[1:]
    i = 0
    named = set(NAMED_SEGMENTS)
    while i < len(parts):
        token = parts[i]
        if token in named and i + 1 < len(parts):
            out[token] = parts[i + 1]
            i += 2
            continue
        out["metric"] = "/".join(parts[i:])
        break
    return out


def parse_value(payload: object) -> float | None:
    if payload is None:
        return None
    if isinstance(payload, bool):
        return None
    if isinstance(payload, (int, float)):
        if payload != payload:  # NaN
            return None
        return float(payload)
    text = str(payload).strip()
    if not text or text.lower() in {"null", "none", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def decode_payload(raw: bytes) -> str | None:
    if not raw:
        return None
    return raw.decode("utf-8", errors="replace")


def item_from(
    topic: str,
    payload: str | None,
    qos: int,
    retain: bool | int,
    received_at: str,
    server_id: int | None = None,
    server_name: str | None = None,
) -> dict:
    parsed = parse_topic(topic)
    return {
        "received_at": received_at,
        "topic": topic,
        "payload": payload,
        "qos": qos,
        "retain": int(retain),
        "building": parsed["building"],
        "floor": parsed["floor"],
        "room": parsed["room"],
        "device": parsed["device"],
        "metric": parsed["metric"],
        "value_num": parse_value(payload),
        "server_id": server_id,
        "server_name": server_name,
    }
