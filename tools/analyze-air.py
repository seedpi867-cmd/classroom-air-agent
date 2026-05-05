#!/usr/bin/env python3
"""Classify indoor air readings and write operational receipts."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config"
CONTEXT = ROOT / "context"
DATA = ROOT / "data"
OUTPUT = ROOT / "output"
KNOWLEDGE = ROOT / "knowledge"
ROOMS = KNOWLEDGE / "rooms"
DB_PATH = DATA / "air.sqlite"
ADMISSION_LEDGER = KNOWLEDGE / "admission-ledger.md"
RECOVERY_LEDGER = KNOWLEDGE / "recovery-ledger.md"
REQUIRED_COLUMNS = {"timestamp", "room", "co2_ppm", "pm25_ugm3", "temp_c", "rh_percent"}
RECOVERY_COLUMNS = {"action", "receipt_id", "reason"}

CO2_WATCH = 1000.0
CO2_ACT = 1500.0
PM25_WATCH = 12.0
PM25_ACT = 35.0
TEMP_LOW_WATCH = 18.0
TEMP_HIGH_WATCH = 27.0
TEMP_LOW_ACT = 16.0
TEMP_HIGH_ACT = 30.0
RH_LOW_WATCH = 30.0
RH_HIGH_WATCH = 60.0
RH_LOW_ACT = 25.0
RH_HIGH_ACT = 70.0

DEFAULT_THRESHOLDS = {
    "co2_ppm": {"watch": CO2_WATCH, "act": CO2_ACT},
    "pm25_ugm3": {"watch": PM25_WATCH, "act": PM25_ACT},
    "temp_c": {
        "watch_low": TEMP_LOW_WATCH,
        "watch_high": TEMP_HIGH_WATCH,
        "act_low": TEMP_LOW_ACT,
        "act_high": TEMP_HIGH_ACT,
    },
    "rh_percent": {
        "watch_low": RH_LOW_WATCH,
        "watch_high": RH_HIGH_WATCH,
        "act_low": RH_LOW_ACT,
        "act_high": RH_HIGH_ACT,
    },
}


@dataclass
class Reading:
    source: str
    timestamp: str
    room: str
    co2_ppm: float | None
    pm25_ugm3: float | None
    temp_c: float | None
    rh_percent: float | None


@dataclass
class RecoveryDirective:
    source: str
    action: str
    receipt_id: str
    reason: str


def parse_float(value: str | None) -> float | None:
    if value is None or value.strip() == "":
        return None
    return float(value)


def current_time() -> str:
    return os.environ.get("AIR_AGENT_NOW") or datetime.now(timezone.utc).isoformat()


def is_recovery_csv(path: Path) -> bool:
    name = path.name.lower()
    return "retraction" in name or "recovery" in name


def reading_receipt_id(reading: Reading) -> str:
    payload = {
        "source": reading.source,
        "timestamp": reading.timestamp,
        "room": reading.room,
        "co2_ppm": reading.co2_ppm,
        "pm25_ugm3": reading.pm25_ugm3,
        "temp_c": reading.temp_c,
        "rh_percent": reading.rh_percent,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def record_admission_rejections(rejections: list[str], now: str) -> None:
    if not rejections:
        return
    KNOWLEDGE.mkdir(exist_ok=True)
    existing = ADMISSION_LEDGER.read_text(encoding="utf-8") if ADMISSION_LEDGER.exists() else "# Admission Ledger\n\nRejected input receipts.\n"
    lines = [f"\n## Admission Gate - {now}\n"]
    for rejection in rejections:
        lines.append(f"- DENY | {rejection}\n")
    block = "".join(lines)
    if block not in existing:
        needs_header = not ADMISSION_LEDGER.exists() or ADMISSION_LEDGER.stat().st_size == 0
        with ADMISSION_LEDGER.open("a", encoding="utf-8") as handle:
            if needs_header:
                handle.write("# Admission Ledger\n\nRejected input receipts.\n")
            handle.write(block)


def load_readings() -> list[Reading]:
    readings: list[Reading] = []
    rejections: list[str] = []
    now = current_time()
    for path in sorted(CONTEXT.glob("*.csv")):
        if is_recovery_csv(path):
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = set(reader.fieldnames or [])
            missing = sorted(REQUIRED_COLUMNS - fieldnames)
            if missing:
                rejections.append(f"{path.name} | missing required columns: {', '.join(missing)}")
                continue
            for row in reader:
                if not row.get("room"):
                    rejections.append(f"{path.name} | row denied: missing room")
                    continue
                try:
                    readings.append(
                        Reading(
                            source=path.name,
                            timestamp=(row.get("timestamp") or "").strip(),
                            room=(row.get("room") or "").strip(),
                            co2_ppm=parse_float(row.get("co2_ppm")),
                            pm25_ugm3=parse_float(row.get("pm25_ugm3")),
                            temp_c=parse_float(row.get("temp_c")),
                            rh_percent=parse_float(row.get("rh_percent")),
                        )
                    )
                except ValueError as exc:
                    rejections.append(f"{path.name} | row denied: invalid numeric field ({exc})")
    record_admission_rejections(rejections, now)
    return readings


def load_recovery_directives() -> list[RecoveryDirective]:
    directives: list[RecoveryDirective] = []
    rejections: list[str] = []
    now = current_time()
    for path in sorted(CONTEXT.glob("*retraction*.csv")) + sorted(CONTEXT.glob("*recovery*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = set(reader.fieldnames or [])
            missing = sorted(RECOVERY_COLUMNS - fieldnames)
            if missing:
                rejections.append(f"{path.name} | missing required recovery columns: {', '.join(missing)}")
                continue
            for row in reader:
                action = (row.get("action") or "").strip().lower()
                receipt_id = (row.get("receipt_id") or "").strip()
                reason = (row.get("reason") or "").strip()
                if action != "retract":
                    rejections.append(f"{path.name} | row denied: unsupported recovery action `{action or 'blank'}`")
                    continue
                if not receipt_id or not reason:
                    rejections.append(f"{path.name} | row denied: receipt_id and reason are required")
                    continue
                directives.append(RecoveryDirective(path.name, action, receipt_id, reason))
    record_admission_rejections(rejections, now)
    return directives


def load_thresholds() -> dict:
    path = CONFIG / "thresholds.json"
    if not path.exists():
        return DEFAULT_THRESHOLDS
    configured = json.loads(path.read_text(encoding="utf-8"))
    thresholds = json.loads(json.dumps(DEFAULT_THRESHOLDS))
    for signal, values in configured.items():
        if isinstance(values, dict) and signal in thresholds:
            thresholds[signal].update(values)
    return thresholds


def severity(reading: Reading, thresholds: dict) -> tuple[str, list[str], list[str]]:
    reasons: list[str] = []
    actions: list[str] = []
    level = "ok"

    def raise_to(candidate: str) -> None:
        nonlocal level
        order = {"ok": 0, "watch": 1, "act": 2}
        if order[candidate] > order[level]:
            level = candidate

    if reading.co2_ppm is not None:
        co2_act = float(thresholds["co2_ppm"]["act"])
        co2_watch = float(thresholds["co2_ppm"]["watch"])
        if reading.co2_ppm >= co2_act:
            raise_to("act")
            reasons.append(f"CO2 {reading.co2_ppm:.0f} ppm >= {co2_act:.0f}")
            actions.append("increase outdoor air now; verify HVAC mode; re-test in 15 minutes")
        elif reading.co2_ppm >= co2_watch:
            raise_to("watch")
            reasons.append(f"CO2 {reading.co2_ppm:.0f} ppm >= {co2_watch:.0f}")
            actions.append("increase ventilation before the next occupied period")

    if reading.pm25_ugm3 is not None:
        pm25_act = float(thresholds["pm25_ugm3"]["act"])
        pm25_watch = float(thresholds["pm25_ugm3"]["watch"])
        if reading.pm25_ugm3 >= pm25_act:
            raise_to("act")
            reasons.append(f"PM2.5 {reading.pm25_ugm3:.1f} ug/m3 >= {pm25_act:.0f}")
            actions.append("check filtration and recent emission sources; avoid high-exertion use")
        elif reading.pm25_ugm3 >= pm25_watch:
            raise_to("watch")
            reasons.append(f"PM2.5 {reading.pm25_ugm3:.1f} ug/m3 >= {pm25_watch:.0f}")
            actions.append("inspect filters and compare with outdoor conditions")

    if reading.temp_c is not None:
        temp_act_low = float(thresholds["temp_c"]["act_low"])
        temp_act_high = float(thresholds["temp_c"]["act_high"])
        temp_watch_low = float(thresholds["temp_c"]["watch_low"])
        temp_watch_high = float(thresholds["temp_c"]["watch_high"])
        if reading.temp_c <= temp_act_low or reading.temp_c >= temp_act_high:
            raise_to("act")
            reasons.append(f"temperature {reading.temp_c:.1f} C outside {temp_act_low:.0f}-{temp_act_high:.0f}")
            actions.append("adjust heating or cooling before occupancy")
        elif reading.temp_c <= temp_watch_low or reading.temp_c >= temp_watch_high:
            raise_to("watch")
            reasons.append(f"temperature {reading.temp_c:.1f} C outside {temp_watch_low:.0f}-{temp_watch_high:.0f}")

    if reading.rh_percent is not None:
        rh_act_low = float(thresholds["rh_percent"]["act_low"])
        rh_act_high = float(thresholds["rh_percent"]["act_high"])
        rh_watch_low = float(thresholds["rh_percent"]["watch_low"])
        rh_watch_high = float(thresholds["rh_percent"]["watch_high"])
        if reading.rh_percent <= rh_act_low or reading.rh_percent >= rh_act_high:
            raise_to("act")
            reasons.append(f"humidity {reading.rh_percent:.0f}% outside {rh_act_low:.0f}-{rh_act_high:.0f}")
            actions.append("check humidification, leaks, or condensation risk")
        elif reading.rh_percent <= rh_watch_low or reading.rh_percent >= rh_watch_high:
            raise_to("watch")
            reasons.append(f"humidity {reading.rh_percent:.0f}% outside {rh_watch_low:.0f}-{rh_watch_high:.0f}")

    if not reasons:
        reasons.append("all supplied readings inside watch thresholds")
        actions.append("keep normal ventilation schedule and continue logging")

    deduped_actions = list(dict.fromkeys(actions))
    return level, reasons, deduped_actions


def init_db() -> sqlite3.Connection:
    DATA.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_at TEXT NOT NULL,
            source TEXT NOT NULL,
            timestamp TEXT,
            room TEXT NOT NULL,
            co2_ppm REAL,
            pm25_ugm3 REAL,
            temp_c REAL,
            rh_percent REAL,
            level TEXT NOT NULL,
            reasons_json TEXT NOT NULL,
            actions_json TEXT NOT NULL,
            UNIQUE(generated_at, source, timestamp, room)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_readings_room_time
        ON readings(room, generated_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reading_receipts (
            receipt_id TEXT PRIMARY KEY,
            admitted_at TEXT NOT NULL,
            source TEXT NOT NULL,
            timestamp TEXT,
            room TEXT NOT NULL,
            co2_ppm REAL,
            pm25_ugm3 REAL,
            temp_c REAL,
            rh_percent REAL,
            level TEXT NOT NULL,
            reasons_json TEXT NOT NULL,
            actions_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'accepted',
            supersedes_receipt_id TEXT,
            superseded_by_receipt_id TEXT,
            recovery_reason TEXT,
            recovered_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_reading_receipts_room_status
        ON reading_receipts(room, status, timestamp, admitted_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recovery_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            generated_at TEXT NOT NULL,
            action TEXT NOT NULL,
            receipt_id TEXT NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            outcome TEXT NOT NULL
        )
        """
    )
    return conn


def append_recovery_ledger(events: list[str], now: str) -> None:
    if not events:
        return
    existing = RECOVERY_LEDGER.read_text(encoding="utf-8") if RECOVERY_LEDGER.exists() else ""
    block = f"\n## Recovery Run - {now}\n" + "".join(events)
    if block not in existing:
        needs_header = not RECOVERY_LEDGER.exists() or RECOVERY_LEDGER.stat().st_size == 0
        with RECOVERY_LEDGER.open("a", encoding="utf-8") as handle:
            if needs_header:
                handle.write("# Recovery Ledger\n\nRetracted and superseded receipt events.\n")
            handle.write(block)


def write_database(results: list[dict], now: str) -> list[str]:
    conn = init_db()
    recovery_events: list[str] = []
    with conn:
        for item in results:
            readings = item["readings"]
            conn.execute(
                """
                INSERT OR IGNORE INTO readings (
                    generated_at, source, timestamp, room,
                    co2_ppm, pm25_ugm3, temp_c, rh_percent,
                    level, reasons_json, actions_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    item["source"],
                    item["timestamp"],
                    item["room"],
                    readings["co2_ppm"],
                    readings["pm25_ugm3"],
                    readings["temp_c"],
                    readings["rh_percent"],
                    item["level"],
                    json.dumps(item["reasons"], sort_keys=True),
                    json.dumps(item["actions"], sort_keys=True),
                ),
            )
            existing = conn.execute(
                """
                SELECT receipt_id FROM reading_receipts
                WHERE source = ? AND timestamp = ? AND room = ? AND status = 'accepted' AND receipt_id != ?
                ORDER BY admitted_at DESC
                """,
                (item["source"], item["timestamp"], item["room"], item["receipt_id"]),
            ).fetchall()
            supersedes = existing[0][0] if existing else None
            inserted = conn.execute(
                """
                INSERT OR IGNORE INTO reading_receipts (
                    receipt_id, admitted_at, source, timestamp, room,
                    co2_ppm, pm25_ugm3, temp_c, rh_percent,
                    level, reasons_json, actions_json, status, supersedes_receipt_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?)
                """,
                (
                    item["receipt_id"],
                    now,
                    item["source"],
                    item["timestamp"],
                    item["room"],
                    readings["co2_ppm"],
                    readings["pm25_ugm3"],
                    readings["temp_c"],
                    readings["rh_percent"],
                    item["level"],
                    json.dumps(item["reasons"], sort_keys=True),
                    json.dumps(item["actions"], sort_keys=True),
                    supersedes,
                ),
            )
            if existing and inserted.rowcount:
                conn.execute(
                    """
                    UPDATE reading_receipts
                    SET status = 'superseded',
                        superseded_by_receipt_id = ?,
                        recovered_at = ?,
                        recovery_reason = 'superseded by newer admitted receipt for same source/time/room'
                    WHERE source = ? AND timestamp = ? AND room = ? AND status = 'accepted' AND receipt_id != ?
                    """,
                    (item["receipt_id"], now, item["source"], item["timestamp"], item["room"], item["receipt_id"]),
                )
                for old_receipt in existing:
                    reason = "superseded by newer admitted receipt for same source/time/room"
                    conn.execute(
                        """
                        INSERT INTO recovery_events (generated_at, action, receipt_id, source, reason, outcome)
                        VALUES (?, 'supersede', ?, ?, ?, ?)
                        """,
                        (
                            now,
                            old_receipt[0],
                            item["source"],
                            reason,
                            f"superseded by {item['receipt_id']}",
                        ),
                    )
                    recovery_events.append(
                        f"- SUPERSEDE | `{old_receipt[0]}` | superseded by `{item['receipt_id']}` | "
                        f"reason: {reason}\n"
                    )
    conn.close()
    append_recovery_ledger(recovery_events, now)
    return recovery_events


def apply_recovery_directives(directives: list[RecoveryDirective], now: str) -> list[str]:
    if not directives:
        return []
    conn = init_db()
    events: list[str] = []
    with conn:
        for directive in directives:
            row = conn.execute(
                "SELECT status, room, source FROM reading_receipts WHERE receipt_id = ?",
                (directive.receipt_id,),
            ).fetchone()
            if row is None:
                outcome = "ignored: unknown receipt"
            elif row[0] == "accepted":
                conn.execute(
                    """
                    UPDATE reading_receipts
                    SET status = 'retracted', recovery_reason = ?, recovered_at = ?
                    WHERE receipt_id = ?
                    """,
                    (directive.reason, now, directive.receipt_id),
                )
                outcome = f"retracted {row[1]} from {row[2]}"
            else:
                outcome = f"ignored: already {row[0]}"
            conn.execute(
                """
                INSERT INTO recovery_events (generated_at, action, receipt_id, source, reason, outcome)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (now, directive.action, directive.receipt_id, directive.source, directive.reason, outcome),
            )
            events.append(
                f"- {directive.action.upper()} | `{directive.receipt_id}` | {outcome} | reason: {directive.reason}\n"
            )
    conn.close()
    append_recovery_ledger(events, now)
    return events


def slug_room(room: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", room.lower()).strip("-")
    return slug or "unknown-room"


def row_to_result(row: sqlite3.Row | tuple) -> dict:
    return {
        "receipt_id": row["receipt_id"],
        "source": row["source"],
        "timestamp": row["timestamp"],
        "room": row["room"],
        "level": row["level"],
        "reasons": json.loads(row["reasons_json"]),
        "actions": json.loads(row["actions_json"]),
        "readings": {
            "co2_ppm": row["co2_ppm"],
            "pm25_ugm3": row["pm25_ugm3"],
            "temp_c": row["temp_c"],
            "rh_percent": row["rh_percent"],
        },
    }


def accepted_current_results() -> list[dict]:
    conn = init_db()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT r.*
        FROM reading_receipts r
        WHERE r.status = 'accepted'
          AND NOT EXISTS (
            SELECT 1 FROM reading_receipts newer
            WHERE newer.status = 'accepted'
              AND newer.room = r.room
              AND (
                COALESCE(newer.timestamp, '') > COALESCE(r.timestamp, '')
                OR (
                  COALESCE(newer.timestamp, '') = COALESCE(r.timestamp, '')
                  AND newer.admitted_at > r.admitted_at
                )
              )
          )
        ORDER BY r.room
        """
    ).fetchall()
    conn.close()
    return [row_to_result(row) for row in rows]


def accepted_room_history() -> list[dict]:
    conn = init_db()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT * FROM reading_receipts
        WHERE status = 'accepted' AND level != 'ok'
        ORDER BY room, timestamp, admitted_at
        """
    ).fetchall()
    conn.close()
    return [row_to_result(row) for row in rows]


def write_outputs(results: list[dict], now: str) -> None:
    DATA.mkdir(exist_ok=True)
    OUTPUT.mkdir(exist_ok=True)
    KNOWLEDGE.mkdir(exist_ok=True)
    ROOMS.mkdir(parents=True, exist_ok=True)

    order = {"act": 0, "watch": 1, "ok": 2}
    ranked = sorted(results, key=lambda item: (order[item["level"]], item["room"]))

    actions_lines = [f"# Current Air Actions\n\nGenerated: {now}\n"]
    if not ranked:
        actions_lines.append("\nNo accepted readings available after recovery.\n")
    else:
        for item in ranked:
            actions_lines.append(f"\n## {item['room']} - {item['level'].upper()}\n")
            actions_lines.append(f"- Receipt: `{item['receipt_id']}`\n")
            actions_lines.append(f"- Source: `{item['source']}` at `{item['timestamp'] or 'unknown time'}`\n")
            actions_lines.append(f"- Evidence: {'; '.join(item['reasons'])}\n")
            actions_lines.append(f"- Action: {'; '.join(item['actions'])}\n")

    (OUTPUT / "current-actions.md").write_text("".join(actions_lines), encoding="utf-8")
    (KNOWLEDGE / "latest.json").write_text(json.dumps({"generated_at": now, "rooms": ranked}, indent=2), encoding="utf-8")

    ledger_lines = [f"\n## Cycle Receipt - {now}\n"]
    if not ranked:
        ledger_lines.append("- No accepted readings available after recovery.\n")
    for item in ranked:
        ledger_lines.append(
            f"- {item['level'].upper()} | `{item['receipt_id']}` | {item['room']} | {item['source']} | "
            f"{'; '.join(item['reasons'])} | action: {'; '.join(item['actions'])}\n"
        )
    ledger_path = KNOWLEDGE / "air-ledger.md"
    existing_ledger = ledger_path.read_text(encoding="utf-8") if ledger_path.exists() else ""
    if f"## Cycle Receipt - {now}\n" not in existing_ledger:
        with ledger_path.open("a", encoding="utf-8") as handle:
            handle.writelines(ledger_lines)

    for room_file in ROOMS.glob("*.md"):
        room_file.unlink()
    for item in accepted_room_history():
        room_file = ROOMS / f"{slug_room(item['room'])}.md"
        if not room_file.exists():
            room_file.write_text(f"# {item['room']}\n\n", encoding="utf-8")
        entry = (
            f"- {item['timestamp'] or item['receipt_id']}: {item['level'].upper()} "
            f"receipt `{item['receipt_id']}` from `{item['source']}` - "
            f"{'; '.join(item['reasons'])}. Action: {'; '.join(item['actions'])}\n"
        )
        if entry not in room_file.read_text(encoding="utf-8"):
            with room_file.open("a", encoding="utf-8") as handle:
                handle.write(entry)


def main() -> int:
    readings = load_readings()
    recovery_directives = load_recovery_directives()
    thresholds = load_thresholds()
    results = []
    for reading in readings:
        level, reasons, actions = severity(reading, thresholds)
        results.append(
            {
                "receipt_id": reading_receipt_id(reading),
                "source": reading.source,
                "timestamp": reading.timestamp,
                "room": reading.room,
                "level": level,
                "reasons": reasons,
                "actions": actions,
                "readings": {
                    "co2_ppm": reading.co2_ppm,
                    "pm25_ugm3": reading.pm25_ugm3,
                    "temp_c": reading.temp_c,
                    "rh_percent": reading.rh_percent,
                },
            }
        )
    now = current_time()
    supersession_events = write_database(results, now)
    recovery_events = apply_recovery_directives(recovery_directives, now)
    current_results = accepted_current_results()
    write_outputs(current_results, now)
    print(
        f"processed {len(results)} readings; "
        f"applied {len(supersession_events) + len(recovery_events)} recovery events; "
        f"rebuilt {len(current_results)} current accepted rooms"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
