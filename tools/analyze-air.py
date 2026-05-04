#!/usr/bin/env python3
"""Classify indoor air readings and write operational receipts."""

from __future__ import annotations

import csv
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


def parse_float(value: str | None) -> float | None:
    if value is None or value.strip() == "":
        return None
    return float(value)


def load_readings() -> list[Reading]:
    readings: list[Reading] = []
    for path in sorted(CONTEXT.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if not row.get("room"):
                    continue
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
    return readings


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
    return conn


def write_database(results: list[dict], now: str) -> None:
    conn = init_db()
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
    conn.close()


def slug_room(room: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", room.lower()).strip("-")
    return slug or "unknown-room"


def write_outputs(results: list[dict]) -> None:
    DATA.mkdir(exist_ok=True)
    OUTPUT.mkdir(exist_ok=True)
    KNOWLEDGE.mkdir(exist_ok=True)
    ROOMS.mkdir(parents=True, exist_ok=True)

    now = os.environ.get("AIR_AGENT_NOW") or datetime.now(timezone.utc).isoformat()
    order = {"act": 0, "watch": 1, "ok": 2}
    ranked = sorted(results, key=lambda item: (order[item["level"]], item["room"]))

    actions_lines = [f"# Current Air Actions\n\nGenerated: {now}\n"]
    if not ranked:
        actions_lines.append("\nNo readings found in `context/*.csv`.\n")
    else:
        for item in ranked:
            actions_lines.append(f"\n## {item['room']} - {item['level'].upper()}\n")
            actions_lines.append(f"- Source: `{item['source']}` at `{item['timestamp'] or 'unknown time'}`\n")
            actions_lines.append(f"- Evidence: {'; '.join(item['reasons'])}\n")
            actions_lines.append(f"- Action: {'; '.join(item['actions'])}\n")

    (OUTPUT / "current-actions.md").write_text("".join(actions_lines), encoding="utf-8")
    (KNOWLEDGE / "latest.json").write_text(json.dumps({"generated_at": now, "rooms": ranked}, indent=2), encoding="utf-8")
    write_database(ranked, now)

    ledger_lines = [f"\n## Cycle Receipt - {now}\n"]
    if not ranked:
        ledger_lines.append("- No CSV readings found.\n")
    for item in ranked:
        ledger_lines.append(
            f"- {item['level'].upper()} | {item['room']} | {item['source']} | "
            f"{'; '.join(item['reasons'])} | action: {'; '.join(item['actions'])}\n"
        )
    ledger_path = KNOWLEDGE / "air-ledger.md"
    existing_ledger = ledger_path.read_text(encoding="utf-8") if ledger_path.exists() else ""
    if f"## Cycle Receipt - {now}\n" not in existing_ledger:
        with ledger_path.open("a", encoding="utf-8") as handle:
            handle.writelines(ledger_lines)

    for item in ranked:
        if item["level"] == "ok":
            continue
        room_file = ROOMS / f"{slug_room(item['room'])}.md"
        if not room_file.exists():
            room_file.write_text(f"# {item['room']}\n\n", encoding="utf-8")
        entry = (
            f"- {now}: {item['level'].upper()} from `{item['source']}` - "
            f"{'; '.join(item['reasons'])}. Action: {'; '.join(item['actions'])}\n"
        )
        if entry not in room_file.read_text(encoding="utf-8"):
            with room_file.open("a", encoding="utf-8") as handle:
                handle.write(entry)


def main() -> int:
    readings = load_readings()
    thresholds = load_thresholds()
    results = []
    for reading in readings:
        level, reasons, actions = severity(reading, thresholds)
        results.append(
            {
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
    write_outputs(results)
    print(f"processed {len(results)} readings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
