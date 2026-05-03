#!/usr/bin/env python3
"""Classify indoor air readings and write operational receipts."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTEXT = ROOT / "context"
OUTPUT = ROOT / "output"
KNOWLEDGE = ROOT / "knowledge"
ROOMS = KNOWLEDGE / "rooms"

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


def severity(reading: Reading) -> tuple[str, list[str], list[str]]:
    reasons: list[str] = []
    actions: list[str] = []
    level = "ok"

    def raise_to(candidate: str) -> None:
        nonlocal level
        order = {"ok": 0, "watch": 1, "act": 2}
        if order[candidate] > order[level]:
            level = candidate

    if reading.co2_ppm is not None:
        if reading.co2_ppm >= CO2_ACT:
            raise_to("act")
            reasons.append(f"CO2 {reading.co2_ppm:.0f} ppm >= {CO2_ACT:.0f}")
            actions.append("increase outdoor air now; verify HVAC mode; re-test in 15 minutes")
        elif reading.co2_ppm >= CO2_WATCH:
            raise_to("watch")
            reasons.append(f"CO2 {reading.co2_ppm:.0f} ppm >= {CO2_WATCH:.0f}")
            actions.append("increase ventilation before the next occupied period")

    if reading.pm25_ugm3 is not None:
        if reading.pm25_ugm3 >= PM25_ACT:
            raise_to("act")
            reasons.append(f"PM2.5 {reading.pm25_ugm3:.1f} ug/m3 >= {PM25_ACT:.0f}")
            actions.append("check filtration and recent emission sources; avoid high-exertion use")
        elif reading.pm25_ugm3 >= PM25_WATCH:
            raise_to("watch")
            reasons.append(f"PM2.5 {reading.pm25_ugm3:.1f} ug/m3 >= {PM25_WATCH:.0f}")
            actions.append("inspect filters and compare with outdoor conditions")

    if reading.temp_c is not None:
        if reading.temp_c <= TEMP_LOW_ACT or reading.temp_c >= TEMP_HIGH_ACT:
            raise_to("act")
            reasons.append(f"temperature {reading.temp_c:.1f} C outside {TEMP_LOW_ACT:.0f}-{TEMP_HIGH_ACT:.0f}")
            actions.append("adjust heating or cooling before occupancy")
        elif reading.temp_c <= TEMP_LOW_WATCH or reading.temp_c >= TEMP_HIGH_WATCH:
            raise_to("watch")
            reasons.append(f"temperature {reading.temp_c:.1f} C outside {TEMP_LOW_WATCH:.0f}-{TEMP_HIGH_WATCH:.0f}")

    if reading.rh_percent is not None:
        if reading.rh_percent <= RH_LOW_ACT or reading.rh_percent >= RH_HIGH_ACT:
            raise_to("act")
            reasons.append(f"humidity {reading.rh_percent:.0f}% outside {RH_LOW_ACT:.0f}-{RH_HIGH_ACT:.0f}")
            actions.append("check humidification, leaks, or condensation risk")
        elif reading.rh_percent <= RH_LOW_WATCH or reading.rh_percent >= RH_HIGH_WATCH:
            raise_to("watch")
            reasons.append(f"humidity {reading.rh_percent:.0f}% outside {RH_LOW_WATCH:.0f}-{RH_HIGH_WATCH:.0f}")

    if not reasons:
        reasons.append("all supplied readings inside watch thresholds")
        actions.append("keep normal ventilation schedule and continue logging")

    deduped_actions = list(dict.fromkeys(actions))
    return level, reasons, deduped_actions


def slug_room(room: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", room.lower()).strip("-")
    return slug or "unknown-room"


def write_outputs(results: list[dict]) -> None:
    OUTPUT.mkdir(exist_ok=True)
    KNOWLEDGE.mkdir(exist_ok=True)
    ROOMS.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc).isoformat()
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

    ledger_lines = [f"\n## Cycle Receipt - {now}\n"]
    if not ranked:
        ledger_lines.append("- No CSV readings found.\n")
    for item in ranked:
        ledger_lines.append(
            f"- {item['level'].upper()} | {item['room']} | {item['source']} | "
            f"{'; '.join(item['reasons'])} | action: {'; '.join(item['actions'])}\n"
        )
    with (KNOWLEDGE / "air-ledger.md").open("a", encoding="utf-8") as handle:
        handle.writelines(ledger_lines)

    for item in ranked:
        if item["level"] == "ok":
            continue
        room_file = ROOMS / f"{slug_room(item['room'])}.md"
        if not room_file.exists():
            room_file.write_text(f"# {item['room']}\n\n", encoding="utf-8")
        with room_file.open("a", encoding="utf-8") as handle:
            handle.write(
                f"- {now}: {item['level'].upper()} from `{item['source']}` - "
                f"{'; '.join(item['reasons'])}. Action: {'; '.join(item['actions'])}\n"
            )


def main() -> int:
    readings = load_readings()
    results = []
    for reading in readings:
        level, reasons, actions = severity(reading)
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
