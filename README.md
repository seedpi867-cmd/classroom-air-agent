# classroom-air-agent

An autonomous bash-loop agent for indoor air-quality operations.

It watches cheap sensor exports, classifies each room, writes a receipt, and produces a short action list. The point is not to make a dashboard. The point is to give schools, libraries, studios, clinics, and community rooms a tireless little operator that notices when the air has gone bad and records what evidence it used.

Built on [`brain-loop`](https://github.com/seedpi867-cmd/brain-loop): one shell loop, files as database, any LLM CLI.

## What it reads

Drop CSV files into `context/`.

Required columns:

```csv
timestamp,room,co2_ppm,pm25_ugm3,temp_c,rh_percent
```

Optional columns are ignored unless future tools use them.

## What it writes

- `output/current-actions.md` - the current ranked action list.
- `knowledge/air-ledger.md` - append-only cycle receipts.
- `knowledge/latest.json` - machine-readable latest classification.
- `knowledge/rooms/<room>.md` - room history when a room crosses thresholds.
- `data/air.sqlite` - structured reading and classification history.

## Thresholds

Defaults are deliberately simple and editable in `config/thresholds.json`:

| Signal | Watch | Act |
| --- | ---: | ---: |
| CO2 | 1000 ppm | 1500 ppm |
| PM2.5 | 12 ug/m3 | 35 ug/m3 |
| Temperature | below 18 C or above 27 C | below 16 C or above 30 C |
| Humidity | below 30% or above 60% | below 25% or above 70% |

This is not a medical or regulatory determination. It is an operational triage tool.

## Quick start

```bash
git clone https://github.com/seedpi867-cmd/classroom-air-agent.git
cd classroom-air-agent
python3 tools/analyze-air.py
cat output/current-actions.md
```

To run the full loop:

```bash
nano config.sh
chmod +x brain-loop.sh
./brain-loop.sh
```

## Why this should exist

Bad air is usually invisible until someone complains. A cheap sensor is useful, but a sensor without a loop becomes another unread graph. This agent turns readings into a small operational memory: what room, what threshold, what action, what happened next.
