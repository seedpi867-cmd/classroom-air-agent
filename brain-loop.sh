#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# brain-loop.sh — The entire agent system in one file.
#
# A bash loop that wakes up, builds a prompt from files on disk,
# sends it through whatever LLM CLI you have, and sleeps.
# That's it. No frameworks. No API keys. No dependencies.
#
# Works with: codex, claude, ollama, llm, aichat, sgpt, or
# any CLI that takes text in and returns text out.
# ═══════════════════════════════════════════════════════════════
set -o pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
source "$ROOT/config.sh" 2>/dev/null || true

# Defaults — override in config.sh
: "${LLM_CMD:=codex exec --dangerously-bypass-approvals-and-sandbox}"
: "${SLEEP_SECONDS:=300}"
: "${MAX_TIMEOUT:=1800}"
: "${CYCLE_LOG_KEEP:=50}"

mkdir -p "$ROOT/data/logs" "$ROOT/context" "$ROOT/output" "$ROOT/knowledge"

# Singleton
LOCKFILE="/tmp/brain-loop-$(echo "$ROOT" | md5sum | cut -c1-8).lock"
exec 200>"$LOCKFILE" || exit 1
flock -n 200 || { echo "[agent] Already running."; exit 1; }

CYCLE=0
[ -f "$ROOT/data/cycle.txt" ] && CYCLE=$(cat "$ROOT/data/cycle.txt" 2>/dev/null || echo 0)

echo "[agent] Brain loop starting (PID $$)"

while true; do
    CYCLE=$((CYCLE + 1))
    echo "$CYCLE" > "$ROOT/data/cycle.txt"
    LOG="$ROOT/data/logs/cycle_${CYCLE}.log"
    echo "[agent] ── CYCLE $CYCLE ── $(date '+%Y-%m-%d %H:%M:%S')"

    # ── BUILD THE PROMPT ─────────────────────────────────
    PROMPT="$ROOT/tmp_prompt.md"

    # Identity — who is this agent
    cat "$ROOT/AGENT.md" > "$PROMPT" 2>/dev/null

    # Memory — what happened before
    if [ -f "$ROOT/data/memory.md" ]; then
        printf '\n---\n\n## Recent Memory\n' >> "$PROMPT"
        tail -30 "$ROOT/data/memory.md" >> "$PROMPT"
    fi

    # Context — fresh input this cycle
    printf '\n---\n\n## Context\n' >> "$PROMPT"
    for f in "$ROOT/context"/*.md "$ROOT/context"/*.txt; do
        [ -f "$f" ] && printf '\n### %s\n' "$(basename "$f")" >> "$PROMPT" && cat "$f" >> "$PROMPT"
    done

    # Tasks
    if [ -f "$ROOT/data/tasks.md" ]; then
        printf '\n---\n\n## Tasks\n' >> "$PROMPT"
        cat "$ROOT/data/tasks.md" >> "$PROMPT"
    fi

    # Instructions for this cycle
    printf '\n---\n\n## Instructions\n' >> "$PROMPT"
    cat "$ROOT/INSTRUCTIONS.md" >> "$PROMPT" 2>/dev/null

    # ── CALL THE LLM ─────────────────────────────────────
    echo "[agent] Calling LLM..." | tee -a "$LOG"

    cd "$ROOT"
    PROMPT_TEXT=$(cat "$PROMPT")
    timeout "$MAX_TIMEOUT" $LLM_CMD "$PROMPT_TEXT" >> "$LOG" 2>&1 || true

    echo "[agent] LLM done"

    # ── POST-CYCLE ───────────────────────────────────────
    # Append cycle summary to memory
    echo "- Cycle $CYCLE ($(date '+%H:%M')): completed" >> "$ROOT/data/memory.md"

    # Trim memory to last 100 lines
    if [ -f "$ROOT/data/memory.md" ]; then
        tail -100 "$ROOT/data/memory.md" > "$ROOT/data/memory.md.tmp"
        mv "$ROOT/data/memory.md.tmp" "$ROOT/data/memory.md"
    fi

    # Rotate logs
    ls -t "$ROOT/data/logs"/cycle_*.log 2>/dev/null | tail -n +$((CYCLE_LOG_KEEP + 1)) | xargs rm -f 2>/dev/null

    rm -f "$PROMPT"

    # ── SLEEP ────────────────────────────────────────────
    echo "[agent] Sleeping ${SLEEP_SECONDS}s..."
    sleep "$SLEEP_SECONDS"
done
