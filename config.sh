# ═══════════════════════════════════════════════════════════════
# config.sh — Point this at whatever LLM CLI you have.
# ═══════════════════════════════════════════════════════════════

# ── Pick ONE. Uncomment the line for your setup. ──

# Codex (OpenAI) — free with OAuth login
LLM_CMD="codex exec --dangerously-bypass-approvals-and-sandbox"

# Claude Code (Anthropic) — free with OAuth login
# LLM_CMD="claude -p --dangerously-skip-permissions --max-turns 180 --output-format text"

# Ollama (local, no internet needed)
# LLM_CMD="ollama run llama3.2"

# llm (Simon Willison's CLI — works with any provider)
# LLM_CMD="llm -m gpt-4o"

# aichat (multi-provider CLI)
# LLM_CMD="aichat"

# Any command that takes a prompt as its last argument works.
# The loop does: $LLM_CMD "$PROMPT_TEXT"

# ── Timing ──
SLEEP_SECONDS=300      # seconds between cycles (5 min default)
MAX_TIMEOUT=1800       # kill the LLM call after this many seconds
CYCLE_LOG_KEEP=50      # keep last N cycle logs
