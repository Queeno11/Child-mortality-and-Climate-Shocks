#!/usr/bin/env bash
#
# guard-edits.sh — PreToolUse guard for WSL2 (bash port of guard-edits.ps1).
#
# This is the SECOND, semantic layer of defense. The FIRST layer is the OS-level
# sandbox.filesystem.denyWrite block in ../settings.json, enforced by bubblewrap
# for every Bash command — that's what actually stops `rm`, `cp`, redirects,
# etc. from touching Data/, .claude/, .git/config, .git/hooks, or the *.zip
# archives. This script exists for the policy the OS boundary can't express:
# "git commit is fine anywhere, but push/reset --hard/rebase/merge/history-
# rewrites are never fine" — a directory allow/deny list can't distinguish
# git subcommands, so that has to be done here by inspecting the command text.
#
# Dispatches on tool_name:
#
#   Edit/Write/MultiEdit/NotebookEdit:
#     Default-ALLOW anywhere inside the project, deny-list Data/, .git/,
#     .claude/, and *.zip, and anything outside the project root. (Redundant
#     with permissions.deny in settings.json — belt and suspenders, and this
#     one also catches path-traversal tricks.)
#
#   Bash:
#     git guard — denies destructive/structure-changing git ops (push, reset
#       --hard/--keep, rebase, merge, filter-branch/filter-repo, update-ref,
#       symbolic-ref, reflog expire, gc, branch/tag deletion) and hook-bypass
#       flags (--no-verify, core.hooksPath), on ANY branch. Normal add/commit/
#       status/diff/log/checkout is fine — that's the "ok for scratching" part.
#
# Wired in .claude/settings.json on the Edit|Write|MultiEdit|NotebookEdit|Bash
# matcher. Fails CLOSED (deny) on an unparseable/missing payload or a missing
# python3 — an unreadable input must not slip past the sandbox.
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}"

deny() {
  printf '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"%s"}}\n' "$1"
  exit 0
}

command -v python3 >/dev/null 2>&1 || deny "Sandbox guard: python3 not found; blocking tool call as a precaution."

ROOT="$ROOT" python3 -c '
import sys, os, re, json

# === EDIT THESE ==============================================================
PROTECTED_DIRS = ["data", ".git", ".claude"]     # compared case-insensitively, relative to root
PROTECTED_EXT  = [".zip"]
DESTRUCTIVE    = r"push|commit\b|add\b|reset\s+--hard|reset\s+--keep|rebase|merge|filter-branch|filter-repo|update-ref|symbolic-ref|reflog\s+expire|gc\b|branch\s+-D|branch\s+-d|tag\s+-d|update-index"
# =============================================================================

def deny(reason):
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": reason}}))
    raise SystemExit(0)

raw = sys.stdin.read()
if not raw.strip():
    raise SystemExit(0)  # nothing to inspect -> let normal flow proceed
try:
    payload = json.loads(raw)
except Exception:
    deny("Sandbox guard could not parse the tool payload; blocking as a precaution.")

root = os.path.realpath(os.environ["ROOT"])
tool = payload.get("tool_name") or ""
ti = payload.get("tool_input") or {}

# ---------------------------------------------------------------------------
# Bash branch: git guard (structure/destructive ops + hook-bypass), any branch
# ---------------------------------------------------------------------------
if tool == "Bash":
    cmd = ti.get("command") or ""

    # Strip quote obfuscation only (g"i"t, "gh") - keep slashes intact so
    # /-based path matching downstream (not used here, kept for parity/clarity).
    norm = re.sub(r"[\"\x27]", "", cmd)

    for word in ("git", "gh"):
        if (re.search(rf"\b{word}\b", norm) and not re.search(rf"\b{word}\b", cmd)):
            deny(f"Guard: obfuscated {word} invocation detected "
                 "(quoted/escaped spelling). Stop and report instead of working around the guard.")

    if re.search(r"\bgit\b", norm):
        if re.search(r"\bgit\b[^|;&\n]*\b(" + DESTRUCTIVE + r")", norm):
            deny("Git guard: blocked git operation (commit, add, push, reset --hard, "
                 "rebase, merge, history rewrites, branch/tag deletion). "
                 "Nicolas commits manually - summarize changes instead.")
        if re.search(r"--no-verify\b", norm) or re.search(r"core\.hooksPath", norm, re.IGNORECASE):
            deny("Git guard: --no-verify / core.hooksPath is never allowed. "
                 "Fix the underlying issue instead of bypassing hooks.")

    raise SystemExit(0)  # other Bash commands: the OS-level sandbox denyWrite
                          # handles path protection; nothing more to check here.

# ---------------------------------------------------------------------------
# Edit/Write/MultiEdit/NotebookEdit branch: deny-list, not allow-list
# ---------------------------------------------------------------------------
fp = ti.get("file_path") or ti.get("notebook_path")
if not fp:
    deny("Sandbox guard found no target path on the edit; blocking as a precaution.")

full = os.path.realpath(fp if os.path.isabs(fp) else os.path.join(root, fp))

if not (full == root or full.startswith(root + os.sep)):
    deny(f"Sandboxed: edits outside the project folder are not allowed. Blocked: {fp}")

rel = os.path.relpath(full, root).replace(os.sep, "/").lower()

for d in PROTECTED_DIRS:
    dl = d.lower()
    if rel == dl or rel.startswith(dl + "/"):
        deny(f"Sandboxed: \"{d}\" is protected from edits. Blocked: {fp}")

for ext in PROTECTED_EXT:
    if full.lower().endswith(ext):
        deny(f"Sandboxed: \"{ext}\" archives are protected from edits. Blocked: {fp}")
'
