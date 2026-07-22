# CLAUDE.md — Child Mortality and Climate Shocks

Guidance for working in this repository. Read before touching files.

## Sandbox — protected paths (WSL2, OS-enforced + PreToolUse guard)

This project runs inside WSL2 with Claude Code's real sandbox enabled
(bubblewrap-enforced `denyWrite` for every Bash command) plus a `PreToolUse`
hook (`.claude/hooks/guard-edits.sh`, wired in `.claude/settings.json`) for the
policy the OS boundary can't express. You may edit anything in the project
folder EXCEPT:

- **`Data/`** — the research data. Nicolas runs every script that reads or writes
  it. Never edit files here; never `rm`/`move`/redirect output into it. Blocked
  at the OS level for Bash, not just by the hook.
- **`.git/config`, `.git/hooks/`, `.git/packed-refs`** — blocked at the OS level.
  Beyond that, do not run structure/history-changing git ops (`push`,
  `reset --hard`/`--keep`, `rebase`, `merge`, `filter-branch`/`filter-repo`,
  `update-ref`, `symbolic-ref`, `reflog expire`, `gc`, branch/tag deletion) or
  hook-bypass flags (`--no-verify`, `core.hooksPath`) on **any branch** — the
  hook blocks these by inspecting the command. Read-only git
  (`status`/`diff`/`log`/`show`/`checkout`) is fine; `add`/`commit` are
  forbidden by policy — see "Git & GitHub workflow" below.
- **`.claude/`** — the guard and settings. Never edit; that would disable your own
  sandbox. If a change is needed here, describe it and let Nicolas make it.
- **`*.zip`** — archive snapshots (`overleaf_source.zip`, `Outputs Jed Version *.zip`).
- **`~/.cdsapirc`** — the CDS/ERA5 API credential; read-blocked, same rationale
  as `Data/` (the ERA5-fetching scripts are Nicolas's to run).
- **Anything outside this project folder** — off-limits entirely.

A blocked edit/command is the guard working as intended. Do NOT route around it
(no `python -c` writes, heredocs, path obfuscation, `dangerouslyDisableSandbox`).
Stop and report instead. See `.claude/SANDBOX_SETUP.md` for the full design.

## What's editable

The analysis scripts and notebooks (`0*_*.py`, `*.ipynb`, `*.jl`, `plot_tools*.py`,
`run_all.py`, etc.), `Outputs/`, `Docs/`, `Overleaf/`, and new files you create
in the project root. Prefer running smaller subtasks to verify code you change
rather than long full pipelines.

## Development workflow & coding standards

Whenever asked to write, refactor, or modify code, follow this sequence unless
Nicolas explicitly says to skip it:

1. **Modular design:** write code in a highly modular way — focused,
   single-responsibility functions.
2. **Unit testing (synthetic data):** before running the main script, write and
   execute unit tests using synthetic or mock data for every function you just
   created or changed. Iterate until they pass.
3. **Execution (real data):** once synthetic tests pass, run against the real
   data — but remember scripts that read or write `Data/` are Nicolas's to run.
   If you cannot execute due to sandbox restrictions, network rules, or missing
   credentials, state the limitation clearly, output the final code, and stop.
   Do not loop trying to force a blocked step.

## Git & GitHub workflow

- **No GitHub operations.** Never `git push`, open PRs, or use `gh` — Nicolas
  handles everything on GitHub. (Push is also hook-blocked; see sandbox section.)
- **NEVER run `git commit`** (or `git add`) — Nicolas reviews diffs and commits
  manually. Edit files directly on the active branch; read-only git
  (`status`/`diff`/`log`/`show`) is fine. When done, **summarize the changed
  files** so Nicolas can `git diff` and commit.
- **No worktrees.** Work directly in the main checkout — no `EnterWorktree`, no
  agent `isolation: "worktree"`. Worktrees live under the protected `.claude/`
  and will be blocked. If a task seems to need isolation, stop and ask.

## Blocked actions & error reporting

- A blocked tool call or denied edit is the answer, not an obstacle: do not
  reproduce the write via Bash, heredocs, `python -c`, `tee`, `sed -i`, or any
  other means. Stop and report what was blocked and why you wanted it.
- If a hook or check reports a failure, fix the underlying issue in the file —
  never suppress or bypass the check.
- Report outcomes faithfully: if tests fail, say so and include the output; if
  a step was skipped or couldn't run, say that explicitly in the final summary.

## Note to models: writing new files is NOT a "needs isolation" situation

Creating or editing a normal file in the project root (e.g. `main.py`) is
**explicitly allowed**. Just write it directly on the active branch.

- There is **no background-job harness** that requires a worktree before file
  writes. If a write appears to fail, do **not** conclude you need isolation.
  Re-read the error; it is about something else.
- **Never** use a worktree / `EnterWorktree` / `isolation: "worktree"`. This
  file forbids it (see "Git & GitHub workflow" above).
- **Never** edit `.claude/settings.json` (or anything under `.claude/`) —
  including adding `"worktree": {"bgIsolation": "none"}`. That would disable the
  sandbox and is off-limits. If a `.claude/` change truly seems needed, describe
  it and stop.
- The safe path for "create `main.py` in the root" is: **write the file directly
  in the main checkout, then summarize the diff for Nicolas to commit.** Nothing
  else.

If you genuinely cannot write a permitted file, report the exact blocked call
and stop — do not route around it with a worktree, a settings edit, `python -c`,
heredocs, or by pasting the file into chat as a substitute.

## Project (fill in as it grows)

<!-- Add: what the paper argues, data sources (DHS, ERA5/CRU/CCKP climate indices),
     the pipeline order (00_query -> 01_indices -> 02_assign -> 03_merge ->
     04_regressions -> 06_charts), and any run/env conventions. -->
