# Claude Code sandbox (WSL2) — how it's built and how to run it

This project now runs Claude Code inside WSL2, which gets the **real, kernel-
enforced sandbox** (bubblewrap) — a step up from the earlier native-Windows
setup, which could only string-match commands. Read this before changing
anything under `.claude/`.

## Two layers, different jobs

1. **OS-level `sandbox` block in `settings.json`** — enforced by bubblewrap for
   every `Bash` command and its child processes. This is airtight: no shell
   trick, obfuscation, or subprocess can write to a `denyWrite` path, because
   the kernel — not a string match — refuses the write. This is what actually
   protects `Data/`, `.claude/`, the `.zip` archives, and the git config/hooks
   files from any Bash command.
2. **`guard-edits.sh` (`PreToolUse` hook)** — a second, semantic layer for the
   one thing the OS boundary can't express: *directory* allow/deny can't tell
   `git commit` from `git push` or `git reset --hard`, since both write inside
   `.git/`. This hook inspects the command text to allow ordinary scratch git
   (`add`, `commit`, `status`, `diff`, `log`, `checkout`) everywhere, while
   blocking destructive/history-altering ops and hook-bypass flags. It also
   deny-lists the same paths for the `Edit`/`Write`/`MultiEdit`/`NotebookEdit`
   tools, which the sandbox doesn't cover (see below).

Both layers read the **same** protected-path list; that's intentional
redundancy, not duplication to prune.

## Why both — a note on scope

Per Anthropic's docs, `sandbox.filesystem` restricts **only Bash subprocesses**.
The built-in `Edit`/`Write`/`MultiEdit`/`NotebookEdit` tools go through the
permission system directly, not the sandbox. So protecting those requires the
`permissions.deny` rules in `settings.json` *and* `guard-edits.sh` (the hook
also catches path-traversal tricks like `src/../Data/x.csv` that a plain glob
rule might miss).

**A concrete gotcha found by running this for real:** Claude Code only matches
`Edit(path)` rules in `permissions.deny` for the file-editing tools — `Edit(...)`
covers `Edit`/`Write`/`MultiEdit`/`NotebookEdit` collectively. A paired
`Write(path)` rule is silently ignored and prints a startup warning telling you
so. `settings.json` here only lists `Edit(...)` entries for this reason; if you
ever add a new protected path, add only the `Edit(...)` form.

## What's protected

| Path | Enforced by | Notes |
|------|-------------|-------|
| `Data/` | sandbox `denyWrite` + `permissions.deny` + hook | no Bash write, no Edit/Write — you run anything that touches it |
| `.claude/` | sandbox `denyWrite` + `permissions.deny` + hook | the guard can't disable itself |
| `.git/config`, `.git/hooks/`, `.git/packed-refs` | sandbox `denyWrite` | blocks rewriting remotes, planting hooks, or rewriting packed refs directly |
| `.git/` (destructive ops) | hook only | `push`, `reset --hard`/`--keep`, `rebase`, `merge`, `filter-branch`/`filter-repo`, `update-ref`, `symbolic-ref`, `reflog expire`, `gc`, branch/tag deletion — blocked on **any branch**. Plain `commit`/`add`/`status` is fine anywhere ("ok for scratching") |
| `--no-verify`, `core.hooksPath` | hook | hook-bypass flags blocked outright |
| `*.zip` (`overleaf_source.zip`, `Outputs Jed Version *.zip`) | sandbox `denyWrite` + `permissions.deny` + hook | archive snapshots |
| anything outside the project root | sandbox default + hook | Bash can't write there by default; hook blocks Edit/Write there too |
| `~/.cdsapirc` | sandbox `credentials.files: deny` | the CDS/ERA5 API key — read-blocked for the same reason as `Data/`: the data-fetching scripts (`00_query_ERA5_*.py`) are yours to run |

Everything else in the project is editable by default — this is a **deny-list**,
not an allow-list (unlike the NY prototype, which only allows `src/`+`paper/`).

## The files

```
<project>\
  .claude\
    settings.json           <- sandbox block + hook wiring + permissions.deny
    hooks\
      guard-edits.sh          <- WSL2/Linux hook (bash + embedded python3)
      guard-edits.ps1          <- native-Windows fallback hook (kept for reference;
                                  not wired while you're on WSL2 — see below)
    SANDBOX_SETUP.md           <- this file
  CLAUDE.md                    <- house rules Claude reads every session
```

`guard-edits.ps1` is left in place in case you ever go back to running Claude
Code natively on Windows (see the earlier native-Windows setup); it isn't
referenced by the current `settings.json`, which now points at `guard-edits.sh`.

## How it's wired (`settings.json`)

```json
"hooks": { "PreToolUse": [ { "matcher": "Edit|Write|MultiEdit|NotebookEdit|Bash",
  "hooks": [ { "type": "command",
    "command": "bash \"$CLAUDE_PROJECT_DIR/.claude/hooks/guard-edits.sh\"" } ] } ] }
```

```json
"sandbox": {
  "enabled": true,
  "failIfUnavailable": true,
  "allowUnsandboxedCommands": false,
  "filesystem": {
    "allowWrite": ["~/.cache", "~/.conda", "~/miniconda3", "~/miniforge3",
                   "~/mambaforge", "~/.julia", "~/.local/share/jupyter", "~/.jupyter"],
    "denyWrite": ["./Data", "./.claude", "./.git/config", "./.git/hooks",
                  "./.git/packed-refs", "./overleaf_source.zip",
                  "./Outputs Jed Version 11-8.zip", "./Outputs Jed Version 16-8.zip"]
  },
  "network": { "allowedDomains": [ /* CDS/Copernicus, PyPI, conda-forge, GitHub, Julia registry */ ] },
  "credentials": { "files": [ { "path": "~/.cdsapirc", "mode": "deny" } ] }
}
```

Notes on choices:
- `failIfUnavailable: true` — if bubblewrap/socat aren't installed, Claude Code
  refuses to start unsandboxed rather than silently falling back. See
  "Install the sandbox dependencies" below.
- `allowUnsandboxedCommands: false` — disables the `dangerouslyDisableSandbox`
  escape hatch entirely; a command that can't run sandboxed simply fails rather
  than being retried outside the boundary.
- `allowWrite` entries are for the conda/pip/Julia/Jupyter caches and env
  install directories — these live outside the project (in `$HOME`) and are
  **not** writable by default (default write = project dir + temp only), so
  package installs would otherwise fail with permission errors. Adjust the
  conda paths if your install prefix differs (`conda info --base` will tell you).
- `network.allowedDomains` covers CDS/Copernicus (ERA5 downloads), PyPI/
  conda-forge (package installs), GitHub (git fetch, Julia registry), and the
  Julia package server. **Domain lists are a best guess** — I could not verify
  them live (network was blocked in the environment I built this in). The
  first time a command needs an unlisted domain, Claude Code will prompt for
  approval (or fail, given `allowUnsandboxedCommands: false`) — that's expected;
  approve it once and add it here if it recurs.
- `~/.cdsapirc` is deliberately **not** in `allowWrite` — see the table above.

## Install the sandbox dependencies (one-time, in WSL2)

```bash
sudo apt-get update && sudo apt-get install -y bubblewrap socat
```

Then restart Claude Code and run `/sandbox` — it should show Mode/Overrides/
Config tabs with no "Dependencies" warning. If Ubuntu 24.04+ blocks bubblewrap's
user namespaces (`sysctl kernel.apparmor_restrict_unprivileged_userns` returns
`1`), see the AppArmor profile fix in Anthropic's sandboxing docs.

## Verify it works

```bash
cd "/mnt/c/Working Papers/Paper - Child Mortality and Climate Shocks"

# should ALLOW (no output)
echo '{"tool_name":"Edit","tool_input":{"file_path":"01_compute_climate_indices.py"}}' | bash .claude/hooks/guard-edits.sh

# should DENY — Data/
echo '{"tool_name":"Edit","tool_input":{"file_path":"Data/dhs/clean.dta"}}' | bash .claude/hooks/guard-edits.sh

# should DENY — destructive git, any branch
echo '{"tool_name":"Bash","tool_input":{"command":"git reset --hard HEAD~1"}}' | bash .claude/hooks/guard-edits.sh

# should ALLOW — scratch commit
echo '{"tool_name":"Bash","tool_input":{"command":"git commit -am wip"}}' | bash .claude/hooks/guard-edits.sh
```

Then inside Claude Code, run `/sandbox` (Config tab) to confirm the resolved
sandbox settings, and `/hooks` to confirm `guard-edits.sh` is registered.
As a live test, ask Claude to `cat Data/<any file>` (should work — reads are
allowed) and then to write a file into `Data/` (should be refused, both by the
sandbox and the hook).

## Extending it

- **Protect another folder/extension:** add it to `denyWrite` in `settings.json`
  (glob patterns aren't confirmed to work in sandbox paths, so list exact
  relative paths) **and** to `PROTECTED_DIRS`/`PROTECTED_EXT` at the top of
  `guard-edits.sh` (keeps the Edit/Write-tool layer in sync).
- **Change protected git ops:** edit the `DESTRUCTIVE` pattern in `guard-edits.sh`.
- **Never** loosen this from inside a Claude session — `.claude/` is deny-listed
  by both layers precisely so the agent can't disable its own guard. Edit these
  files yourself.

## Limitations (read once)

Per Anthropic's docs: the sandbox's network layer doesn't terminate/inspect
TLS by default, so a broad allowed domain (e.g. `github.com`) is a theoretical
exfiltration path via domain fronting — not a concern for a private research
repo, but worth knowing. The default *read* policy still allows reading most
of the filesystem (only listed `denyRead`/`credentials.files` block reads);
we haven't blocked reads of `Data/` itself (only writes), since you wanted
edits protected, not visibility.
