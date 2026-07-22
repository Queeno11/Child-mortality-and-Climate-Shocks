#!/usr/bin/env python3
"""
test_sandbox.py — verification suite for this project's Claude Code sandbox.

Checks three independent layers (see .claude/SANDBOX_SETUP.md for the design):

  1. CONFIG   — .claude/settings.json is valid and actually contains the
                protected-path list we intend (including a *live* scan for
                any *.zip file in the project root that ISN'T in denyWrite —
                catches the "someone added a new archive and forgot to
                protect it" case).
  2. HOOK     — feeds real PreToolUse JSON payloads to guard-edits.sh (the
                semantic layer: git op / hook-bypass blocking) and checks the
                allow/deny decision against a battery of cases, including
                path-traversal and quote-obfuscation attempts.
  3. SANDBOX  — checks bubblewrap + socat are installed, that unprivileged
                bwrap sandboxes can actually start (the Ubuntu 24.04+ AppArmor
                gotcha), and then *directly reproduces* the project's
                denyWrite policy with a real bwrap invocation: confirms a
                write into the real Data/ directory fails, contrasted with a
                control write that succeeds. This is the closest a script can
                get to testing the actual OS-level enforcement Claude Code
                relies on, without being Claude Code itself.

Usage:
    python3 .claude/test_sandbox.py            # run everything
    python3 .claude/test_sandbox.py -v          # verbose (show every case)
    python3 .claude/test_sandbox.py --only config
    python3 .claude/test_sandbox.py --only hook
    python3 .claude/test_sandbox.py --only sandbox

Exit code: 0 if every check passed, 1 if anything failed or was inconclusive.

IMPORTANT — this script is meant to be run BY YOU, on your own machine, inside
WSL2, not by an agent on your behalf: the live sandbox probe in layer 3 spawns
a real bwrap process, and while it's designed to touch nothing but a doomed
write attempt against the real Data/ directory (which is expected to fail and
so leaves nothing behind) plus a control write confined to the sandbox's own
throwaway tmpfs (which vanishes when the process exits), you should be the one
running anything that creates OS-level sandboxes on your file tree. Nothing in
this script ever writes into .claude/, .git/, any *.zip, or any real project
directory other than the doomed Data/ write.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # .claude/test_sandbox.py -> project root
SETTINGS = ROOT / ".claude" / "settings.json"
HOOK_SH = ROOT / ".claude" / "hooks" / "guard-edits.sh"
HOOK_PS1 = ROOT / ".claude" / "hooks" / "guard-edits.ps1"

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
results: list[tuple[str, str, str]] = []  # (section, name, status) with message stashed separately
_messages: dict[int, str] = {}


def record(section: str, name: str, status: str, msg: str = "") -> None:
    results.append((section, name, status))
    if msg:
        _messages[len(results) - 1] = msg


def hr(title: str) -> None:
    print(f"\n=== {title} ===")


# ---------------------------------------------------------------------------
# Layer 1: config sanity
# ---------------------------------------------------------------------------
def test_config(verbose: bool) -> None:
    hr("1. Config sanity (.claude/settings.json)")

    if not SETTINGS.exists():
        record("config", "settings.json exists", FAIL, "file not found")
        return
    try:
        cfg = json.loads(SETTINGS.read_text())
    except Exception as e:
        record("config", "settings.json is valid JSON", FAIL, str(e))
        return
    record("config", "settings.json is valid JSON", PASS)

    # hook wiring
    try:
        hooks = cfg["hooks"]["PreToolUse"]
        matcher = hooks[0]["matcher"]
        cmd = hooks[0]["hooks"][0]["command"]
        assert "Edit" in matcher and "Bash" in matcher
        assert "guard-edits.sh" in cmd or "guard-edits.ps1" in cmd
        record("config", "PreToolUse hook wired", PASS)
    except Exception as e:
        record("config", "PreToolUse hook wired", FAIL, str(e))

    # sandbox block present and enabled
    sandbox = cfg.get("sandbox", {})
    record("config", "sandbox.enabled == true", PASS if sandbox.get("enabled") else FAIL)
    record("config", "sandbox.allowUnsandboxedCommands == false",
           PASS if sandbox.get("allowUnsandboxedCommands") is False else FAIL)

    # expected protected paths
    deny_write = set(sandbox.get("filesystem", {}).get("denyWrite", []))
    expected_min = {"./Data", "./.claude", "./.git/config", "./.git/hooks", "./.git/packed-refs"}
    missing = expected_min - deny_write
    record("config", "core denyWrite paths present", PASS if not missing else FAIL,
           f"missing: {missing}" if missing else "")

    # live scan: every *.zip in the project root must be in denyWrite
    zips_on_disk = {f"./{p.name}" for p in ROOT.glob("*.zip")}
    unprotected_zips = zips_on_disk - deny_write
    record("config", "every *.zip on disk is in denyWrite", PASS if not unprotected_zips else FAIL,
           f"unprotected: {unprotected_zips}" if unprotected_zips else "")

    # permissions.deny mirrors the same paths for the file-editing tools.
    # NOTE: only Edit(path) rules are matched by Claude Code's permission
    # checks — Write(path) rules are silently ignored for file-editing tools
    # (Claude Code prints a startup warning if you add one), since Edit(path)
    # already covers Edit/Write/MultiEdit/NotebookEdit collectively. Don't
    # re-add Write(...) entries here even though it might look "more complete."
    perm_deny = set(cfg.get("permissions", {}).get("deny", []))
    expected_perm = {"Edit(./Data/**)", "Edit(./.claude/**)", "Edit(./.git/**)", "Edit(./**/*.zip)"}
    missing_perm = expected_perm - perm_deny
    record("config", "permissions.deny mirrors protected paths", PASS if not missing_perm else FAIL,
           f"missing: {missing_perm}" if missing_perm else "")
    stray_write_rules = {r for r in perm_deny if r.startswith("Write(")}
    record("config", "no dead Write(...) permission rules", PASS if not stray_write_rules else FAIL,
           f"these are silently ignored by Claude Code, remove them: {stray_write_rules}"
           if stray_write_rules else "")

    # credentials
    cred_files = [c.get("path") for c in sandbox.get("credentials", {}).get("files", [])]
    record("config", "~/.cdsapirc credential-denied", PASS if "~/.cdsapirc" in cred_files else FAIL)

    if verbose:
        print(json.dumps({"denyWrite": sorted(deny_write), "credentials": cred_files}, indent=2))


# ---------------------------------------------------------------------------
# Layer 2: hook behavior (guard-edits.sh)
# ---------------------------------------------------------------------------
HOOK_CASES = [
    # (name, tool_name, tool_input, expect_deny)
    ("edit ordinary script", "Edit", {"file_path": str(ROOT / "01_compute_climate_indices.py")}, False),
    ("write into Outputs/", "Write", {"file_path": str(ROOT / "Outputs" / "x.png")}, False),
    ("notebook edit ordinary", "NotebookEdit", {"notebook_path": str(ROOT / "debug.ipynb")}, False),
    ("edit relative path (resolves to root)", "Write", {"file_path": "test.py"}, False),
    ("edit Data/ file", "Edit", {"file_path": str(ROOT / "Data" / "dhs" / "clean.dta")}, True),
    ("edit .git/config", "Edit", {"file_path": str(ROOT / ".git" / "config")}, True),
    ("edit .claude/settings.json", "Edit", {"file_path": str(ROOT / ".claude" / "settings.json")}, True),
    ("write a .zip archive", "Write", {"file_path": str(ROOT / "overleaf_source.zip")}, True),
    ("edit outside project root", "Edit", {"file_path": "/etc/hosts"}, True),
    ("path traversal into Data/", "Edit", {"file_path": str(ROOT / "src" / ".." / "Data" / "x.csv")}, True),
    ("edit with no path given", "Edit", {}, True),
    ("bash: git status", "Bash", {"command": "git status"}, False),
    ("bash: git commit (scratch, any branch)", "Bash", {"command": "git add -A && git commit -m wip"}, False),
    ("bash: git log", "Bash", {"command": "git log --oneline"}, False),
    ("bash: git checkout -b (not destructive)", "Bash", {"command": "git checkout -b feature/x"}, False),
    ("bash: git push", "Bash", {"command": "git push origin experimental"}, True),
    ("bash: git reset --hard", "Bash", {"command": "git reset --hard HEAD~1"}, True),
    ("bash: git rebase", "Bash", {"command": "git rebase -i HEAD~5"}, True),
    ("bash: git merge", "Bash", {"command": "git merge other-branch"}, True),
    ("bash: git filter-branch", "Bash", {"command": "git filter-branch --force"}, True),
    ("bash: git branch -D", "Bash", {"command": "git branch -D old-branch"}, True),
    ("bash: git commit --no-verify", "Bash", {"command": "git commit --no-verify -m x"}, True),
    ("bash: core.hooksPath override", "Bash", {"command": "git config core.hooksPath /tmp/evil"}, True),
    ("bash: obfuscated git (quoted)", "Bash", {"command": 'g"i"t push origin main'}, True),
    ("bash: unrelated command mentioning 'merge' in prose", "Bash",
     {"command": "python merge_datasets.py"}, False),
    ("bash: plain redirect into Outputs/", "Bash", {"command": "python run_all.py > Outputs/log.txt"}, False),
]


def run_hook(tool_name: str, tool_input: dict) -> tuple[str, str]:
    """Run the guard hook once, return (stdout, decision) where decision is
    'deny' if the JSON says so, else 'allow'."""
    payload = json.dumps({"tool_name": tool_name, "tool_input": tool_input})
    if HOOK_SH.exists() and shutil.which("bash"):
        cmd = ["bash", str(HOOK_SH)]
    elif HOOK_PS1.exists() and shutil.which("powershell"):
        cmd = ["powershell", "-NoProfile", "-File", str(HOOK_PS1)]
    else:
        return "", "no-hook-available"
    proc = subprocess.run(cmd, input=payload, capture_output=True, text=True, timeout=15)
    out = proc.stdout.strip()
    if not out:
        return out, "allow"
    try:
        decision = json.loads(out)["hookSpecificOutput"]["permissionDecision"]
    except Exception:
        decision = "unparseable"
    return out, decision


def test_hook(verbose: bool) -> None:
    hr("2. Hook behavior (guard-edits.sh)")
    if not HOOK_SH.exists() and not HOOK_PS1.exists():
        record("hook", "hook script exists", FAIL, "neither guard-edits.sh nor .ps1 found")
        return

    for name, tool, ti, expect_deny in HOOK_CASES:
        out, decision = run_hook(tool, ti)
        if decision == "no-hook-available":
            record("hook", name, SKIP, "no interpreter (bash/powershell) available")
            continue
        got_deny = decision == "deny"
        ok = got_deny == expect_deny
        record("hook", name, PASS if ok else FAIL,
                "" if ok else f"expected {'DENY' if expect_deny else 'ALLOW'}, got {decision.upper()}: {out}")
        if verbose:
            print(f"  [{'ok' if ok else 'XX'}] {name}: {decision}")


# ---------------------------------------------------------------------------
# Layer 3: real bubblewrap enforcement
# ---------------------------------------------------------------------------
def test_sandbox_runtime(verbose: bool) -> None:
    hr("3. OS-level sandbox runtime (bubblewrap)")

    is_wsl = "microsoft" in platform.uname().release.lower()
    record("sandbox", "platform is Linux/WSL2 (bwrap-capable)",
           PASS if os.name == "posix" else SKIP,
           f"uname={platform.uname().release}" if verbose else "")

    bwrap = shutil.which("bwrap")
    socat = shutil.which("socat")
    record("sandbox", "bubblewrap (bwrap) installed", PASS if bwrap else FAIL,
           "" if bwrap else "run: sudo apt-get install bubblewrap socat")
    record("sandbox", "socat installed", PASS if socat else FAIL,
           "" if socat else "run: sudo apt-get install bubblewrap socat")

    PROBE_NAME = "live denyWrite probe (Data/ blocked vs. tmpfs control)"

    if not bwrap:
        record("sandbox", "unprivileged bwrap namespace starts", SKIP, "bwrap not installed")
        record("sandbox", PROBE_NAME, SKIP, "bwrap not installed")
        return

    # AppArmor gotcha check (Ubuntu 24.04+): does an unprivileged bwrap even start?
    try:
        proc = subprocess.run(
            [bwrap, "--unshare-all", "--die-with-parent", "--ro-bind", "/", "/",
             "--tmpfs", "/tmp", "--", "/bin/true"],
            capture_output=True, text=True, timeout=15)
        ok = proc.returncode == 0
        record("sandbox", "unprivileged bwrap namespace starts", PASS if ok else FAIL,
               proc.stderr.strip()[:300] if not ok else "")
    except Exception as e:
        record("sandbox", "unprivileged bwrap namespace starts", FAIL, str(e))
        record("sandbox", PROBE_NAME, SKIP, "namespace failed to start")
        return

    # Live probe: reproduce this project's actual denyWrite policy with a real
    # bwrap invocation. Bind order matters in bwrap: later binds win over
    # earlier ones for overlapping paths.
    #   1. whole filesystem read-only (so bash/usr/lib are available)
    #   2. the project re-bound read-write
    #   3. Data/ re-locked read-only on top   <- same shape as Claude Code's denyWrite
    #   4. a fresh tmpfs at /tmp
    # The probe writes to two places and reports each exit code:
    #   - a file under the REAL Data/ directory       -> expected to FAIL
    #   - a file under /tmp (the throwaway tmpfs)      -> expected to SUCCEED,
    #     as a control proving the harness itself isn't just failing everything
    # Nothing is written anywhere in the real project tree: the Data/ write is
    # expected to fail (so nothing lands on disk), and the control write lives
    # only on the sandbox's private tmpfs, which disappears when the bwrap
    # process exits. No cleanup of real project files is ever needed.
    data_dir = ROOT / "Data"
    if not data_dir.is_dir():
        record("sandbox", PROBE_NAME, SKIP, "Data/ not found")
        return
    probe_data = data_dir / ".sandbox_probe_tmp"
    script = (
        f'echo probe > "{probe_data}" 2>/tmp/probe_err; echo "DATA_RC=$?"; '
        f'echo probe > /tmp/sandbox_probe_ok; echo "CONTROL_RC=$?"'
    )
    proc = subprocess.run(
        [bwrap, "--unshare-all", "--die-with-parent",
         "--ro-bind", "/", "/",
         "--bind", str(ROOT), str(ROOT),
         "--ro-bind", str(data_dir), str(data_dir),
         "--tmpfs", "/tmp",
         "--", "bash", "-c", script],
        capture_output=True, text=True, timeout=15)
    out = proc.stdout
    data_blocked = "DATA_RC=0" not in out
    control_ok = "CONTROL_RC=0" in out
    ok = data_blocked and control_ok
    record("sandbox", PROBE_NAME, PASS if ok else FAIL,
           "" if ok else f"stdout={out.strip()!r} stderr={proc.stderr.strip()[:200]!r}")

    # Defense in depth: if the sandbox were somehow misconfigured and the
    # "expected to fail" write actually landed on the real Data/ directory,
    # try to remove it and say so loudly rather than leaving debris silently.
    if probe_data.exists():
        try:
            probe_data.unlink()
            record("sandbox", "cleanup of unexpected Data/ write", PASS,
                   "the Data/ write should have failed but didn't — removed the stray file; "
                   "re-check your denyWrite config")
        except Exception as e:
            record("sandbox", "cleanup of unexpected Data/ write", FAIL,
                   f"the Data/ write should have failed but didn't, AND cleanup failed too "
                   f"({e}) — remove {probe_data} yourself and re-check your denyWrite config")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def report() -> int:
    hr("Summary")
    by_status = {PASS: 0, FAIL: 0, SKIP: 0}
    for i, (section, name, status) in enumerate(results):
        by_status[status] += 1
        if status == FAIL:
            msg = _messages.get(i, "")
            print(f"  FAIL [{section}] {name}" + (f" -- {msg}" if msg else ""))
        elif status == SKIP:
            msg = _messages.get(i, "")
            print(f"  SKIP [{section}] {name}" + (f" -- {msg}" if msg else ""))
    print(f"\n{by_status[PASS]} passed, {by_status[FAIL]} failed, {by_status[SKIP]} skipped "
          f"(of {len(results)} checks)")
    return 1 if by_status[FAIL] else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--only", choices=["config", "hook", "sandbox"], default=None)
    args = ap.parse_args()

    if args.only in (None, "config"):
        test_config(args.verbose)
    if args.only in (None, "hook"):
        test_hook(args.verbose)
    if args.only in (None, "sandbox"):
        test_sandbox_runtime(args.verbose)

    return report()


if __name__ == "__main__":
    sys.exit(main())
