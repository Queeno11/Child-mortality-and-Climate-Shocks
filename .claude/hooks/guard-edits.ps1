#Requires -Version 5.1
<#
  guard-edits.ps1 - Windows-native PreToolUse guard for Claude Code.

  Reads the PreToolUse JSON payload on stdin and dispatches on tool_name:

    Edit / Write / MultiEdit / NotebookEdit:
      Default-ALLOW inside the project root, with a deny-list. An edit is
      DENIED when its resolved target is:
        - outside the project root                (cannot touch anything outside)
        - inside  Data\  .git\  or  .claude\      (protected folders)
        - a *.zip archive                          (protected archives)

    Bash:
      - git guard : blocks destructive / structure-changing git ops
                    (push, reset --hard, rebase, merge, filter-branch,
                     update-ref, reflog expire, gc, branch/tag deletion)
                    and hook-bypass flags (--no-verify, core.hooksPath).
      - write guard (best-effort): blocks shell write/delete commands that
                    name a protected target (Data\, .git\, .claude\, *.zip).

  Wired in .claude\settings.json on the Edit|Write|MultiEdit|NotebookEdit|Bash
  matcher. Fails CLOSED (deny) on unparseable input, a missing target path, or
  an unresolvable project root - an unreadable input must not slip past.

  NOTE: On native Windows there is no OS-level sandbox, so the Bash write guard
  is best-effort string inspection, not airtight isolation. The Edit-family
  path allowlist IS reliable. See .claude\SANDBOX_SETUP.md.

  To change protections, edit $ProtectedDirs / $ProtectedExt below and the
  $Destructive git pattern.
#>

$ErrorActionPreference = 'Stop'

# ============================ CONFIG =================================
$ProtectedDirs = @('data', '.git', '.claude')   # compared case-insensitively
$ProtectedExt  = @('.zip')
$Destructive   = 'push|commit\b|add\b|reset\s+--hard|reset\s+--keep|rebase|merge|filter-branch|filter-repo|update-ref|symbolic-ref|reflog\s+expire|gc\b|branch\s+-D|branch\s+-d|tag\s+-d|update-index'
# ====================================================================

function Deny([string]$reason) {
    $obj = @{
        hookSpecificOutput = @{
            hookEventName            = 'PreToolUse'
            permissionDecision       = 'deny'
            permissionDecisionReason = $reason
        }
    }
    ($obj | ConvertTo-Json -Compress -Depth 5)
    exit 0
}

# --- Read payload from stdin ---
$raw = [Console]::In.ReadToEnd()
if ([string]::IsNullOrWhiteSpace($raw)) { exit 0 }   # nothing to inspect

try {
    $payload = $raw | ConvertFrom-Json
} catch {
    Deny 'Sandbox guard could not parse the tool payload; blocking as a precaution.'
}

# --- Resolve project root (env var first, else two levels up from this script) ---
$root = $env:CLAUDE_PROJECT_DIR
if ([string]::IsNullOrWhiteSpace($root)) {
    $root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
try {
    $root = [System.IO.Path]::GetFullPath($root)
} catch {
    Deny 'Sandbox guard could not resolve the project root; blocking as a precaution.'
}
$rootLower = $root.TrimEnd('\', '/').ToLowerInvariant()

$tool = [string]$payload.tool_name
$ti   = $payload.tool_input

# ============================ BASH BRANCH ===========================
if ($tool -eq 'Bash') {
    $cmd = [string]$ti.command
    if ($null -eq $cmd) { $cmd = '' }

    # Strip quote obfuscation (g"i"t, "gh") before pattern-matching. NOTE: we do
    # NOT strip backslashes here - on Windows '\' is the path separator and must
    # survive so the protected-path check below can see 'Data\', '.git\', etc.
    $norm = ($cmd -replace '["\x27]', '')

    # --- git guard ---
    if ($norm -match '\bgit\b') {
        if ($norm -match "\bgit\b[^|;&\n]*\b($Destructive)") {
            Deny 'Git guard: destructive / structure-changing git operation blocked to protect the repo (push, reset --hard, rebase, merge, filter-branch, update-ref, reflog expire, gc, branch/tag deletion). Run it yourself if you intend it.'
        }
        if ($norm -match '--no-verify' -or $norm -match 'core\.hooksPath') {
            Deny 'Git guard: hook-bypass flags (--no-verify / core.hooksPath) are not allowed. Fix the underlying issue instead of bypassing hooks.'
        }
    }

    # --- best-effort protected-path write guard ---
    $writeVerb = '\brm\b|\brmdir\b|\bdel\b|\berase\b|\bmove\b|\bmv\b|\bcp\b|\bcopy\b|\bren\b|\brename\b|Remove-Item|Move-Item|Copy-Item|Set-Content|Add-Content|Out-File|New-Item|Clear-Content|>>|>'
    if ($norm -match $writeVerb) {
        $protectedTarget = '(?i)(^|[\s"\x27=(\\/])Data[\\/]|(^|[\s"\x27=(\\/])\.git[\\/]|(^|[\s"\x27=(\\/])\.claude[\\/]|\.zip\b'
        if ($norm -match $protectedTarget) {
            Deny 'Sandbox guard: a shell write/delete command references a protected path (Data\, .git\, .claude\, or a .zip). Blocked. Run it yourself if you intend it.'
        }
    }

    exit 0   # Bash command is fine -> proceed
}

# =================== EDIT / WRITE / NOTEBOOK BRANCH =================
if (@('Edit', 'Write', 'MultiEdit', 'NotebookEdit') -contains $tool) {
    $fp = $ti.file_path
    if ([string]::IsNullOrWhiteSpace($fp)) { $fp = $ti.notebook_path }
    if ([string]::IsNullOrWhiteSpace($fp)) {
        Deny 'Sandbox guard found no target path on the edit; blocking as a precaution.'
    }

    try {
        if (-not [System.IO.Path]::IsPathRooted($fp)) {
            $fp = Join-Path $root $fp
        }
        $full = [System.IO.Path]::GetFullPath($fp)
    } catch {
        Deny 'Sandbox guard could not resolve the edit path; blocking as a precaution.'
    }
    $fullLower = $full.ToLowerInvariant()

    # 1) Must be inside the project root.
    if (-not ($fullLower -eq $rootLower -or
              $fullLower.StartsWith($rootLower + '\') -or
              $fullLower.StartsWith($rootLower + '/'))) {
        Deny "Sandboxed: edits outside the project folder are not allowed. Blocked: $fp"
    }

    # Relative portion under root, for folder checks.
    $rel = $fullLower.Substring($rootLower.Length).TrimStart('\', '/')

    # 2) Protected folders.
    foreach ($d in $ProtectedDirs) {
        $dl = $d.ToLowerInvariant()
        if ($rel -eq $dl -or $rel.StartsWith($dl + '\') -or $rel.StartsWith($dl + '/')) {
            Deny "Sandboxed: '$d' is protected from edits. Blocked: $fp"
        }
    }

    # 3) Protected archive extensions.
    foreach ($ext in $ProtectedExt) {
        if ($fullLower.EndsWith($ext.ToLowerInvariant())) {
            Deny "Sandboxed: '$ext' archives are protected from edits. Blocked: $fp"
        }
    }

    exit 0   # edit target is allowed -> proceed
}

# Any other tool: no opinion.
exit 0
