<#
build_guard.ps1 - PreToolUse hook for the build agent and for Claude Code sessions at the repo root.

It lives in the build agent's home (build/.claude/) and runs from two settings files:
  build/.claude/settings.json  - sessions opened on the build/ folder (the build agent)
  .claude/settings.json        - sessions opened on the repo root (general development)
It finds the build folder and the repo from its own location, not from the folder Claude Code
opened, so the same rules apply in both.

Claude Code runs this before every shell command (Bash, PowerShell) and every file edit (Edit,
Write, NotebookEdit) that Claude makes. The deny/ask rules in settings.json only
match the START of a command; this script reads the whole command, so it also catches a
dangerous word in the middle (a DROP TABLE inside "bq query", a delete wrapped in "cmd /c").

It answers with one of:
  deny  - the call is refused and Claude is told why. Used for things the build must never do.
  ask   - you are shown the call and decide. Used for things that are sometimes fine.
  (none)- normal permission rules apply.
It never answers "allow", so it can only make Claude Code stricter, never looser.

Every call it sees is written to <data_root>\_build_logs\agent_audit_<yyyyMMdd>.log (data_root
from build/monthly_build.local.json) with the decision: a record of what the agent actually ran.

If this script fails, it answers "ask", so a broken guard stops for you instead of letting
calls through.

Test by piping a hook payload in, from the build folder, for example:
  '{"tool_name":"PowerShell","tool_input":{"command":"bq rm -f x.y"},"cwd":"."}' |
      pwsh -NoProfile -File .claude/hooks/build_guard.ps1
#>

$ErrorActionPreference = 'Stop'
$decision = $null          # 'deny' or 'ask'
$reasons = [System.Collections.Generic.List[string]]::new()
$tool = ''
$subject = ''
$sessionId = ''
$auditDir = $null

function Flag([string]$Level, [string]$Why) {
    # deny beats ask
    if ($Level -eq 'deny' -or -not $script:decision) { $script:decision = $Level }
    $script:reasons.Add("[$Level] $Why")
}

function Norm([string]$s) {
    # lower case, forward slashes, Git Bash /d/... -> d:/...
    if (-not $s) { return '' }
    $t = $s.ToLowerInvariant() -replace '\\\\', '/' -replace '\\', '/'
    $t = [regex]::Replace($t, '(^|[\s"''=(,;])/([a-z])/', '$1$2:/')
    return ($t -replace '(?<!:)/{2,}', '/')
}

try {
    $raw = [Console]::In.ReadToEnd()
    $in = $raw | ConvertFrom-Json
    $tool = [string]$in.tool_name
    $sessionId = [string]$in.session_id
    $cwd = if ($in.cwd) { [string]$in.cwd } else { (Get-Location).Path }

    # Folders from this script's own location (<repo>/build/.claude/hooks), whichever folder
    # Claude Code opened: the build folder (the build agent's home) or the repo root.
    $buildDir = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $repoDir = Split-Path $buildDir -Parent
    $buildN = (Norm $buildDir).TrimEnd('/')
    $repoN = (Norm $repoDir).TrimEnd('/')
    $cwdN = (Norm $cwd).TrimEnd('/')

    # How the wrapper and the build's Python scripts may be named: by full path always; as
    # build/<name> from the repo root; as ./<name> or <name> from the build folder.
    $prefixes = @([regex]::Escape($buildN + '/'))
    if ($cwdN -eq $repoN) { $prefixes += 'build/', '\./build/' }
    if ($cwdN -eq $buildN) { $prefixes += '\./', '' }
    $pre = '(?:' + ($prefixes -join '|') + ')'
    $wrapRx = $pre + 'run_build\.ps1\b'
    $pyRx = $pre + '(fetch_delivery|verify_snapshot|prepare_snapshot)\.py\b'

    # ---- machine paths (git-ignored config) ----
    $dataN = ''; $drives = @(); $currentN = ''
    $cfgPath = Join-Path $buildDir 'monthly_build.local.json'
    if (Test-Path $cfgPath) {
        $cfg = Get-Content -Raw $cfgPath | ConvertFrom-Json
        if ($cfg.data_root) {
            $dataN = (Norm $cfg.data_root).TrimEnd('/')
            $auditDir = if ($env:BUILD_GUARD_AUDIT_DIR) { $env:BUILD_GUARD_AUDIT_DIR } else { Join-Path $cfg.data_root '_build_logs' }
            # the delivery being built = the newest delivery_<date>.json under data_root
            $newest = Get-ChildItem -Path $cfg.data_root -Filter 'delivery_*.json' -Recurse -Depth 2 -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1
            if ($newest) { $currentN = (Norm $newest.DirectoryName).TrimEnd('/') }
        }
        foreach ($d in @($cfg.mlrs_drop, $cfg.nlsdb_drop)) {
            if ($d -and $d -match '^[A-Za-z]:') { $drives += (Norm $d.Substring(0, 2)) + '/' }
        }
        $drives = @($drives | Select-Object -Unique)
    }

    function Under([string]$pathN, [string]$rootN) {
        if (-not $rootN) { return $false }
        return ($pathN -eq $rootN -or $pathN.StartsWith($rootN + '/'))
    }

    if ($tool -in @('Bash', 'PowerShell')) {
        $cmd = [string]$in.tool_input.command
        $subject = $cmd
        $n = Norm $cmd
        # A tool named with a path or an extension (git.exe, gsutil.cmd, c:/x/bq.cmd,
        # & 'c:/program files/git/cmd/git.exe') is reduced to its bare name, so every check
        # below sees it the same as plain "git", "gsutil" or "bq".
        $tools = 'git|gsutil|bq|gcloud|pwsh|powershell|python3?|py|cmd|conda|pip3?|mamba|winget|choco|scoop|npm|reg|setx|schtasks|netsh'
        $ext = '(?:\.exe|\.cmd|\.bat|\.com)'
        $n = [regex]::Replace($n, '([''"])[^''"]*/(' + $tools + ')' + $ext + '?\1', '$2')
        $n = [regex]::Replace($n, '(^|[\s;&|(])[^\s''";&|()]*/(' + $tools + ')' + $ext + '?(?=[\s;&|)]|$)', '$1$2')
        $n = [regex]::Replace($n, '(^|[\s;&|(])(' + $tools + ')' + $ext + '(?=[\s;&|)]|$)', '$1$2')
        # drop output-to-null redirects so "2>$null" is not read as a file write
        $ns = $n -replace '\d?>\s*&\s*\d', ' ' -replace '\d?>\s*(\$null|/dev/null|nul)\b', ' '

        $writeRx = '\b(remove-item|move-item|rename-item|copy-item|set-content|add-content|out-file|new-item|clear-content|clear-item|tee-object|expand-archive|compress-archive|robocopy|xcopy)\b|(^|[\s;&|(])(rm|rmdir|rd|del|erase|mv|move|ren|rename|cp|copy|touch|mkdir|md|tee|unzip|tar)\s|shutil\.|os\.(remove|unlink|rename|replace|rmdir|makedirs|mkdir)|\.(unlink|rmdir|write_text|write_bytes)\(|\bopen\([^)]*[''"][wax]b?\+?[''"]|>'
        $delRx = '\b(remove-item|move-item|rename-item|clear-content|clear-item)\b|(^|[\s;&|(])(rm|rmdir|rd|del|erase|mv|move|ren|rename)\s|robocopy[^\n]*\s/(mir|purge|mov|move)\b|shutil\.(rmtree|move)|os\.(remove|unlink|rename|replace|rmdir)|\.(unlink|rmdir)\('
        $writes = $ns -match $writeRx
        $deletes = $ns -match $delRx

        # ---------- 4/9: BigQuery ----------
        foreach ($m in [regex]::Matches($ns, '\bbq\b((?:\s+--?[^\s]+)*)\s+([a-z_-]+)([^|;&\n]*)')) {
            $sub = $m.Groups[2].Value; $rest = $m.Groups[1].Value + ' ' + $m.Groups[3].Value
            switch -regex ($sub) {
                '^(rm|truncate|undelete)$' { Flag 'deny' "bq $sub deletes or replaces BigQuery data" }
                '^cp$' {
                    if ($rest -match '(\s-f\b|--force|\s-a\b|--append_table|--no_clobber=false)') { Flag 'deny' 'bq cp that overwrites or appends to a table' }
                    else { Flag 'ask' 'bq cp creates a table' }
                }
                '^load$' {
                    if ($rest -match '--replace') { Flag 'deny' 'bq load --replace run directly (loads go through run_build.ps1 -Step load)' }
                    else { Flag 'ask' 'bq load run directly (loads go through run_build.ps1 -Step load)' }
                }
                '^(mk|update|insert|extract|partition|set-iam-policy|add-iam-policy-binding|remove-iam-policy-binding)$' { Flag 'ask' "bq $sub changes BigQuery objects or exports data" }
                '^query$' {
                    $sql = $ns   # the whole command, so a second statement after ';' is checked too
                    # SQL read from a file: "< file.sql", "$(Get-Content file)", "$(cat file)"
                    foreach ($f in [regex]::Matches($cmd, '(?:<\s*|get-content\s+(?:-raw\s+)?|\bcat\s+|\bgc\s+)["'']?([^"''\s|;&)]+)', 'IgnoreCase')) {
                        $p = $f.Groups[1].Value
                        if (-not [System.IO.Path]::IsPathRooted($p)) { $p = Join-Path $cwd $p }
                        if (Test-Path $p -PathType Leaf) { $sql += "`n" + (Get-Content -Raw $p).ToLowerInvariant() }
                        else { Flag 'ask' "bq query reads SQL from '$($f.Groups[1].Value)', which the guard could not open to check" }
                    }
                    $dml = '\b(drop\s+(table|view|schema|function|procedure|materialized|snapshot|external|row\s+access)|delete\s+from|delete\s+[a-z_`]|truncate\s+table|insert\s+into|insert\s+[a-z_`]|update\s+[^\s]+\s+(as\s+[a-z_]+\s+)?set\b|merge\s+(into\s+)?[^\s]+|alter\s+(table|view|schema|materialized)|create\s+(or\s+replace\s+)?(temp\s+|temporary\s+)?(table|view|schema|function|procedure|materialized|snapshot|external)|grant\s|revoke\s|export\s+data|load\s+data|call\s)'
                    if ($sql -match $dml) { Flag 'deny' "bq query with a statement that changes data or tables ('$($Matches[0].Trim())'); the agent only reads" }
                    if ($rest -match '(--destination_table|--replace|--append_table|--destination_schema)') { Flag 'deny' 'bq query that writes its results to a table' }
                    if ($rest -notmatch '--maximum_bytes_billed[=\s]+\d+' -and $rest -notmatch '--dry_run') {
                        Flag 'deny' 'bq query without a cost cap: add --maximum_bytes_billed=10000000000 (10 GB) and select only the columns you need'
                    }
                }
            }
        }

        # ---------- 5: Cloud Storage ----------
        foreach ($m in [regex]::Matches($ns, '\bgsutil\b((?:\s+-(?:[oh]\s+[^\s]+|[a-z]+))*)\s+([a-z]+)([^|;&\n]*)')) {
            $sub = $m.Groups[2].Value; $rest = $m.Groups[3].Value
            switch -regex ($sub) {
                '^(rm|mv|rb)$' { Flag 'deny' "gsutil $sub deletes or moves Cloud Storage objects (GCS cleanup is never automatic)" }
                '^rsync$' {
                    if ($rest -match '\s-[a-z]*d') { Flag 'deny' 'gsutil rsync -d deletes objects' } else { Flag 'ask' 'gsutil rsync writes to Cloud Storage or local disk' }
                }
                '^(cp|setmeta|acl|iam|defacl|lifecycle|retention|versioning|mb|compose|label|cors|web|notification|kms)$' { Flag 'ask' "gsutil $sub writes or changes settings" }
            }
        }
        foreach ($m in [regex]::Matches($ns, '\bgcloud\b(?:\s+--?[^\s]+)*\s+([^|;&\n]*)')) {
            $rest = $m.Groups[1].Value.Trim()
            if ($rest -match '^storage\s+(rm|mv)\b|^storage\s+buckets\s+(delete|update|create)|^storage\s+rsync\b.*--delete-unmatched') { Flag 'deny' 'gcloud storage delete or move' }
            # ---------- 12: sign-in, account and project ----------
            elseif ($rest -match '^auth\s+(revoke|activate-service-account|application-default|print-access-token|print-identity-token|configure-docker)|^config\s+(set|unset|configurations\s+(activate|create|delete|rename))|^iam\b|^projects\s+(add-iam|remove-iam|set-iam|delete|create|undelete)|^services\s+(enable|disable)' -or $m.Value -match '--impersonate-service-account|--account[=\s]|--project[=\s]') {
                Flag 'deny' 'gcloud command that changes the account, project or permissions, prints a token, or runs as another account'
            }
            elseif ($rest -match '^auth\s+login') { Flag 'ask' 'gcloud auth login opens a browser for you to sign in' }
            elseif ($rest -match '^storage\s+(cp|rsync)\b') { Flag 'ask' 'gcloud storage writes to Cloud Storage or local disk' }
            elseif ($rest -notmatch '^(auth\s+list|config\s+(list|get)|info|version|storage\s+(ls|du|cat)\b|storage\s+(objects|buckets)\s+(describe|list))') { Flag 'ask' "gcloud $($rest.Split(' ')[0..1] -join ' ') is not on the guard's read-only list" }
        }

        # ---------- 2: git ----------
        foreach ($m in [regex]::Matches($ns, '\bgit\b((?:\s+-c\s+(?:"[^"]*"|''[^'']*''|[^\s]+)|\s+--?[a-z-]+(?:=[^\s]+)?)*)\s+([a-z-]+)([^|;&\n]*)')) {
            $sub = $m.Groups[2].Value; $rest = ' ' + $m.Groups[3].Value + ' '
            switch -regex ($sub) {
                '^(push|commit|reset|clean|restore|filter-branch|filter-repo|gc|prune|update-ref|reflog|replace)$' { Flag 'deny' "git $sub (commits, pushes and discarding work are yours)" }
                '^branch$' {
                    if ($rest -match '\s(-d|--delete|-m|--move|-c|--copy|-f|--force|--set-upstream-to|-u|--unset-upstream|--edit-description)\s') { Flag 'deny' 'git branch delete, rename or force' }
                    elseif ($rest -notmatch '^\s*((--show-current|-a|-r|--all|--remotes|--list|-v|-vv|--contains\s+\S+|--merged|--no-merged)\s*)*$') { Flag 'ask' 'git branch creates a branch' }
                }
                '^checkout$' {
                    if ($rest -match '\s(--|\.|-f|--force|-b|-B|--orphan|-p|--patch)\s') { Flag 'deny' 'git checkout that discards changes or creates a branch' } else { Flag 'ask' 'git checkout switches branches' }
                }
                '^stash$' { if ($rest -match '\s(drop|clear)\s') { Flag 'deny' 'git stash drop/clear deletes saved work' } else { Flag 'ask' 'git stash moves your uncommitted changes' } }
                '^tag$' { if ($rest -match '\s(-d|--delete|-f|--force)\s') { Flag 'deny' 'git tag delete or force' } elseif ($rest.Trim() -and $rest -notmatch '^\s*(-l|--list)') { Flag 'ask' 'git tag creates a tag' } }
                '^(switch|merge|rebase|cherry-pick|revert|am|apply|add|rm|mv|pull|worktree|submodule|config|remote|init|clone|notes|bisect|sparse-checkout)$' { Flag 'ask' "git $sub changes the repo or its settings" }
            }
        }

        # ---------- 1: rebuilding a loaded snapshot ----------
        if ($ns -match '-allowrebuild|--allow-rebuild') { Flag 'ask' 'rebuild of a snapshot that is already in BigQuery' }

        # ---------- 6: the shared drive ----------
        foreach ($d in $drives) {
            if ($ns.Contains($d) -and $writes) { Flag 'deny' "writes, moves or deletes on the shared drive ($($d.ToUpper())) - the agent only reads the zips there" }
        }

        # ---------- 3: the agent's own rules ----------
        if ($ns -match '\.claude/(settings[^\s"'']*\.json|hooks)' -and $writes) { Flag 'deny' 'changes Claude Code settings or hooks' }
        elseif ($ns -match '\.claude/' -and $writes) { Flag 'ask' 'changes a file under .claude/ (the build runbook)' }

        # ---------- 11: data folders ----------
        if ($dataN -and $ns.Contains($dataN)) {
            if ($deletes) { Flag 'deny' 'deletes, moves or renames files in the data folder' }
            elseif ($writes -and -not ($currentN -and $ns.Contains($currentN))) { Flag 'ask' 'writes in a data folder other than the current delivery''s month folder' }
        }
        if ($deletes -and -not $decision) { Flag 'ask' 'deletes, moves or renames files' }

        # scripts and schemas edited from the shell (Edit/Write to them already ask). In the build
        # folder a relative path names no folder, so any write there that is not in the data
        # folder asks too.
        $isWrapper = $ns -match ('^\s*pwsh\s+-noprofile\s+-executionpolicy\s+bypass\s+-file\s+' + $wrapRx + '[^|;&>]*$')
        $inBuild = Under $cwdN $buildN
        if ($writes -and -not $isWrapper -and
            ($ns -match '(^|[\s"''/])(build|schemas)/' -or ($inBuild -and -not ($dataN -and $ns.Contains($dataN))))) {
            Flag 'ask' 'may write under build/ or schemas/'
        }

        # ---------- code the guard cannot read ----------
        if ($ns -match '(^|[\s;&|(])(python3?|python\.exe|py|conda\s+run)\b' -and $ns -notmatch ('(^|[\s;&|(])python3?(\.exe)?\s+(-u\s+)?' + $pyRx)) {
            Flag 'ask' 'runs Python code the guard cannot check'
        }
        if ($ns -match '\b(pwsh|powershell)(\.exe)?\s+[^|;&]*-(c|command|e|ec|encodedcommand)\b' -or
            ($ns -match '\b(pwsh|powershell)(\.exe)?\s+[^|;&]*-file\s+' -and $ns -notmatch ('-file\s+' + $wrapRx)) -or
            $ns -match '\bcmd(\.exe)?\s+/[ck]\b|\b(invoke-expression|iex|start-process|saps|invoke-command|icm|start-job)\b|\b(bash|sh|wsl)(\.exe)?\s+-c\b') {
            Flag 'ask' 'runs a nested shell or script the guard cannot check'
        }

        # ---------- installs (always confirmed) and system settings ----------
        if ($ns -match '\b(pip3?|conda|mamba|winget|choco|scoop|npm|pnpm|yarn)\s+(install|i|add|update|upgrade|remove|uninstall)\b|\bgcloud\s+components\s+(install|update|remove)') {
            Flag 'ask' 'installs, updates or removes software'
        }
        if ($ns -match '\bsetx\b|setenvironmentvariable|set-executionpolicy|\breg(\.exe)?\s+(add|delete|import)|new-itemproperty|set-itemproperty|remove-itemproperty|\bschtasks\b|register-scheduledtask|\bnetsh\b') {
            Flag 'deny' 'changes system settings, environment variables or scheduled tasks'
        }
    }
    elseif ($tool -in @('Edit', 'Write', 'NotebookEdit', 'MultiEdit')) {
        $fp = [string]$in.tool_input.file_path
        if (-not $fp) { $fp = [string]$in.tool_input.notebook_path }
        if ($fp -and -not [System.IO.Path]::IsPathRooted($fp)) { $fp = Join-Path $cwd $fp }
        $subject = $fp
        $pn = Norm $fp
        foreach ($d in $drives) { if ($pn.StartsWith($d)) { Flag 'deny' "file edit on the shared drive ($($d.ToUpper()))" } }
        if ($pn -match '/\.claude/(settings[^/]*\.json|hooks/)') { Flag 'deny' 'edit to Claude Code settings or hooks (change these yourself)' }
        elseif ($pn -match '/\.claude/') { Flag 'ask' 'edit to a file under .claude/ (an agent runbook)' }
        if ((Under $pn $buildN) -or (Under $pn ($repoN + '/schemas'))) { Flag 'ask' 'edit to a build script or schema file' }
        if ($dataN -and (Under $pn $dataN)) {
            if (-not $currentN) { Flag 'deny' 'file edit in the data folder before any delivery has been fetched' }
            elseif (-not (Under $pn $currentN)) { Flag 'deny' "file edit in the data folder outside the current delivery's folder ($currentN)" }
        }
    }
}
catch {
    Flag 'ask' "build guard could not check this call ($($_.Exception.Message))"
}

# ---------- 10: audit log ----------
try {
    if ($auditDir) {
        New-Item -ItemType Directory -Force -Path $auditDir | Out-Null
        $line = '{0} | s:{1} | {2} | {3} | {4} | {5}' -f (Get-Date -Format 'yyyy-MM-ddTHH:mm:ss'),
            ($sessionId -replace '^(.{8}).*', '$1'), $tool, $(if ($decision) { $decision } else { 'rules' }),
            ($reasons -join ' '), ($subject -replace '\s*\r?\n\s*', ' ').Substring(0, [Math]::Min(2000, ($subject -replace '\s*\r?\n\s*', ' ').Length))
        Add-Content -Path (Join-Path $auditDir ("agent_audit_{0}.log" -f (Get-Date -Format 'yyyyMMdd'))) -Value $line -Encoding utf8
    }
} catch { }

if ($decision) {
    @{ hookSpecificOutput = @{
            hookEventName            = 'PreToolUse'
            permissionDecision       = $decision
            permissionDecisionReason = 'Build guard (build/.claude/hooks/build_guard.ps1): ' + ($reasons -join '; ')
        } } | ConvertTo-Json -Compress -Depth 4
}
exit 0
