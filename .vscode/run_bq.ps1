# Runs a .sql file from this repo against BigQuery via `bq`, feeding it on stdin via real OS-level
# file redirection (`cmd /c "bq ... < file"`) rather than any PowerShell/.NET stdin-stream API.
#
# That distinction matters and cost real time to pin down (2026-09-04). The original
# `Get-Content file | bq` pattern (PowerShell pipeline) injected a UTF-8 BOM that bq's SQL parser
# rejects as `Syntax error: Illegal input character "\357" at [1:1]`, even though the .sql file
# itself has no BOM. Tracing it further: `[Diagnostics.Process]::Start()` with
# `RedirectStandardInput` -- even writing raw bytes straight to `.StandardInput.BaseStream`,
# bypassing any text encoding of our own -- *still* produced the same corrupted bytes on the far
# side (confirmed with a throwaway Python script that echoed exactly what it received on stdin:
# the clean bytes we sent arrived with `\xef\xbb\xbf` prepended). Root cause: .NET Framework's
# `Process.StandardInput` always lazily creates its own internal StreamWriter using this machine's
# default console encoding (confirmed elsewhere to be UTF-8 *with* BOM) the moment the property is
# touched, and `ProcessStartInfo` on .NET Framework (what Windows PowerShell 5.1 runs on) has no
# `StandardInputEncoding` property to override that -- only `StandardOutputEncoding` and
# `StandardErrorEncoding` exist, which don't help here. So any .NET stdin-stream approach on this
# machine is a dead end regardless of how carefully the writing code is written.
#
# `cmd /c "... < file"` sidesteps all of it: cmd.exe's `<` redirects a file handle directly onto
# the child's stdin at the OS level, with no .NET StreamWriter and no PowerShell pipeline text
# conversion anywhere in the path. This exact syntax was verified working in a one-off manual
# test; it only broke when inlined directly into a VS Code tasks.json "command" string (VS Code
# hands powershell.exe the whole command line as a single string, which goes through a native
# command-line re-parse pass before PowerShell's own script parser sees it, and that re-parse
# pass mangled the backtick-escaped quotes needed to keep `<` inside a string literal). Putting it
# in a real .ps1 file avoids that: PowerShell reads script text directly off disk, so there is no
# shell string for anything to re-parse.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dryrun', 'run', 'csv')]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [string]$SqlFile,

    [string]$OutDir
)

function Invoke-BqWithFile {
    param(
        [string[]]$BqArgs,
        [string]$SqlFile
    )

    # True OS-level stdin redirection via cmd.exe -- see the file header for why this is the
    # only approach on this machine that doesn't end up with a BOM injected ahead of the query.
    $joinedArgs = $BqArgs -join ' '
    $output = cmd /c "bq $joinedArgs < `"$SqlFile`"" 2>&1
    $script:BqExitCode = $LASTEXITCODE
    return $output -split "`r?`n"
}

function Assert-BqSucceeded {
    # 2026-10-13: a failed `bq` call (e.g. an expired sign-in) used to fall through to the CSV writer, which
    # saved the error text as a CSV and reported it as data rows. Stop instead, show the error, exit non-zero.
    param([string[]]$Lines)

    $failed = ($script:BqExitCode -ne 0) -or ($Lines | Where-Object { $_ -match '^(BigQuery error|ERROR:)' })
    if ($failed) {
        $Lines | Where-Object { $_.Trim() } | ForEach-Object { [Console]::Error.WriteLine($_) }
        if ($Lines -match 'Reauthentication failed|auth tokens') {
            [Console]::Error.WriteLine('Sign-in expired: run `gcloud auth login` in a terminal, then retry the task.')
        }
        exit 1
    }
}

function Select-ResultTables {
    # `bq --format=pretty` output for a script, statement by statement (verified 2026-10-13):
    #   - the statement's SQL text, then marker lines `-- at Dynamic SQL[2:5]` / `-- at [3:1]` (a static statement
    #     ends its own last line with `; -- at [8:1]` instead);
    #   - then its result table, drawn with `+` and `|` lines; or NOTHING if a SELECT returned zero rows; or a
    #     message such as `Created ...` for a statement that only builds a temp table.
    # Keep the result tables, show an empty SELECT as "no rows", drop the SQL text, markers and messages, and
    # label each result when there is more than one. (2026-10-13: replaces Strip-DynamicSqlEcho, which kept only
    # what followed the LAST marker and so showed only the last result of a multi-result script.)
    param([string[]]$Lines)

    $results = New-Object System.Collections.Generic.List[object]
    $table = $null; $firstWord = $null; $skipMessage = $false
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        $line = $Lines[$i]
        if ($line -match '^[+|]') {
            if ($null -eq $table) { $table = New-Object System.Collections.Generic.List[string] }
            $table.Add($line); continue
        }
        if ($null -ne $table) { $results.Add($table); $table = $null }
        if ($line -match '^-- at Dynamic SQL' -or -not $line.Trim()) { continue }
        if ($skipMessage) { $skipMessage = $false; continue }
        if ($null -eq $firstWord) { $firstWord = ($line.Trim() -split '\s+')[0].ToUpper() }
        if ($line -match '-- at \[\d+:\d+\]\s*$') {
            $next = if ($i + 1 -lt $Lines.Count) { $Lines[$i + 1] } else { '' }
            $isQuery = $firstWord -in @('SELECT', 'WITH', '(SELECT', '(WITH')
            if ($next -notmatch '^[+|]') {
                if ($isQuery) { $results.Add('(no rows)') } else { $skipMessage = $true }
            }
            $firstWord = $null
        }
    }
    if ($null -ne $table) { $results.Add($table) }
    if ($results.Count -eq 0) { return $Lines | Where-Object { $_ -notmatch '^-- at ' } }
    $out = New-Object System.Collections.Generic.List[string]
    for ($k = 0; $k -lt $results.Count; $k++) {
        $label = if ($results.Count -gt 1) { "-- Result $($k + 1) of $($results.Count)" } else { $null }
        if ($results[$k] -is [string]) {
            $out.Add(''); $out.Add((@($label, 'no rows') | Where-Object { $_ }) -join ': ')
        } else {
            if ($label) { $out.Add(''); $out.Add($label) }
            $out.AddRange([string[]]$results[$k])
        }
    }
    return $out
}

switch ($Mode) {
    'dryrun' {
        # CAVEAT: --dry_run reports real byte estimates only for plain static SQL. For the
        # DECLARE + EXECUTE IMMEDIATE FORMAT(...) pattern used by most queries in this repo,
        # BigQuery cannot analyse the dynamic SQL statically and always reports "0 bytes" --
        # this confirms the script PARSES, not that it's cheap to run.
        $lines = Invoke-BqWithFile -BqArgs @('query', '--use_legacy_sql=false', '--dry_run') -SqlFile $SqlFile
        Assert-BqSucceeded -Lines $lines
        $lines
    }
    'run' {
        $lines = Invoke-BqWithFile -BqArgs @('query', '--quiet', '--use_legacy_sql=false', '--format=pretty', '--max_rows=200') -SqlFile $SqlFile
        Assert-BqSucceeded -Lines $lines
        Select-ResultTables -Lines $lines
    }
    'csv' {
        # SAFETY: caller must pass -OutDir pointing OUTSIDE this repo's working tree (this repo is PUBLIC; results
        # never go in it). tasks.json derives it as the sibling Query_Results folder, never a path inside the repo.
        #
        # 2026-10-13: CSV mode now runs run_bq_csv.py (BigQuery Python client) instead of `bq --format=csv`, so a
        # script that returns several result tables writes one CSV per table (`__part1`, `__part2`, ...) instead
        # of only the last one. See that file's header for why `bq` cannot split them reliably. It uses the repo's
        # .venv (requirements.txt) and the credentials from `gcloud auth application-default login`.
        $python = Join-Path $PSScriptRoot '..\.venv\Scripts\python.exe'
        if (-not (Test-Path $python)) {
            [Console]::Error.WriteLine("No Python environment at $python")
            [Console]::Error.WriteLine('Create it from the repo folder: python -m venv .venv, then .venv\Scripts\pip install -r requirements.txt')
            exit 1
        }
        & $python (Join-Path $PSScriptRoot 'run_bq_csv.py') $SqlFile $OutDir
        exit $LASTEXITCODE
    }
}
