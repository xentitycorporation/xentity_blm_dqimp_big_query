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
    return $output -split "`r?`n"
}

function Strip-DynamicSqlEcho {
    # For DECLARE + EXECUTE IMMEDIATE scripts, bq echoes the assembled dynamic SQL to stdout
    # before the real results, ending in lines like `-- at Dynamic SQL[2:5]` / `-- at [3:1]`.
    # Keep everything after the LAST such line. Plain static SQL has no such marker and passes
    # through untouched.
    param([string[]]$Lines)

    $lastMarker = -1
    for ($i = 0; $i -lt $Lines.Count; $i++) {
        if ($Lines[$i] -match '^-- at ') { $lastMarker = $i }
    }
    if ($lastMarker -ge 0) { return $Lines[($lastMarker + 1)..($Lines.Count - 1)] }
    return $Lines
}

switch ($Mode) {
    'dryrun' {
        # CAVEAT: --dry_run reports real byte estimates only for plain static SQL. For the
        # DECLARE + EXECUTE IMMEDIATE FORMAT(...) pattern used by most queries in this repo,
        # BigQuery cannot analyse the dynamic SQL statically and always reports "0 bytes" --
        # this confirms the script PARSES, not that it's cheap to run.
        $lines = Invoke-BqWithFile -BqArgs @('query', '--use_legacy_sql=false', '--dry_run') -SqlFile $SqlFile
        $lines
    }
    'run' {
        $lines = Invoke-BqWithFile -BqArgs @('query', '--use_legacy_sql=false', '--format=pretty', '--max_rows=200') -SqlFile $SqlFile
        Strip-DynamicSqlEcho -Lines $lines
    }
    'csv' {
        # SAFETY: caller must pass -OutDir pointing OUTSIDE this repo's working tree (this repo
        # is PUBLIC and query results contain live BLM case data). tasks.json derives it as the
        # sibling Query_Results folder, never a path inside the repo.
        if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }
        $base = [System.IO.Path]::GetFileNameWithoutExtension($SqlFile)
        $dest = Join-Path $OutDir ($base + '_' + (Get-Date -Format 'yyyyMMdd_HHmmss') + '.csv')
        $lines = Invoke-BqWithFile -BqArgs @('query', '--use_legacy_sql=false', '--format=csv', '--max_rows=100000') -SqlFile $SqlFile
        $clean = Strip-DynamicSqlEcho -Lines $lines
        # NOT `Out-File -Encoding utf8` -- Windows PowerShell 5.1 (unlike PowerShell 7+) always
        # writes a UTF-8 BOM with that encoding name, which corrupts the first column header
        # (`test_connection` -> `﻿test_connection`) for any downstream `pandas.read_csv`.
        # Confirmed 2026-09-04. WriteAllLines with an explicit no-BOM UTF8Encoding avoids it --
        # this is a plain file write, not a process-stdin pipe, so none of the Console/Process
        # default-encoding issues documented above apply here.
        [System.IO.File]::WriteAllLines($dest, $clean, (New-Object System.Text.UTF8Encoding($false)))
        Write-Host "Wrote $dest ($($clean.Count - 1) data rows)"
    }
}
