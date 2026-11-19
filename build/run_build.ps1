<#
run_build.ps1 — one entry point for each step of the monthly build, used by the VS Code tasks
and by the build agent (.claude/skills/monthly-build). It runs the existing scripts in this
folder; it does not replace them.

  -Step preflight   tools, sign-ins, shared folders, disk space (changes nothing)
  -Step fetch       copy the next delivery's zips from the shared folders (fetch_delivery.py)
  -Step prepare     prepare_snapshot.py in the month folder (-CheckOnly: layout checks, no upload)
  -Step load        BLM_DQIMP_OneStop_param_v4.ps1 -NoPause (-CheckOnly: pre-flight checks only)
  -Step verify      verify_snapshot.py: compare with the previous snapshot

Each step activates the conda env from the config, writes its full output to a log file
(printed at the start), and exits with the underlying script's exit code:
  0 OK · 1 error · 2 layout change (prepare/load) · 3 nothing new (fetch) · 4 disk space (fetch)
  5 needs a person (fetch) · 6 flags to look at (verify)

Machine paths come from monthly_build.local.json beside this script (copy
monthly_build.example.json). The month folder for -Date is found from the delivery_<date>.json
that the fetch step writes; pass -MonthFolder for a delivery fetched by hand.

Examples:
  .\run_build.ps1 -Step preflight
  .\run_build.ps1 -Step fetch
  .\run_build.ps1 -Step prepare -Date 20261115 -CheckOnly
  .\run_build.ps1 -Step prepare -Date 20261115
  .\run_build.ps1 -Step load    -Date 20261115
  .\run_build.ps1 -Step verify  -Date 20261115
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('preflight', 'fetch', 'prepare', 'load', 'verify')]
    [string]$Step,

    [ValidatePattern('^(\d{8})?$')]
    [string]$Date = '',

    [switch]$CheckOnly,
    [switch]$AllowRebuild,
    [string]$MonthFolder = '',
    [string]$Config = ''
)

$ErrorActionPreference = 'Continue'
$BuildDir = $PSScriptRoot
$RepoDir = Split-Path $BuildDir -Parent
if (-not $Config) { $Config = Join-Path $BuildDir 'monthly_build.local.json' }

if (-not (Test-Path $Config)) {
    Write-Host "ERROR: $Config not found." -ForegroundColor Red
    Write-Host "  Copy monthly_build.example.json to monthly_build.local.json and fill in this machine's paths." -ForegroundColor Red
    exit 1
}
try {
    $cfg = Get-Content -Raw $Config | ConvertFrom-Json -ErrorAction Stop
} catch {
    Write-Host "ERROR: $Config is not valid JSON (write Windows paths with doubled backslashes): $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# --- conda env (ogr2ogr with Parquet + GEOS, fiona) ---
$hook = Join-Path $cfg.conda_root 'shell\condabin\conda-hook.ps1'
if (-not (Test-Path $hook)) {
    Write-Host "ERROR: conda hook not found at $hook (check conda_root in $Config)" -ForegroundColor Red
    exit 1
}
. $hook
conda activate $cfg.conda_env
$py = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $py -or $py -notlike "*$($cfg.conda_env)*") {
    Write-Host "ERROR: could not activate conda env '$($cfg.conda_env)' (python resolves to '$py')" -ForegroundColor Red
    exit 1
}

# --- month folder for steps that need one ---
function Find-MonthFolder {
    if ($MonthFolder) { return (Resolve-Path $MonthFolder).Path }
    $marker = Get-ChildItem -Path $cfg.data_root -Filter "delivery_$Date.json" -Recurse -Depth 2 -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($marker) { return $marker.DirectoryName }
    Write-Host "ERROR: no delivery_$Date.json under $($cfg.data_root). Run -Step fetch first, or pass -MonthFolder." -ForegroundColor Red
    exit 1
}

if ($Step -in @('prepare', 'load', 'verify') -and -not $Date) {
    Write-Host "ERROR: -Step $Step needs -Date YYYYMMDD" -ForegroundColor Red
    exit 1
}

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
if ($Step -in @('preflight', 'fetch')) {
    $logDir = Join-Path $cfg.data_root '_build_logs'
} else {
    $folder = Find-MonthFolder
    $logDir = Join-Path $folder 'build_logs'
}
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$suffix = if ($CheckOnly) { '_checkonly' } else { '' }
$log = Join-Path $logDir ("{0}{1}{2}_{3}.log" -f $Step, $(if ($Date) { "_$Date" } else { '' }), $suffix, $stamp)
Write-Host "Log: $log"

function Invoke-Logged {
    # Run a native command; show its output live and write it to the log. Returns its exit code.
    param([string]$Exe, [string[]]$Arguments)
    "> $Exe $($Arguments -join ' ')" | Tee-Object -FilePath $log -Append | Out-Host
    & $Exe @Arguments 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $log -Append | Out-Host
    return $LASTEXITCODE
}

switch ($Step) {
    'preflight' {
        $fails = 0
        function Check([string]$Label, [scriptblock]$Test) {
            $ok = $false
            try { $ok = [bool](& $Test) } catch { $ok = $false }
            $line = if ($ok) { "  PASS  $Label" } else { "  FAIL  $Label" }
            $line | Tee-Object -FilePath $log -Append | Out-Host
            if (-not $ok) { $script:fails++ }
        }
        Check "conda env '$($cfg.conda_env)' active ($py)" { $true }
        Check 'ogr2ogr has the Parquet driver' { (ogr2ogr --formats 2>$null | Select-String 'Parquet') }
        Check 'python can import fiona' { python -c "import fiona" 2>$null; $LASTEXITCODE -eq 0 }
        Check 'gcloud is signed in' { gcloud auth print-access-token --quiet 2>$null | Out-Null; $LASTEXITCODE -eq 0 }
        Check 'bq can list the dataset' { bq ls --max_results=1 'xentity-sandbox-huy:blm_seta_dqimp' 2>$null | Out-Null; $LASTEXITCODE -eq 0 }
        Check 'gsutil can list the bucket' { gsutil ls 'gs://sandbox-blm-seta-dqimp-qaqc/' 2>$null | Out-Null; $LASTEXITCODE -eq 0 }
        Check "shared MLRS folder reachable" { Test-Path $cfg.mlrs_drop }
        Check "shared NLSDB folder reachable" { Test-Path $cfg.nlsdb_drop }
        Check "data folder exists ($($cfg.data_root))" { Test-Path $cfg.data_root }
        $drive = (Get-Item $cfg.data_root).PSDrive
        $freeGb = [math]::Round($drive.Free / 1GB)
        Check "at least 120 GB free on $($drive.Name): ($freeGb GB)" { $freeGb -ge 120 }
        $branch = git -C $RepoDir rev-parse --abbrev-ref HEAD 2>$null
        $dirty = @(git -C $RepoDir status --porcelain -- build schemas 2>$null).Count
        "  INFO  scripts from branch '$branch'; $dirty uncommitted change(s) under build/ and schemas/" |
            Tee-Object -FilePath $log -Append | Out-Host
        if ($fails) {
            "Preflight: $fails check(s) FAILED." | Tee-Object -FilePath $log -Append | Out-Host
            exit 1
        }
        'Preflight: all checks passed.' | Tee-Object -FilePath $log -Append | Out-Host
        exit 0
    }
    'fetch' {
        $a = @('-u', (Join-Path $BuildDir 'fetch_delivery.py'), '--config', $Config)
        if ($Date) { $a += @('--date', $Date) }
        if ($AllowRebuild) { $a += '--allow-rebuild' }
        exit (Invoke-Logged -Exe $py -Arguments $a)
    }
    'prepare' {
        Push-Location $folder
        try {
            $a = @('-u', (Join-Path $BuildDir 'prepare_snapshot.py'), '--date', $Date)
            if ($CheckOnly) { $a += '--check-only' }
            $code = Invoke-Logged -Exe $py -Arguments $a
        } finally { Pop-Location }
        exit $code
    }
    'load' {
        # PowerShell 7, like this wrapper. (The load script is saved as UTF-8 with a BOM so Windows
        # PowerShell 5.1 can also read its non-ASCII characters when it is run by hand.)
        $a = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', (Join-Path $BuildDir 'BLM_DQIMP_OneStop_param_v4.ps1'),
               '-Date', $Date, '-NoPause')
        if ($CheckOnly) { $a += '-CheckOnly' }
        exit (Invoke-Logged -Exe 'pwsh' -Arguments $a)
    }
    'verify' {
        $out = Join-Path $folder "verify_$Date.md"
        exit (Invoke-Logged -Exe $py -Arguments @('-u', (Join-Path $BuildDir 'verify_snapshot.py'), '--date', $Date, '--out', $out))
    }
}
