# BLM DQIMP - One-Stop Load + Cleanup (v4, wildcard BQ loads)
#
# Changes from v3:
#   - Added -SchemaDir parameter so JSON schemas don't have to live next to this script.
#     When omitted the folder is auto-detected (see the resolver below) rather than
#     hardcoded to one workstation.
#   - NLSDB Parquet names are now derived from $Date (Case_<DATE>.parquet /
#     CaseLands_<DATE>.parquet), matching what prepare_snapshot.py uploads.
#     No more hardcoded timestamps in the filenames.
#   - BQ dataset name is declared once in $Dataset.
#
# Changes from v4 (June 2026 — wildcard BQ loads):
#   - Removed gsutil compose step. prepare_snapshot.py now uploads raw per-state
#     .load files directly to GCS; BQ loads them via wildcard URI pattern.
#   - BqLoadCsv now does two calls per table: CR_FULL_<TABLE>_*.load (create),
#     then MC_<TABLE>_*.load (--append_table). No intermediate CSV needed.
#   - Uses --max_bad_records=0 so any malformed row (e.g. an embedded pipe in a
#     text field) fails the load loudly instead of being silently dropped.
#     prepare_snapshot.py validates column counts before upload to catch these.
#   - Cleanup simplified to a single gsutil -m rm *.load command.
#
# Bug fixes (June 2026 — post-first-run corrections):
#   - Removed --append_table from MC bq load: this flag is not supported by the
#     installed bq CLI version; default write disposition is already WRITE_APPEND.
#   - Added --replace to CR_FULL bq load so re-runs truncate stale data rather
#     than appending duplicates to existing tables.
#   - Changed GCS wildcard paths to per-table subdirectories ($base/$name/...)
#     to prevent prefix collision (e.g. ACTION matching ACTION_CASE, ACTION_LAND).
#     prepare_snapshot.py was updated simultaneously to upload to matching subdirs.
#   - Updated cleanup pattern from *.load to **/*.load for recursive subdir removal.
#   - Added --replace to BqLoadParquet for consistent re-run safety on NLSDB tables.
#
# NLSDB geometry fix (June 2026):
#   - Changed NLSDB load from bq load --autodetect to external table DDL + CTAS.
#     bq --autodetect recognizes GeoParquet metadata and validates Shape as GEOGRAPHY;
#     this rejects spherically-invalid polygons (crossing edges) even after ogr2ogr
#     -makevalid repairs planar invalidity. Fix: create a temp external table with Shape
#     typed as BYTES (bypasses geography validation), then CTAS with
#     ST_GEOGFROMWKB(Shape, make_valid => TRUE) to convert and repair in BigQuery.
#   - BqLoadParquet replaced by BqLoadNlsdbParquet with the three-step pattern.
#
# Layout checks (November 2026):
#   - The NLSDB column lists are no longer hardcoded here. They are read from
#     schemas/nlsdb_schemas/nlsdb_case.json and nlsdb_case_land.json — the same files
#     prepare_snapshot.py checks the Parquet files against. (The Azure migration changed
#     the NLSDB layout in October 2026; November was loaded with stale hardcoded lists and
#     BigQuery raised no error, because an external table matches Parquet columns by name.)
#   - Pre-flight checks run before ANY load, and stop the script if they fail:
#       1. the GCS snapshot folder holds exactly the 24 expected table folders and both
#          Parquet files;
#       2. BigQuery's own reading of each Parquet file (a temporary schema-detected
#          external table) has the columns and types in the layout JSON.
#   - -CheckOnly runs the pre-flight checks and loads nothing.
#
# Usage:
#   .\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607
#   .\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607 -CheckOnly
#   .\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607 -SchemaDir "C:\path\to\schemas\mlrs_export_schemas"

param(
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^\d{8}$')]
    [string]$Date,

    [Parameter(Mandatory=$false)]
    [string]$SchemaDir,

    [Parameter(Mandatory=$false)]
    [string]$NlsdbSchemaDir,

    [switch]$CheckOnly
)

Set-Location -Path $PSScriptRoot
$ErrorActionPreference = 'Continue'

# --- Locate the schema folder ------------------------------------------------
# NO path to any particular workstation appears in this script: the repo's own
# schemas\mlrs_export_schemas\ is the single source of truth. Search order:
#   1. any ancestor directory containing schemas\mlrs_export_schemas (the repo, any depth)
#   2. a schemas\ folder sitting next to this script
#   3. $env:BLM_DQIMP_SCHEMA_DIR — for running from a month folder outside the repo
# -SchemaDir overrides the search entirely.
$SchemaEnvVar = 'BLM_DQIMP_SCHEMA_DIR'

function Test-SchemaFolder {
    param([string]$Path)
    if (-not $Path) { return $false }
    if (-not (Test-Path $Path)) { return $false }
    return [bool](Get-ChildItem -Path $Path -Filter '*_schema.json' -ErrorAction SilentlyContinue)
}

if (-not $SchemaDir) {
    # 1. walk up from this script looking for the repo's schemas\mlrs_export_schemas
    $directory = $PSScriptRoot
    while ($directory) {
        $candidate = Join-Path $directory 'schemas\mlrs_export_schemas'
        if (Test-SchemaFolder $candidate) { $SchemaDir = $candidate; break }
        $parent = Split-Path $directory -Parent
        if ($parent -eq $directory -or -not $parent) { break }
        $directory = $parent
    }
    # 2. a schemas\ folder beside this script
    if (-not $SchemaDir) {
        $beside = Join-Path $PSScriptRoot 'schemas'
        if (Test-SchemaFolder $beside) { $SchemaDir = $beside }
    }
    # 3. explicit environment override, for month folders outside the repo
    if (-not $SchemaDir) {
        $fromEnv = [Environment]::GetEnvironmentVariable($SchemaEnvVar)
        if (Test-SchemaFolder $fromEnv) { $SchemaDir = $fromEnv }
    }
    if (-not $SchemaDir) {
        $envValue = [Environment]::GetEnvironmentVariable($SchemaEnvVar)
        if (-not $envValue) { $envValue = 'not set' }
        Write-Host "ERROR: could not locate the BQ schema JSONs. Searched:" -ForegroundColor Red
        Write-Host "         ancestor directories of $PSScriptRoot containing schemas\mlrs_export_schemas" -ForegroundColor Red
        Write-Host "         $(Join-Path $PSScriptRoot 'schemas')" -ForegroundColor Red
        Write-Host "         `$env:$SchemaEnvVar (currently: $envValue)" -ForegroundColor Red
        Write-Host "       Pass -SchemaDir <path>, or set $SchemaEnvVar." -ForegroundColor Red
        exit 1
    }
}

# Resolve SchemaDir so error messages show the full path
$SchemaDir = (Resolve-Path $SchemaDir -ErrorAction Stop).Path

# The NLSDB layout JSONs live beside the MLRS schema folder: schemas\nlsdb_schemas\.
if (-not $NlsdbSchemaDir) { $NlsdbSchemaDir = Join-Path (Split-Path $SchemaDir -Parent) 'nlsdb_schemas' }

$Project = 'xentity-sandbox-huy'
$Dataset = 'blm_seta_dqimp'
$Bucket  = 'sandbox-blm-seta-dqimp-qaqc'
$base    = "gs://$Bucket/snapshots/$Date"
$bt      = [char]96  # backtick for BQ standard SQL table quoting — avoids PS escape conflict

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Invoke-Step {
    param(
        [Parameter(Mandatory=$true)][scriptblock]$Action,
        [Parameter(Mandatory=$true)][string]$Label
    )
    try {
        & $Action
        $code = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } else { 0 }
        if ($code -eq 0) {
            Write-Host "  OK:      $Label"
        } else {
            Write-Host "  WARNING: $Label (exit $code)"
        }
    } catch {
        Write-Host "  ERROR:   $Label — $($_.Exception.Message)"
    }
}

function Stop-Build {
    # Print every problem found and stop before anything is loaded to BigQuery.
    param([string[]]$Problems, [string[]]$WhatToUpdate)
    Write-Host ""
    Write-Host ("!" * 60) -ForegroundColor Red
    Write-Host "STOPPED — the snapshot does not match the expected layout." -ForegroundColor Red
    Write-Host "Nothing has been loaded to BigQuery." -ForegroundColor Red
    Write-Host ("!" * 60) -ForegroundColor Red
    foreach ($p in $Problems) { Write-Host "  - $p" -ForegroundColor Red }
    Write-Host ""
    if ($WhatToUpdate) {
        Write-Host "If BLM changed the export on purpose, update:"
        foreach ($w in $WhatToUpdate) { Write-Host "  $w" }
        Write-Host "then re-run. If the change is not expected, raise it with BLM first."
    }
    try { Read-Host "Press Enter to close" | Out-Null } catch { }
    exit 2
}

function Read-NlsdbLayout {
    # [ordered] name -> BQ type from one NLSDB layout JSON; stops on a missing or invalid file.
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        Write-Host "ERROR: NLSDB layout file not found: $Path" -ForegroundColor Red
        Write-Host "       Pass -NlsdbSchemaDir <path> if it lives elsewhere." -ForegroundColor Red
        exit 1
    }
    try {
        $columns = Get-Content -Raw -Path $Path | ConvertFrom-Json -ErrorAction Stop
    } catch {
        Write-Host "ERROR: NLSDB layout file is not valid JSON: $Path" -ForegroundColor Red
        exit 1
    }
    $layout = [ordered]@{}
    foreach ($c in $columns) {
        if (-not $c.name -or -not $c.type) {
            Write-Host "ERROR: every entry in $Path needs a name and a type" -ForegroundColor Red
            exit 1
        }
        $layout[$c.name] = $c.type.ToUpper()
    }
    return $layout
}

function ConvertTo-ColumnsDdl {
    # "  NAME TYPE," lines for the external table DDL. Shape stays BYTES (see BqLoadNlsdbParquet).
    param($Layout)
    return (($Layout.Keys | ForEach-Object { "  $_ $($Layout[$_])" }) -join ",`n")
}

function Get-ParquetLayoutFromBq {
    # Ask BigQuery how it reads a Parquet file: create a schema-detected external table
    # (metadata only, no data scanned), read its columns, drop it.
    # Returns [ordered] name -> type, or $null if the file could not be read.
    param([string]$TableBase, [string]$GcsParquetPath)
    $probeName = "${TableBase}_layoutprobe"
    $probe = "${bt}${Project}.${Dataset}.${probeName}${bt}"
    "CREATE OR REPLACE EXTERNAL TABLE $probe OPTIONS (format = 'PARQUET', uris = ['$GcsParquetPath']);" |
        bq query --nouse_legacy_sql --quiet 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { return $null }
    $json = "SELECT column_name, data_type FROM ${bt}${Project}.${Dataset}.INFORMATION_SCHEMA.COLUMNS${bt} WHERE table_name = '$probeName' ORDER BY ordinal_position" |
        bq query --nouse_legacy_sql --format=json --max_rows=1000 2>$null | Select-Object -Last 1
    "DROP TABLE IF EXISTS $probe;" | bq query --nouse_legacy_sql --quiet 2>$null | Out-Null
    try { $rows = $json | ConvertFrom-Json -ErrorAction Stop } catch { return $null }
    if (-not $rows) { return $null }
    $layout = [ordered]@{}
    foreach ($r in $rows) { $layout[$r.column_name] = $r.data_type }
    return $layout
}

function Compare-NlsdbLayout {
    # Differences between BigQuery's reading of a Parquet file and its layout JSON.
    param($Found, $Expected, [string]$Label)
    $problems = @()
    foreach ($name in $Found.Keys) {
        $type = $Found[$name]
        # ogr2ogr adds a GeoParquet <geometry>_bbox struct; the DDL omits it on purpose.
        if ($name -like '*_bbox' -and $type -like 'STRUCT*') { continue }
        if (-not $Expected.Contains($name)) { $problems += "${Label}: NEW column $name ($type)" }
    }
    foreach ($name in $Expected.Keys) {
        $want = $Expected[$name]
        if (-not $Found.Contains($name)) { $problems += "${Label}: column $name ($want) is MISSING"; continue }
        $got = $Found[$name]
        # The WKB geometry column is detected as GEOGRAPHY; we load it as BYTES on purpose.
        if ($want -eq 'BYTES' -and $got -eq 'GEOGRAPHY') { continue }
        if ($got -ne $want) { $problems += "${Label}: column $name changed type $want -> $got" }
    }
    return $problems
}

function BqLoadCsv {
    param(
        [Parameter(Mandatory=$true)][string]$SchemaFile,
        [Parameter(Mandatory=$true)][string]$TableId,
        [Parameter(Mandatory=$true)][string]$GcsCsvPath,
        [switch]$Append
    )
    # Pass values through env vars so the pipe character in --field_delimiter="|"
    # is handled by cmd.exe (via bq.cmd) rather than PowerShell.
    # --quote="" disables CSV quote recognition: BLM .load files use no quoting,
    # so literal " in field values (e.g. case names) must not trigger CSV quoting logic.
    $env:BQ_SCHEMA = $SchemaFile
    $env:BQ_TABLE  = $TableId
    $env:BQ_PATH   = $GcsCsvPath
    try {
        Write-Host "  Loading $TableId ..."
        if ($Append) {
            # Default write disposition is WRITE_APPEND — no flag needed
            bq --% load --source_format=CSV --field_delimiter="|" --quote="" --skip_leading_rows=0 --max_bad_records=0 --schema=%BQ_SCHEMA% %BQ_TABLE% %BQ_PATH%
        } else {
            # --replace (WRITE_TRUNCATE) ensures re-runs start clean
            bq --% load --source_format=CSV --field_delimiter="|" --quote="" --skip_leading_rows=0 --max_bad_records=0 --replace --schema=%BQ_SCHEMA% %BQ_TABLE% %BQ_PATH%
        }
    } finally {
        Remove-Item Env:\BQ_SCHEMA, Env:\BQ_TABLE, Env:\BQ_PATH -ErrorAction SilentlyContinue
    }
}

function BqLoadNlsdbParquet {
    param(
        [Parameter(Mandatory=$true)][string]$TableBase,
        [Parameter(Mandatory=$true)][string]$GcsParquetPath,
        [Parameter(Mandatory=$true)][string]$ColumnsDdl
    )
    # bq load --autodetect detects GeoParquet metadata and validates Shape as GEOGRAPHY;
    # NLSDB data has spherically-invalid polygons that survive GEOS makevalid but fail
    # BQ's S2 validator. Fix: external table with Shape BYTES (no geography validation),
    # then CTAS with ST_GEOGFROMWKB(Shape, make_valid => TRUE).
    Write-Host "  Loading NLSDB PARQUET $TableBase (ext table + CTAS) ..."
    $ext = "${bt}${Project}.${Dataset}.${TableBase}_ext${bt}"
    $tbl = "${bt}${Project}.${Dataset}.${TableBase}${bt}"
    $sql = @"
CREATE OR REPLACE EXTERNAL TABLE $ext
(
$ColumnsDdl
)
OPTIONS (
  format = 'PARQUET',
  uris = ['$GcsParquetPath']
);
CREATE OR REPLACE TABLE $tbl AS
SELECT * EXCEPT(Shape), ST_GEOGFROMWKB(Shape, make_valid => TRUE) AS Shape
FROM $ext;
DROP TABLE $ext;
"@
    $sql | bq query --nouse_legacy_sql
}

# ---------------------------------------------------------------------------
# NLSDB layouts — read from schemas\nlsdb_schemas\ (the same files prepare_snapshot.py
# checks the Parquet files against). Shape is typed BYTES there to bypass BQ geography
# validation on load; it is converted to GEOGRAPHY via ST_GEOGFROMWKB in the CTAS step.
# ---------------------------------------------------------------------------

$NlsdbCaseLayoutFile     = Join-Path $NlsdbSchemaDir 'nlsdb_case.json'
$NlsdbCaseLandLayoutFile = Join-Path $NlsdbSchemaDir 'nlsdb_case_land.json'
$NlsdbCaseLayout         = Read-NlsdbLayout $NlsdbCaseLayoutFile
$NlsdbCaseLandLayout     = Read-NlsdbLayout $NlsdbCaseLandLayoutFile
$NlsdbCaseColumns        = ConvertTo-ColumnsDdl $NlsdbCaseLayout
$NlsdbCaseLandColumns    = ConvertTo-ColumnsDdl $NlsdbCaseLandLayout

# ---------------------------------------------------------------------------
# Table list (name + schema file)
# ---------------------------------------------------------------------------

$loads = @(
    @{ name='ACCOUNT_RPT';          schema='account_rpt_schema.json' },
    @{ name='ACTION';               schema='action_schema.json' },
    @{ name='ADMIN_UNIT';           schema='admin_unit_schema.json' },
    @{ name='ADMIN_UNIT_AGG';       schema='admin_unit_agg_schema.json' },
    @{ name='BLM_CASE';             schema='blm_case_schema.json' },
    @{ name='BLM_PRODUCT';          schema='blm_product_schema.json' },
    @{ name='CASE_ACTION';          schema='case_action_schema.json' },
    @{ name='CASE_CUSTOMER';        schema='case_customer_schema.json' },
    @{ name='CASE_LAND';            schema='case_land_schema.json' },
    @{ name='CASE_TRANSACTION';     schema='case_transaction_schema.json' },
    @{ name='INTREL_D';             schema='intrel_d_schema.json' },
    @{ name='PRODUCT_FEE';          schema='product_fee_schema.json' },
    @{ name='RPT_CALENDAR';         schema='rpt_calendar_schema.json' },
    @{ name='CASE_ACTION_STATUS_D'; schema='case_action_status_d_schema.json' },
    @{ name='COMMODITY_D';          schema='commodity_d_schema.json' },
    @{ name='CASE_GROUP_D';         schema='case_group_d_schema.json' },
    @{ name='MERIDIAN_D';           schema='meridian_d_schema.json' },
    @{ name='SURVEY_TYPE_D';        schema='survey_type_d_schema.json' },
    @{ name='ACTION_CASE';          schema='action_case_schema.json' },
    @{ name='ACTION_LAND';          schema='action_land_schema.json' },
    @{ name='CASE_ADMIN_UNIT';      schema='case_admin_unit_schema.json' },
    @{ name='RECORD_TYPE';          schema='record_type_schema.json' },
    @{ name='SUPPLEMENTAL_USE';     schema='supplemental_use_schema.json' },
    @{ name='US_RIGHT_CASE_LAND';   schema='us_right_case_land_schema.json' }
)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

Write-Host ""
Write-Host "=== BLM DQIMP Compose + Load + Cleanup — $Date ==="
Write-Host "  Dataset:       $Dataset"
Write-Host "  GCS base:      $base"
Write-Host "  Schema dir:    $SchemaDir"
Write-Host "  NLSDB layouts: $NlsdbSchemaDir"
Write-Host ""

# --- Pre-flight: check the snapshot's layout before loading anything ---
Write-Host "--- Pre-flight layout checks ---"
$problems = @()

# 1. GCS folder: exactly the 24 table folders, plus both Parquet files
$listing = @(gsutil ls "$base/" 2>$null)
if ($LASTEXITCODE -ne 0 -or -not $listing) {
    Stop-Build -Problems @("Could not list $base/ — is gsutil authenticated, and did prepare_snapshot.py finish?")
}
$folders = @($listing | Where-Object { $_ -match '/$' } | ForEach-Object { ($_.TrimEnd('/') -split '/')[-1] })
$files   = @($listing | Where-Object { $_ -notmatch '/$' } | ForEach-Object { ($_ -split '/')[-1] })
$expectedTables = @($loads | ForEach-Object { $_.name })
foreach ($t in $expectedTables) {
    if ($folders -notcontains $t) { $problems += "GCS: no $t folder in $base/ (no non-empty .load files were uploaded for it)" }
}
foreach ($f in $folders) {
    if ($expectedTables -notcontains $f) { $problems += "GCS: UNEXPECTED folder $f in $base/ — a table this script does not load" }
}
foreach ($p in @("Case_$Date.parquet", "CaseLands_$Date.parquet")) {
    if ($files -notcontains $p) { $problems += "GCS: $p is missing from $base/" }
}
if ($problems) {
    Stop-Build -Problems $problems -WhatToUpdate @(
        "$SchemaDir  (a <table>_schema.json per table)",
        "and the `$loads list in this script"
    )
}
Write-Host "  OK:      GCS holds the $($expectedTables.Count) table folders and both Parquet files"

# 2. NLSDB: BigQuery's reading of each Parquet file vs its layout JSON
$nlsdbChecks = @(
    @{ label='NLSDB Case';      base="nlsdb_case_$Date";      uri="$base/Case_$Date.parquet";      layout=$NlsdbCaseLayout;     file=$NlsdbCaseLayoutFile },
    @{ label='NLSDB CaseLands'; base="nlsdb_case_land_$Date"; uri="$base/CaseLands_$Date.parquet"; layout=$NlsdbCaseLandLayout; file=$NlsdbCaseLandLayoutFile }
)
foreach ($c in $nlsdbChecks) {
    $found = Get-ParquetLayoutFromBq -TableBase $c.base -GcsParquetPath $c.uri
    if ($null -eq $found) {
        $problems += "$($c.label): BigQuery could not read the columns of $($c.uri)"
        continue
    }
    $layerProblems = @(Compare-NlsdbLayout -Found $found -Expected $c.layout -Label $c.label)
    if ($layerProblems) { $problems += $layerProblems }
    else { Write-Host "  OK:      $($c.label) matches $(Split-Path $c.file -Leaf) ($($c.layout.Count) columns)" }
}
if ($problems) {
    Stop-Build -Problems $problems -WhatToUpdate @(
        $NlsdbCaseLayoutFile,
        $NlsdbCaseLandLayoutFile,
        "(this script builds the NLSDB tables from these files; check the DQ tests that",
        " use any column that was renamed or removed)"
    )
}

if ($CheckOnly) {
    Write-Host ""
    Write-Host "=== Check only — pre-flight checks passed for $Date. Nothing was loaded. ==="
    exit 0
}

# --- MLRS tables: wildcard load CR_FULL then append MC ---
Write-Host ""
Write-Host "--- MLRS loads ($($loads.Count) tables × 2 passes) ---"
foreach ($x in $loads) {
    $name   = $x.name
    $schema = Join-Path $SchemaDir $x.schema
    $table  = "$Dataset.$($name.ToLower())_$Date"
    $cr     = "$base/$name/CR_FULL_${name}_*.load"
    $mc     = "$base/$name/MC_${name}_*.load"

    Write-Host ""
    Write-Host "  [$name]"
    Invoke-Step { BqLoadCsv -SchemaFile $schema -TableId $table -GcsCsvPath $cr         } "bq load CR_FULL $name"
    Invoke-Step { BqLoadCsv -SchemaFile $schema -TableId $table -GcsCsvPath $mc -Append } "bq load MC $name (append)"
}

# --- NLSDB Parquet tables ---
Write-Host ""
Write-Host "--- NLSDB Parquet loads ---"
Invoke-Step {
    BqLoadNlsdbParquet -TableBase "nlsdb_case_$Date" `
                       -GcsParquetPath "$base/Case_$Date.parquet" `
                       -ColumnsDdl $NlsdbCaseColumns
} "bq load nlsdb_case_$Date"

Invoke-Step {
    BqLoadNlsdbParquet -TableBase "nlsdb_case_land_$Date" `
                       -GcsParquetPath "$base/CaseLands_$Date.parquet" `
                       -ColumnsDdl $NlsdbCaseLandColumns
} "bq load nlsdb_case_land_$Date"

# --- GCS cleanup: intentionally NOT automated (July 2026 decision) ---
# The .load files in GCS are the recovery point for this script: if any bq load
# fails, the fix is to re-run this PS1 — which only works while the .load files
# still exist. Automating cleanup would turn a partial failure into a 2+ hour
# re-run of prepare_snapshot.py. After the Step 5 row-count verification passes,
# clean up manually (Step 6 in BLM_Monthly_Data_Snapshot.md):
#   gsutil -m rm "$base/**/*.load"

Write-Host ""
Write-Host "=== Done for $Date. Review any WARNING/ERROR lines above. ==="
Read-Host "Press Enter to close"
