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
#   - Hardcoded NLSDB column schemas added as $NlsdbCaseColumns / $NlsdbCaseLandColumns.
#
# Usage:
#   .\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607
#   .\BLM_DQIMP_OneStop_param_v4.ps1 -Date 20260607 -SchemaDir "C:\path\to\schemas"

param(
    [Parameter(Mandatory=$true)]
    [ValidatePattern('^\d{8}$')]
    [string]$Date,

    [Parameter(Mandatory=$false)]
    [string]$SchemaDir
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

$Dataset = 'blm_seta_dqimp'
$Bucket  = 'sandbox-blm-seta-dqimp-qaqc'
$base    = "gs://$Bucket/snapshots/$Date"

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
    $bt  = [char]96  # backtick for BQ standard SQL table quoting — avoids PS escape conflict
    $ext = "${bt}xentity-sandbox-huy.${Dataset}.${TableBase}_ext${bt}"
    $tbl = "${bt}xentity-sandbox-huy.${Dataset}.${TableBase}${bt}"
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
# NLSDB column schemas for external table DDL
# Shape is typed BYTES here to bypass BQ geography validation on load;
# converted to GEOGRAPHY via ST_GEOGFROMWKB in the CTAS step.
# ---------------------------------------------------------------------------

$NlsdbCaseColumns = @'
  OBJECTID INT64,
  ADMIN_STATE STRING,
  GEO_STATE STRING,
  BLM_PROD STRING,
  CSE_DISP STRING,
  CSE_TYPE_NR STRING,
  CSE_NR STRING,
  LEG_CSE_NR STRING,
  CSE_NAME STRING,
  CMMDTY STRING,
  FRMTN STRING,
  EFF_DT TIMESTAMP,
  EXP_DT TIMESTAMP,
  PRDCNG STRING,
  SALE_DT TIMESTAMP,
  CUST_NM_SEC STRING,
  PCT_INT_SEC FLOAT64,
  INT_REL_SEC STRING,
  CSE_DISP_DT TIMESTAMP,
  CSE_JURIS_CD STRING,
  CSE_JURIS_DESC STRING,
  CSE_WIDTH STRING,
  CSE_LGTH STRING,
  PAT_NR STRING,
  PAT_ISS_DT TIMESTAMP,
  SEG_MIN STRING,
  SEG_SUR STRING,
  PUB_DT TIMESTAMP,
  PUB_TYPE STRING,
  SUPP_USE STRING,
  TITLE_ACC_DT TIMESTAMP,
  FUND_BY STRING,
  SRC STRING,
  QLTY STRING,
  CSE_META STRING,
  PLSSIDS STRING,
  RCRD_ACRS FLOAT64,
  GIS_ACRS FLOAT64,
  PLSS_UPDATE_DT TIMESTAMP,
  ID STRING,
  SF_ID STRING,
  STAGE_ID STRING,
  Created TIMESTAMP,
  Modified TIMESTAMP,
  REC_TYPE_CSE_GRP STRING,
  MC_PATENTED STRING,
  MC_EXCLUDED STRING,
  MC_CONVEYED STRING,
  CSE_DISP_ACTION STRING,
  Shape_Length FLOAT64,
  Shape_Area FLOAT64,
  Shape BYTES
'@

$NlsdbCaseLandColumns = @'
  OBJECTID INT64,
  CSE_NR STRING,
  LEG_CSE_NR STRING,
  REC_TYPE_CSE_GRP STRING,
  BLM_PROD STRING,
  CSE_TYPE_NR STRING,
  CSE_LND_STATUS STRING,
  CSE_LND_STATUS_DT TIMESTAMP,
  CSE_LND_NR STRING,
  US_RIGHTS STRING,
  DOC_TYPE STRING,
  DOC_NR STRING,
  DOC_DT TIMESTAMP,
  SEG_MIN STRING,
  SEG_SUR STRING,
  LND_SELECTED_BY STRING,
  PRIORITY STRING,
  CSE_LND_ACRS FLOAT64,
  CSE_LND_ID STRING,
  SRC STRING,
  QLTY STRING,
  CSE_LND_META STRING,
  PLSSIDS STRING,
  PLSS_UPDATE_DT TIMESTAMP,
  SF_ID STRING,
  SF_CL_ID STRING,
  ID STRING,
  AGG_CLS_ID STRING,
  QLTY_CK STRING,
  Created TIMESTAMP,
  Modified TIMESTAMP,
  Shape_Length FLOAT64,
  Shape_Area FLOAT64,
  Shape BYTES
'@

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
Write-Host "  Dataset:    $Dataset"
Write-Host "  GCS base:   $base"
Write-Host "  Schema dir: $SchemaDir"
Write-Host ""

# --- MLRS tables: wildcard load CR_FULL then append MC ---
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
