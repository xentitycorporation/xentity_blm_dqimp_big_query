# BLM DQIMP - One-Stop Load + Cleanup (v4, wildcard BQ loads, PUBLIC EXTRACT variant)
#
# This is a modified version for use with NLSDB public extracts from the Esri API.
# Differences from the standard PS1:
#   - $NlsdbCaseColumns / $NlsdbCaseLandColumns match the ogr2ogr-path public extract
#     parquet schemas (fewer fields than standard GDB, dates as FLOAT64 epoch-ms,
#     Case has OBJECTID + fid, CaseLands has its own 28-field schema, geometry in
#     a WKB column named 'geom')
#   - CTAS converts each layer's epoch-ms FLOAT64 date fields to TIMESTAMP via
#     TIMESTAMP_MILLIS (per-layer -DateFields parameter)
#   - Third NLSDB load: StatusRecords_<date>.parquet -> nlsdb_status_records_<date>
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
# Schema/DDL correction (July 2026):
#   - Replaced the Feb 2026 ArcPro-path DDL (OBJECTID_1, nlsdb_Shape, Shape_Length/
#     Area, shared Case/CaseLands schema) with the ogr2ogr-path schemas verified
#     from the actual Oct/Nov 2025 parquet footers (pyarrow) and validated in BQ.
#   - CaseLands now has its own DDL and its own date-field conversions
#     (CSE_LND_STATUS_DT, DOC_DT, Created, Modified).
#   - Geometry: ext table declares 'geom BYTES'; CTAS drops the junk FLOAT64
#     'Shape' source column and writes ST_GEOGFROMWKB(geom) AS Shape.
#
# Usage:
#   .\BLM_DQIMP_OneStop_param_v4_public_extract.ps1 -Date 20260607
#   .\BLM_DQIMP_OneStop_param_v4_public_extract.ps1 -Date 20260607 -SchemaDir "C:\path\to\schemas"

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
        [Parameter(Mandatory=$true)][string]$ColumnsDdl,
        [Parameter(Mandatory=$true)][string[]]$DateFields
    )
    # Public extract variant: same geometry fix as standard, plus converts the layer's
    # epoch-ms FLOAT64 date fields ($DateFields) to TIMESTAMP for consistency with
    # standard BLM GDB data. The ogr2ogr-path parquets carry the geometry in a column
    # named 'geom' (WKB) and also contain a junk FLOAT64 column named 'Shape' from the
    # merged source data — both are dropped in the CTAS, and the repaired geography is
    # written out as 'Shape' to match the standard-GDB table shape.
    Write-Host "  Loading NLSDB PARQUET $TableBase (ext table + CTAS, public extract) ..."
    $bt  = [char]96  # backtick for BQ standard SQL table quoting — avoids PS escape conflict
    $ext = "${bt}xentity-sandbox-huy.${Dataset}.${TableBase}_ext${bt}"
    $tbl = "${bt}xentity-sandbox-huy.${Dataset}.${TableBase}${bt}"
    $exceptList = (@($DateFields) + @('Shape', 'geom')) -join ', '
    $dateConversions = ($DateFields | ForEach-Object {
        "  TIMESTAMP_MILLIS(CAST($_ AS INT64)) AS $_,"
    }) -join "`n"
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
SELECT
  * EXCEPT($exceptList),
$dateConversions
  ST_GEOGFROMWKB(geom, make_valid => TRUE) AS Shape
FROM $ext;
DROP TABLE $ext;
"@
    $sql | bq query --nouse_legacy_sql
}

# ---------------------------------------------------------------------------
# NLSDB column schemas for external table DDL
#
# These match the ogr2ogr-path public extract parquets (the standardized path —
# Oct/Nov 2025 pattern), verified July 2026 by reading the parquet footers of
# Case_20251005.parquet / CaseLands_20251005.parquet in GCS with pyarrow, and
# validated end-to-end against those files in BQ.
#   - Geometry lives in 'geom' (WKB BYTES) — converted to GEOGRAPHY named 'Shape'
#     via ST_GEOGFROMWKB in the CTAS step.
#   - 'Shape FLOAT64' is a junk numeric column carried over from the merged
#     source data — it is in the DDL (it exists in the parquet) but dropped
#     in the CTAS.
#   - The parquets also contain a 'geom_bbox' STRUCT (GeoParquet covering) which
#     is deliberately OMITTED from the DDL — BQ matches parquet columns by name.
#   - Date fields are FLOAT64 epoch-milliseconds; each load call passes its
#     layer's date fields via -DateFields for TIMESTAMP conversion in the CTAS.
# NOTE: a Feb 2026-style ArcPro-converted GDB (OBJECTID_1, nlsdb_Shape,
# Shape_Length/Area, TIMESTAMP dates) will NOT match this DDL — regenerate it
# from the actual parquet via pyarrow.parquet.read_schema() per the workflow doc.
# ---------------------------------------------------------------------------

$NlsdbCaseColumns = @'
  OBJECTID INT64,
  fid FLOAT64,
  ADMIN_STATE STRING,
  GEO_STATE STRING,
  BLM_PROD STRING,
  CSE_DISP STRING,
  CSE_TYPE_NR STRING,
  CSE_NR STRING,
  LEG_CSE_NR STRING,
  CSE_NAME STRING,
  CMMDTY STRING,
  CUST_NM_SEC STRING,
  PCT_INT_SEC FLOAT64,
  INT_REL_SEC STRING,
  CSE_DISP_DT FLOAT64,
  CSE_JURIS_CD STRING,
  CSE_JURIS_DESC STRING,
  CSE_WIDTH STRING,
  CSE_LGTH STRING,
  QLTY STRING,
  CSE_META STRING,
  RCRD_ACRS FLOAT64,
  SF_ID STRING,
  REC_TYPE_CSE_GRP STRING,
  CSE_DISP_ACTION STRING,
  Shape FLOAT64,
  FRMTN STRING,
  EFF_DT FLOAT64,
  EXP_DT FLOAT64,
  PRDCNG STRING,
  SALE_DT FLOAT64,
  SRC STRING,
  MC_PATENTED STRING,
  MC_EXCLUDED STRING,
  MC_CONVEYED STRING,
  PAT_NR STRING,
  SEG_MIN STRING,
  SEG_SUR STRING,
  SUPP_USE STRING,
  geom BYTES
'@
$NlsdbCaseDateFields = @('CSE_DISP_DT', 'EFF_DT', 'EXP_DT', 'SALE_DT')

$NlsdbCaseLandColumns = @'
  OBJECTID INT64,
  CSE_NR STRING,
  LEG_CSE_NR STRING,
  REC_TYPE_CSE_GRP STRING,
  BLM_PROD STRING,
  CSE_TYPE_NR STRING,
  CSE_LND_STATUS STRING,
  CSE_LND_STATUS_DT FLOAT64,
  CSE_LND_NR STRING,
  US_RIGHTS STRING,
  DOC_TYPE STRING,
  DOC_NR STRING,
  DOC_DT FLOAT64,
  SEG_MIN STRING,
  SEG_SUR STRING,
  LND_SELECTED_BY STRING,
  PRIORITY STRING,
  CSE_LND_ACRS FLOAT64,
  CSE_LND_ID STRING,
  QLTY STRING,
  CSE_LND_META STRING,
  SF_ID STRING,
  SF_CL_ID STRING,
  ID STRING,
  AGG_CLS_ID STRING,
  QLTY_CK STRING,
  Created FLOAT64,
  Modified FLOAT64,
  Shape FLOAT64,
  geom BYTES
'@
$NlsdbCaseLandDateFields = @('CSE_LND_STATUS_DT', 'DOC_DT', 'Created', 'Modified')

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
                       -ColumnsDdl $NlsdbCaseColumns `
                       -DateFields $NlsdbCaseDateFields
} "bq load nlsdb_case_$Date"

Invoke-Step {
    BqLoadNlsdbParquet -TableBase "nlsdb_case_land_$Date" `
                       -GcsParquetPath "$base/CaseLands_$Date.parquet" `
                       -ColumnsDdl $NlsdbCaseLandColumns `
                       -DateFields $NlsdbCaseLandDateFields
} "bq load nlsdb_case_land_$Date"

# Status Records (CSE_DISP = 'Status Record') are split out of the Case layer by
# prepare_snapshot_public_extract.py and loaded to their own table so the main
# nlsdb_case_<date> table stays consistent with standard-GDB months. Same layer,
# same columns DDL. If the parquet is absent (the month's Case layer had no
# CSE_DISP field, so no split was possible), this step fails with a WARNING —
# benign for this load, but see the WARNING the .py printed about nlsdb_case.
Invoke-Step {
    BqLoadNlsdbParquet -TableBase "nlsdb_status_records_$Date" `
                       -GcsParquetPath "$base/StatusRecords_$Date.parquet" `
                       -ColumnsDdl $NlsdbCaseColumns `
                       -DateFields $NlsdbCaseDateFields
} "bq load nlsdb_status_records_$Date"

# --- GCS cleanup: intentionally NOT automated (July 2026 decision) ---
# The .load files in GCS are the recovery point for this script: if any bq load
# fails, the fix is to re-run this PS1 — which only works while the .load files
# still exist. Automating cleanup would turn a partial failure into a 2+ hour
# re-run of prepare_snapshot_public_extract.py. After the Step 5 row-count
# verification passes, clean up manually (Step 6 in BLM_Monthly_Data_Snapshot.md):
#   gsutil -m rm "$base/**/*.load"

Write-Host ""
Write-Host "=== Done for $Date. Review any WARNING/ERROR lines above. ==="
Read-Host "Press Enter to close"
