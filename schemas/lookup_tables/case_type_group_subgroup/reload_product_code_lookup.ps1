<#
.SYNOPSIS
    Reload the BigQuery Case Type Group / Subgroup lookup table from the committed CSV.

.DESCRIPTION
    Target table : xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup
    Source       : Product Code with Case Type Subgroups.csv  (this folder, authoritative)

    ####################################################################################
    #  ALWAYS LOAD WITH THE EXPLICIT SCHEMA. NEVER LET BIGQUERY AUTO-DETECT.           #
    #                                                                                  #
    #  `BLM Product Code` MUST be STRING. 22 of the 966 codes carry leading zeros      #
    #  (000000, 000445, 007500, 008500 ...). Auto-detect types the column as INTEGER,  #
    #  which strips those zeros: '007500' becomes 7500.                                #
    #                                                                                  #
    #  The join partner, blm_product.CASE_TYPE_CODE, is a zero-padded STRING. Once the #
    #  lookup is INTEGER the join either:                                              #
    #     * errors  -- "uncomparable types STRING and INT64"          (loud, safe), or #
    #     * SILENTLY MISSES all 22 codes if someone bridges it with CAST(... AS        #
    #       STRING), because '7500' != '007500'                       (quiet, harmful) #
    #                                                                                  #
    #  21,432 MLRS cases sit on those 22 codes. They drop to Unclassified.             #
    #                                                                                  #
    #  This has now regressed THREE times (KB 6.4.1 bug 3; KB 9.1). The schema file    #
    #  next to this script is the fix -- use it every time.                            #
    ####################################################################################

    An inline --schema string cannot express these column names, because they contain
    spaces. That is very likely WHY auto-detect keeps getting reached for. Use the
    JSON schema file.

.NOTES
    Authoritative source of the taxonomy is THIS CSV, in the repo -- not the .xlsx.
    See README.md in this folder for the update workflow.

    Verified working 2026-08-11.
#>

[CmdletBinding()]
param(
    [string]$Project = "xentity-sandbox-huy",
    [string]$Dataset = "blm_seta_dqimp",
    [string]$Table   = "Product_Code_Case_Type_Group_Subgroup",
    [switch]$SkipBackup
)

$ErrorActionPreference = "Stop"
$here   = Split-Path -Parent $MyInvocation.MyCommand.Path
$csv    = Join-Path $here "Product Code with Case Type Subgroups.csv"
$schema = Join-Path $here "Product_Code_Case_Type_Group_Subgroup_schema.json"
$target = "${Project}:${Dataset}.${Table}"

foreach ($f in @($csv, $schema)) {
    if (-not (Test-Path $f)) { throw "Required file not found: $f" }
}

# --- Back up the live table first. `bq load --replace` is not reversible, and this
#     table is shared. Skip only if you have another copy.
if (-not $SkipBackup) {
    $backup = "${Project}:${Dataset}.${Table}_backup_$(Get-Date -Format 'yyyyMMdd')"
    Write-Host "Backing up $target -> $backup"
    bq cp -f $target $backup
    if ($LASTEXITCODE -ne 0) { throw "Backup failed - aborting before --replace." }
}

Write-Host "Loading $csv -> $target (explicit STRING schema)"
bq load `
    --source_format=CSV `
    --replace `
    --skip_leading_rows=1 `
    --encoding=UTF-8 `
    --schema="$schema" `
    $target `
    "$csv"
if ($LASTEXITCODE -ne 0) { throw "Load failed." }

# --- Post-load verification. A load that "succeeds" can still be wrong: the failure
#     mode this guards against produces a perfectly valid table with silent join misses.
Write-Host "`nVerifying..."
$sql = @"
SELECT
  COUNT(*)                                                   AS rows_total,
  COUNT(DISTINCT ``BLM Product Code``)                        AS distinct_codes,
  COUNTIF(STARTS_WITH(``BLM Product Code``, '0'))             AS leading_zero_codes,
  COUNT(DISTINCT ``Case Type Group``)                         AS groups_,
  COUNT(DISTINCT ``Case Type Subgroup``)                      AS subgroups_
FROM ``${Project}.${Dataset}.${Table}``
"@
$sql | bq query --use_legacy_sql=false --format=pretty

Write-Host @"

EXPECTED (as of 2026-08-11): 966 rows / 966 distinct codes / 22 leading-zero codes / 7 groups / 32 subgroups.
leading_zero_codes = 0 means the schema did NOT apply -- the load is WRONG. Restore the backup and retry.

Next: run verify_case_type_group_coverage.py in this folder to reconcile the taxonomy
against blm_product and the subgroup queries.
"@
