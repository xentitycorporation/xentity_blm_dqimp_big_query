-- Serial numbers present on one side but not the other, with Case Type Group/Subgroup.
--
-- Supersedes the original version of this file, which carried three bugs (KB S6.4.1).
-- Promoted from the validated local correction, Serial_Trace_Testing\n-- Serial_NR_List_Missing_status_csetyp_DRAFT.sql. Companion rollup:
-- Serial_NR_Count_by_Subgroup_DRAFT.sql.
-- Row-level list of serials present on one side but not the other, with Case Type
-- Group/Subgroup. Companion to Serial_NR_Count_by_Subgroup_DRAFT.sql (the rollup).
--
-- Fixes applied vs. the original (same three bugs, see Serial_NR_Count_by_Subgroup_DRAFT.sql
-- for full detail/quantification):
--   1. NLSDB rows classified via their own CSE_TYPE_NR, not by joining back to blm_case
--      on serial number (that join is circular for exactly the "missing from MLRS" rows
--      and always returned NULL group/subgroup — confirmed empirically against 32,490
--      historical missing_from_mlrs rows already in the manager's tracking workbook:
--      0 of them had a populated Case Type Group).
--   2. MLRS base population excludes both Status AND Bond record types by RECORDTYPEID,
--      not just Status via CASE_STATUS != 'STATUS RECORD' (Bond cases don't exist in
--      NLSDB by design and were leaking through as false "missing from NLSDB" rows).
--   3. Product-code joins use SAFE_CAST(...AS INT64) on both sides instead of STRING
--      comparison, so zero-padded codes (e.g. '007502') match correctly regardless of
--      whether the lookup's `BLM Product Code` column is currently typed STRING or
--      INTEGER (it has been reloaded as both).
--
-- Also carries the same unclassified breakout as the rollup (added 2026-08-11): a case
-- whose product code exists in blm_product but has no taxonomy row is labelled with that
-- code rather than left blank. On 20260802 that is `380800` "ABANDONED MINE LAND INV".

DECLARE snapshot_date STRING DEFAULT '20260802';
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

EXECUTE IMMEDIATE FORMAT("""
  WITH dedup_product AS (
    SELECT ID, CASE_TYPE_CODE
    FROM (
      SELECT ID, CASE_TYPE_CODE,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) AS rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    )
    WHERE rn = 1
  ),

  known_product_codes AS (
    SELECT DISTINCT CASE_TYPE_CODE FROM dedup_product WHERE CASE_TYPE_CODE IS NOT NULL
  ),

  group_lookup AS (
    SELECT DISTINCT
      SAFE_CAST(`BLM Product Code` AS INT64) AS product_code,
      `Case Type Group` AS case_type_group,
      `Case Type Subgroup` AS case_type_subgroup
    FROM `%s`
  ),

  mlrs AS (
    SELECT DISTINCT
      bc.ID,
      bc.SERIAL_NUMBER__C,
      bc.CASE_STATUS,
      IFNULL(lk.case_type_group, '(Unclassified)') AS case_type_group,
      CASE
        WHEN lk.case_type_group IS NOT NULL THEN lk.case_type_subgroup
        WHEN dp.CASE_TYPE_CODE IS NOT NULL
          THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
        ELSE '(no case type code / not in blm_product)'
      END AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS bc
    LEFT JOIN dedup_product AS dp ON bc.BLM_PRODUCT = dp.ID
    LEFT JOIN group_lookup AS lk ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = lk.product_code
    WHERE bc.RECORDTYPEID NOT IN ('0123d0000005ISFAA2', '0123d0000004QFQAA2')  -- Status, Bond
      AND bc.SERIAL_NUMBER__C IS NOT NULL
  ),

  nlsdb AS (
    SELECT DISTINCT
      nc.SF_ID,
      nc.CSE_NR,
      nc.CSE_DISP,
      IFNULL(lk.case_type_group, '(Unclassified)') AS case_type_group,
      CASE
        WHEN lk.case_type_group IS NOT NULL THEN lk.case_type_subgroup
        WHEN kpc.CASE_TYPE_CODE IS NOT NULL
          THEN CONCAT('BLM Product Code ', kpc.CASE_TYPE_CODE, ' (not in taxonomy)')
        ELSE '(no case type code / not in blm_product)'
      END AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` AS nc
    LEFT JOIN group_lookup AS lk ON SAFE_CAST(nc.CSE_TYPE_NR AS INT64) = lk.product_code
    LEFT JOIN known_product_codes AS kpc ON nc.CSE_TYPE_NR = kpc.CASE_TYPE_CODE
    WHERE nc.CSE_NR IS NOT NULL
  )

  SELECT
    nlsdb.CSE_NR AS missing_from_mlrs,
    nlsdb.SF_ID AS missing_from_mlrs_id,
    nlsdb.CSE_DISP AS missing_from_mlrs_disposition,
    nlsdb.case_type_group AS missing_from_mlrs_case_type_group,
    nlsdb.case_type_subgroup AS missing_from_mlrs_case_type_subgroup,

    mlrs.SERIAL_NUMBER__C AS missing_from_nlsdb,
    mlrs.ID AS missing_from_nlsdb_id,
    mlrs.CASE_STATUS AS missing_from_nlsdb_disposition,
    mlrs.case_type_group AS missing_from_nlsdb_case_type_group,
    mlrs.case_type_subgroup AS missing_from_nlsdb_case_type_subgroup
  FROM mlrs
  FULL OUTER JOIN nlsdb
    ON mlrs.SERIAL_NUMBER__C = nlsdb.CSE_NR
  WHERE mlrs.SERIAL_NUMBER__C IS NULL
     OR nlsdb.CSE_NR IS NULL
""",
snapshot_date, lookup_table, snapshot_date, snapshot_date);
