-- =====================================================================================
-- Test ID:      SYT30.1
-- Test name:    Serial numbers present in one system only
-- Test type:    Synchronization (tests/sync/) - a case in MLRS is also in NLSDB (and vice versa) and carries the same value in both
-- Catalog:      Test Catalog v2 row #117 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     STT_serial_missing, UTT_Serial_NR_List_Missing_with_Status
-- Produces:     Serial_Number_MLRS_NLSDB_Count_Differences_<snapshot_date>.xlsx, sheet "Serial Numbers"
--               (PART 1 = the list, PART 2 = the title-line counts). Sheet "Counts by Subgroup" comes from SYT30.
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   * Output is now the FLAT layout the delivered sheet uses -- one exception per row with a
--     `Filter` column ('In MLRS, not in NLSDB' / 'In NLSDB, not in MLRS') -- instead of the
--     10-column side-by-side layout that had to be reshaped by hand.
--   * PART 2 added: the per-Filter counts quoted on the title line.
--   * Missing serial = NULL or blank (NULLIF(TRIM)) on both sides. No blanks exist today.
--
-- Fixes carried from the 2026-08-11 correction (see SYT30 for detail):
--   1. NLSDB rows classified via their own CSE_TYPE_NR, not by joining back to blm_case on
--      serial (circular for exactly the "missing from MLRS" rows).
--   2. MLRS population excludes Status AND Bond by RECORDTYPEID (S1).
--   3. Taxonomy joins SAFE_CAST(... AS INT64) on both sides (S4), with the same unclassified
--      breakout as the companion.
--
-- Matched on SERIAL NUMBER only (blm_case.SERIAL_NUMBER__C = nlsdb_case.CSE_NR). A case in
-- both systems whose serial disagrees appears here on BOTH sides -- those are listed on their
-- own by Serial_NR_Mismatch_Same_Case.sql.
--
-- Edit only `snapshot_date`.
-- =====================================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE serial_exceptions AS
  WITH dedup_product AS (
    SELECT ID, CASE_TYPE_CODE FROM (
      SELECT ID, CASE_TYPE_CODE,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE, NAME) AS rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    ) WHERE rn = 1
  ),
  known_product_codes AS (
    SELECT DISTINCT CASE_TYPE_CODE FROM dedup_product WHERE CASE_TYPE_CODE IS NOT NULL
  ),
  taxonomy AS (
    SELECT DISTINCT SAFE_CAST(`BLM Product Code` AS INT64) AS product_code,
      `Case Type Group` AS case_type_group, `Case Type Subgroup` AS case_type_subgroup
    FROM `%s`
  ),
  mlrs AS (
    SELECT DISTINCT
      bc.ID, bc.SERIAL_NUMBER__C, bc.CASE_STATUS,
      IFNULL(tx.case_type_group, '(Unclassified)') AS case_type_group,
      CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
           WHEN dp.CASE_TYPE_CODE IS NOT NULL
             THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
           ELSE '(no case type code / not in blm_product)' END AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS bc
    LEFT JOIN dedup_product AS dp ON bc.BLM_PRODUCT = dp.ID
    LEFT JOIN taxonomy AS tx ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tx.product_code
    WHERE bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
      AND NULLIF(TRIM(bc.SERIAL_NUMBER__C), '') IS NOT NULL
  ),
  nlsdb AS (
    SELECT DISTINCT
      nc.SF_ID, nc.CSE_NR, nc.CSE_DISP,
      IFNULL(tx.case_type_group, '(Unclassified)') AS case_type_group,
      CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
           WHEN kpc.CASE_TYPE_CODE IS NOT NULL
             THEN CONCAT('BLM Product Code ', kpc.CASE_TYPE_CODE, ' (not in taxonomy)')
           ELSE '(no case type code / not in blm_product)' END AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` AS nc
    LEFT JOIN taxonomy AS tx ON SAFE_CAST(nc.CSE_TYPE_NR AS INT64) = tx.product_code
    LEFT JOIN known_product_codes AS kpc ON nc.CSE_TYPE_NR = kpc.CASE_TYPE_CODE
    WHERE NULLIF(TRIM(nc.CSE_NR), '') IS NOT NULL
  )
  SELECT 'In MLRS, not in NLSDB' AS Filter,
         mlrs.SERIAL_NUMBER__C AS Serial_Number, mlrs.ID AS Case_ID,
         mlrs.CASE_STATUS AS Disposition,
         mlrs.case_type_group AS Case_Type_Group, mlrs.case_type_subgroup AS Case_Type_Subgroup
  FROM mlrs LEFT JOIN nlsdb ON mlrs.SERIAL_NUMBER__C = nlsdb.CSE_NR
  WHERE nlsdb.CSE_NR IS NULL
  UNION ALL
  SELECT 'In NLSDB, not in MLRS',
         nlsdb.CSE_NR, nlsdb.SF_ID, nlsdb.CSE_DISP,
         nlsdb.case_type_group, nlsdb.case_type_subgroup
  FROM nlsdb LEFT JOIN mlrs ON mlrs.SERIAL_NUMBER__C = nlsdb.CSE_NR
  WHERE mlrs.SERIAL_NUMBER__C IS NULL
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);

-- PART 1 -- THE LIST
SELECT * FROM serial_exceptions
ORDER BY Filter, Case_Type_Group, Case_Type_Subgroup, Serial_Number;

-- PART 2 -- TITLE-LINE COUNTS
SELECT Filter, COUNT(*) AS Exceptions FROM serial_exceptions GROUP BY Filter
UNION ALL
SELECT 'TOTAL', COUNT(*) FROM serial_exceptions
ORDER BY Filter = 'TOTAL', Filter;
