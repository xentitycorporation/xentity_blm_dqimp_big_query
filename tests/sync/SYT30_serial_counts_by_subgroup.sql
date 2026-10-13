-- =====================================================================================
-- Test ID:      SYT30
-- Test name:    Serial counts by subgroup
-- Test type:    Synchronization (tests/sync/) - a case in MLRS is also in NLSDB (and vice versa) and carries the same value in both
-- Catalog:      Test Catalog v2 row #116 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     STT_serial_rollup
-- Produces:     Serial_Number_MLRS_NLSDB_Count_Differences_<snapshot_date>.xlsx, sheet "Counts by Subgroup"
--               (PART 1 = the table, PART 2 = the note-line numbers). Sheet "Serial Numbers" comes from SYT30.1.
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   * PART 1 now ends with the 'Total' row the delivered sheet carries (sum of the rows above,
--     which on 20260802 = 5,445,142 MLRS / 5,433,954 NLSDB).
--   * PART 2 added: the numbers quoted in the sheet's note line about the '(Unclassified)' row
--     (NLSDB records with no blm_product row and how many codes, plus NULL and blank codes).
--   * Missing serial = NULL or blank (NULLIF(TRIM)) on both sides. No blanks exist today.
--
-- Fixes carried from the 2026-08-11 correction of the original:
--   1. NLSDB rows are classified via their OWN CSE_TYPE_NR, not by joining back to blm_case on
--      serial (circular for exactly the "missing from MLRS" rows).
--   2. MLRS population excludes Status AND Bond by RECORDTYPEID (S1). Bond cases don't exist
--      in NLSDB by design and leaked through as ~14,053 false "missing from NLSDB" rows.
--   3. Taxonomy joins SAFE_CAST(... AS INT64) on BOTH sides (S4).
--   Unclassified breakout: a code that exists in blm_product but has no taxonomy row gets its
--   own row ('BLM Product Code <code> (not in taxonomy)'); everything else unclassifiable
--   (NLSDB-only codes, NULL/blank codes) stays on '(no case type code / not in blm_product)'.
--
-- Counts are COUNT(DISTINCT serial). Cases with no serial are outside this test (3,071 real
-- MLRS cases on 20260802) -- see SYT_NullSerial_By_Disposition.sql.
--
-- Edit only `snapshot_date`.
-- =====================================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

-- PART 1 -- THE TABLE, with a Total row last.
EXECUTE IMMEDIATE FORMAT("""
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
  mlrs_classified AS (
    SELECT
      bc.SERIAL_NUMBER__C,
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
  nlsdb_classified AS (
    SELECT
      nc.CSE_NR,
      IFNULL(tx.case_type_group, '(Unclassified)') AS case_type_group,
      CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
           WHEN kpc.CASE_TYPE_CODE IS NOT NULL
             THEN CONCAT('BLM Product Code ', kpc.CASE_TYPE_CODE, ' (not in taxonomy)')
           ELSE '(no case type code / not in blm_product)' END AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` AS nc
    LEFT JOIN taxonomy AS tx ON SAFE_CAST(nc.CSE_TYPE_NR AS INT64) = tx.product_code
    LEFT JOIN known_product_codes AS kpc ON nc.CSE_TYPE_NR = kpc.CASE_TYPE_CODE
    WHERE NULLIF(TRIM(nc.CSE_NR), '') IS NOT NULL
  ),
  mlrs_counts AS (
    SELECT case_type_group, case_type_subgroup, COUNT(DISTINCT SERIAL_NUMBER__C) AS mlrs_count
    FROM mlrs_classified GROUP BY 1, 2
  ),
  nlsdb_counts AS (
    SELECT case_type_group, case_type_subgroup, COUNT(DISTINCT CSE_NR) AS nlsdb_count
    FROM nlsdb_classified GROUP BY 1, 2
  ),
  rows_ AS (
    SELECT
      COALESCE(m.case_type_group, n.case_type_group)       AS Case_Type_Group,
      COALESCE(m.case_type_subgroup, n.case_type_subgroup) AS Case_Type_Subgroup,
      IFNULL(m.mlrs_count, 0)                              AS MLRS_Count,
      IFNULL(n.nlsdb_count, 0)                             AS NLSDB_Count,
      IFNULL(m.mlrs_count, 0) - IFNULL(n.nlsdb_count, 0)   AS Difference
    FROM mlrs_counts m
    FULL OUTER JOIN nlsdb_counts n
      ON IFNULL(m.case_type_group, '~NULL~')    = IFNULL(n.case_type_group, '~NULL~')
     AND IFNULL(m.case_type_subgroup, '~NULL~') = IFNULL(n.case_type_subgroup, '~NULL~')
  )
  SELECT * EXCEPT (sort_key) FROM (
    SELECT 0 AS sort_key, * FROM rows_
    UNION ALL
    SELECT 1, CAST(NULL AS STRING), 'Total', SUM(MLRS_Count), SUM(NLSDB_Count), SUM(Difference)
    FROM rows_
  )
  ORDER BY sort_key, Case_Type_Group, Case_Type_Subgroup
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);

-- PART 2 -- NOTE-LINE NUMBERS for the '(Unclassified)' / '(no case type code / not in
-- blm_product)' NLSDB row. Distinct serials, so the three buckets sum to that row's NLSDB count.
EXECUTE IMMEDIATE FORMAT("""
  WITH known_product_codes AS (
    SELECT DISTINCT CASE_TYPE_CODE FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IS NOT NULL
  ),
  taxonomy_codes AS (
    SELECT DISTINCT SAFE_CAST(`BLM Product Code` AS INT64) AS product_code FROM `%s`
  ),
  bucketed AS (
    SELECT nc.CSE_NR, nc.CSE_TYPE_NR,
      CASE WHEN nc.CSE_TYPE_NR IS NULL THEN 'null_code'
           WHEN TRIM(nc.CSE_TYPE_NR) = '' THEN 'blank_code'
           ELSE 'no_blm_product' END AS bucket
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` nc
    LEFT JOIN taxonomy_codes tc ON SAFE_CAST(nc.CSE_TYPE_NR AS INT64) = tc.product_code
    LEFT JOIN known_product_codes kpc ON nc.CSE_TYPE_NR = kpc.CASE_TYPE_CODE
    WHERE NULLIF(TRIM(nc.CSE_NR), '') IS NOT NULL
      AND tc.product_code IS NULL AND kpc.CASE_TYPE_CODE IS NULL
  )
  SELECT
    COUNT(DISTINCT IF(bucket = 'no_blm_product', CSE_NR, NULL))      AS No_BLM_Product_Serials,
    COUNT(DISTINCT IF(bucket = 'no_blm_product', CSE_TYPE_NR, NULL)) AS No_BLM_Product_Codes,
    COUNT(DISTINCT IF(bucket = 'null_code', CSE_NR, NULL))           AS Null_Code_Serials,
    COUNT(DISTINCT IF(bucket = 'blank_code', CSE_NR, NULL))          AS Blank_Code_Serials,
    COUNT(DISTINCT CSE_NR)                                           AS Unclassified_Row_Total
  FROM bucketed
""", snapshot_date, lookup_table, snapshot_date);
