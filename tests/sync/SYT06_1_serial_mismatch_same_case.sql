-- =====================================================================================
-- Test ID:      SYT06.1
-- Test name:    Same case, different serial
-- Test type:    Synchronization (tests/sync/) - a case in MLRS is also in NLSDB (and vice versa) and carries the same value in both
-- Catalog:      Test Catalog v2 row #118 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     STT_serial_mismatch, SVT_MLRS_NLSDB_SerialNumber_Mismatch
-- Produces:     Serial_Number_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx, sheet "Serial Mismatch"
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- Reusable monthly test. Lists cases that genuinely exist in BOTH MLRS and NLSDB (matched
-- reliably via blm_case.ID = nlsdb_case.SF_ID) but whose serial number disagrees
-- (blm_case.SERIAL_NUMBER__C != nlsdb_case.CSE_NR). These are the cases that show up as
-- false-positive "missing" rows in the serial-only comparison
-- (SYT30.1) although neither system is missing them.
--
-- On 20260705 and 20260802 this returned the same 4 cases
-- (NV105221428/AZAZ106311751, UTUT106240137/UTUT106791619, NDMT105485224/NDMT105823539,
-- WYWY106344735/WYWY106718415). Re-running monthly shows whether the set stays fixed or grows.
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   S1 Real MLRS cases only -- Bond and Status record types now excluded (previously not).
--   S3 blm_product dedup tie-broken on NAME.
--   S4 Unclassified rows labelled '(Unclassified)' (previously blank) on both sides; the
--      NLSDB side is classified from its own CSE_TYPE_NR, as in the other serial queries.
--   * Output columns renamed to the spreadsheet headers.
--
-- Edit only `snapshot_date`.
-- =====================================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

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
  )
  SELECT
    b.SERIAL_NUMBER__C                               AS MLRS_Serial_Number,
    b.ID                                             AS MLRS_ID,
    b.CASE_STATUS                                    AS MLRS_Disposition,
    IFNULL(tm.case_type_group, '(Unclassified)')     AS MLRS_Case_Type_Group,
    CASE WHEN tm.case_type_group IS NOT NULL THEN tm.case_type_subgroup
         WHEN dp.CASE_TYPE_CODE IS NOT NULL
           THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
         ELSE '(no case type code / not in blm_product)' END AS MLRS_Case_Type_Subgroup,
    n.CSE_NR                                         AS NLSDB_Serial_Number,
    n.SF_ID                                          AS NLSDB_ID,
    n.CSE_DISP                                       AS NLSDB_Disposition,
    IFNULL(tn.case_type_group, '(Unclassified)')     AS NLSDB_Case_Type_Group,
    CASE WHEN tn.case_type_group IS NOT NULL THEN tn.case_type_subgroup
         WHEN kpc.CASE_TYPE_CODE IS NOT NULL
           THEN CONCAT('BLM Product Code ', kpc.CASE_TYPE_CODE, ' (not in taxonomy)')
         ELSE '(no case type code / not in blm_product)' END AS NLSDB_Case_Type_Subgroup
  FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS b
  JOIN `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` AS n
    ON b.ID = n.SF_ID
  LEFT JOIN dedup_product AS dp ON b.BLM_PRODUCT = dp.ID
  LEFT JOIN taxonomy AS tm ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tm.product_code
  LEFT JOIN taxonomy AS tn ON SAFE_CAST(n.CSE_TYPE_NR AS INT64) = tn.product_code
  LEFT JOIN known_product_codes AS kpc ON n.CSE_TYPE_NR = kpc.CASE_TYPE_CODE
  WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
    AND NULLIF(TRIM(b.SERIAL_NUMBER__C), '') IS NOT NULL
    AND NULLIF(TRIM(n.CSE_NR), '') IS NOT NULL
    AND b.SERIAL_NUMBER__C != n.CSE_NR
  ORDER BY MLRS_Serial_Number
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);
