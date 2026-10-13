-- =====================================================================================
-- Test ID:      AT20
-- Test name:    Null serial at PENDING or '-'
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #119 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     STT_null_by_disp
-- Produces:     MLRS_Null_Serial_Pending_Dispositions_<snapshot_date>.xlsx, sheet "Null Serial - Flagged"
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table, flagged_dispositions
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
--
-- Created 2026-08-17, superseding an earlier SUBMITTED-based query (not in the repo). The client's rule
-- INVERTED: SUBMITTED sits early in the pipeline and is fine, while PENDING without a serial
-- IS a problem (open client question). The flagged dispositions are a parameter
-- because this rule has already changed once -- edit the array, not the query body.
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   S1 Real cases only -- Bond and Status record types now excluded (previously not). On
--      20260802 none of the 5 flagged cases was Bond or Status, so the list is unchanged.
--   S2 Disposition filtered on UPPER(TRIM(...)) (previously an exact string match).
--   S4 Unclassified rows labelled '(Unclassified)' (previously blank).
--   * The `Finding` column is removed -- the delivered sheet never carried it, and with the
--     disposition filter every row was 'RULE VIOLATION' or 'ANOMALY' by construction.
--   * "No serial" = NULL or blank (NULLIF(TRIM)). On 20260802 and 20261004 every missing serial
--     is NULL; the blank guard keeps that true if a blank ever arrives.
--
-- Serial_Number is in the output purely to show it is empty -- it is the filter condition.
--
-- Edit only `snapshot_date` and `flagged_dispositions`.
-- =========================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';
DECLARE flagged_dispositions ARRAY<STRING> DEFAULT ['PENDING', '-'];

EXECUTE IMMEDIATE FORMAT("""
WITH dedup_product AS (
  SELECT ID, CASE_TYPE_CODE, NAME FROM (
    SELECT ID, CASE_TYPE_CODE, NAME,
      ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE, NAME) AS rn
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
  ) WHERE rn = 1
),
taxonomy AS (
  SELECT DISTINCT SAFE_CAST(`BLM Product Code` AS INT64) AS product_code,
    `Case Type Group` AS case_type_group, `Case Type Subgroup` AS case_type_subgroup
  FROM `%s`
)
SELECT
  b.ID                                             AS ID,
  IFNULL(tx.case_type_group, '(Unclassified)')     AS Case_Type_Group,
  CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
       WHEN dp.CASE_TYPE_CODE IS NOT NULL
         THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
       ELSE '(no case type code / not in blm_product)' END AS Case_Type_Subgroup,
  b.CASE_STATUS                                    AS Disposition,
  dp.NAME                                          AS BLM_Product,
  dp.CASE_TYPE_CODE                                AS BLM_Product_Code,
  b.SERIAL_NUMBER__C                               AS Serial_Number,
  b.CREATEDDATE                                    AS CREATEDDATE
FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
LEFT JOIN dedup_product dp ON b.BLM_PRODUCT = dp.ID
LEFT JOIN taxonomy      tx ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tx.product_code
WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
  AND NULLIF(TRIM(b.SERIAL_NUMBER__C), '') IS NULL
  AND UPPER(TRIM(b.CASE_STATUS)) IN UNNEST(%s)
ORDER BY Disposition, BLM_Product_Code, ID
""", snapshot_date, lookup_table, snapshot_date, FORMAT('%T', flagged_dispositions));
