-- =====================================================================================
-- Test ID:      SYT04.1
-- Test name:    Case disposition mismatch (client deliverable)
-- Test type:    Synchronization (tests/sync/) - a case in MLRS is also in NLSDB (and vice versa) and carries the same value in both
-- Catalog:      Test Catalog v2 row #108 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     SVT_CaseDisposition_MLRS_NLSDB_Mismatch
-- Produces:     CaseDisposition_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx, sheet "Case Disposition"
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- MLRS field:  blm_case.CASE_STATUS      NLSDB field: nlsdb_case.CSE_DISP
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   S1 Real MLRS cases only -- Bond and Status record types now excluded (previously not).
--      On 20260802 two Status records joined to NLSDB, so at most 2 rows can change.
--   S3 blm_product dedup tie-broken on NAME so it is deterministic.
--   S4 Unclassified rows labelled '(Unclassified)' (previously blank).
--
-- UPDATED 2026-08-17: lookup repointed to Product_Code_Case_Type_Group_Subgroup; taxonomy
--   join SAFE_CAST(... AS INT64) both sides; NLSDB serial and SF_ID added.
--   Dead table names and superseded SQL forms are DESCRIBED here, never
--   reproduced verbatim -- quoting them makes every future grep for a defect false-positive.
--
-- THE CASE/WHITESPACE NORMALISATION IS LOAD-BEARING. MLRS stores 'AUTHORIZED', NLSDB
--   'Authorized'. Without LOWER(TRIM()) on both sides this returns ~5.4 million rows --
--   essentially every matched case. Values are DISPLAYED as each system stores them.
--
-- Edit only `snapshot_date`.
-- =========================================================================

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
  taxonomy AS (
    SELECT DISTINCT SAFE_CAST(`BLM Product Code` AS INT64) AS product_code,
      `Case Type Group` AS case_type_group, `Case Type Subgroup` AS case_type_subgroup
    FROM `%s`
  )
  SELECT
    IFNULL(tx.case_type_group, '(Unclassified)') AS Case_Type_Group,
    CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
         WHEN dp.CASE_TYPE_CODE IS NOT NULL
           THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
         ELSE '(no case type code / not in blm_product)' END AS Case_Type_Subgroup,
    b.SERIAL_NUMBER__C      AS MLRS_Serial_Number,
    n.CSE_NR                AS NLSDB_Serial_Number,
    b.ID                    AS MLRS_ID,
    n.SF_ID                 AS NLSDB_SF_ID,
    b.CASE_STATUS           AS MLRS_Case_Disposition,
    n.CSE_DISP              AS NLSDB_Case_Disposition
  FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
  JOIN `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` n
    ON b.ID = n.SF_ID
  LEFT JOIN dedup_product dp ON b.BLM_PRODUCT = dp.ID
  LEFT JOIN taxonomy tx ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tx.product_code
  WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
    AND LOWER(IFNULL(TRIM(b.CASE_STATUS), ''))
        IS DISTINCT FROM
        LOWER(IFNULL(TRIM(n.CSE_DISP), ''))
  ORDER BY Case_Type_Group, Case_Type_Subgroup, MLRS_Serial_Number
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);
