-- =====================================================================================
-- Test ID:      AT20.1
-- Test name:    Null serial at ACTIVE, AUTHORIZED, CLOSED or INTERIM
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #120 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Null_Serial_Active_Authorized_Closed_Interim_<snapshot_date>.xlsx, sheet "Null Serial - Flagged"
--               (not yet delivered: 0 cases on 20261004)
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table, flagged_dispositions
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    run on 20261004 on 2026-10-09: 0 cases, confirmed independently (all 3,151 real cases without a serial are DRAFT or SUBMITTED)
-- Added:        2026-10-13
-- =====================================================================================
--
-- Written 2026-10-09 at Mark's request. Companion to AT20 (tests/attribute/AT20_null_serial_pending.sql)
-- (PENDING and '-'): identical logic and columns, plus Admin State as the last column.
--
-- WHY THESE DISPOSITIONS: a case is serialized once it has a customer, a case land and an
-- action and has been paid for. A case that is ACTIVE or AUTHORIZED -- a live,
-- in-force authorisation -- with no serial is the top of the client's severity ladder.
-- CLOSED and INTERIM cases have also moved past intake, so a missing serial there is
-- unexpected too.
--
-- Follows the shared conventions S1-S6 in tests/README_newTests.md:
--   S1 Real cases only -- Bond (0123d0000004QFQAA2) and Status (0123d0000005ISFAA2) excluded.
--   S2 Disposition filtered on UPPER(TRIM(...)), displayed as stored.
--   S3 blm_product deduplicated with ROW_NUMBER, tie-broken on NAME.
--   S4 Taxonomy joined on SAFE_CAST(... AS INT64) both sides; unclassified rows labelled
--      '(Unclassified)' with the code breakout.
--   * "No serial" = NULL or blank (NULLIF(TRIM)).
--
-- PART 2 gives the per-disposition counts (every requested disposition, including zero).
--
-- Edit only `snapshot_date` and `flagged_dispositions`.
-- =========================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';
DECLARE flagged_dispositions ARRAY<STRING> DEFAULT ['ACTIVE', 'AUTHORIZED', 'CLOSED', 'INTERIM'];

-- PART 1 -- THE LIST
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
  b.CREATEDDATE                                    AS CREATEDDATE,
  b.BLM_ADMIN_STATE__C                             AS Admin_State
FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
LEFT JOIN dedup_product dp ON b.BLM_PRODUCT = dp.ID
LEFT JOIN taxonomy      tx ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tx.product_code
WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
  AND NULLIF(TRIM(b.SERIAL_NUMBER__C), '') IS NULL
  AND UPPER(TRIM(b.CASE_STATUS)) IN UNNEST(%s)
ORDER BY
  CASE UPPER(TRIM(b.CASE_STATUS))
    WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'CLOSED' THEN 3 WHEN 'INTERIM' THEN 4 END,
  Admin_State, BLM_Product_Code, ID
""", snapshot_date, lookup_table, snapshot_date, FORMAT('%T', flagged_dispositions));

-- PART 2 -- COUNTS PER DISPOSITION (every requested disposition, including zero)
EXECUTE IMMEDIATE FORMAT("""
WITH flagged AS (
  SELECT UPPER(TRIM(b.CASE_STATUS)) AS disposition
  FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
  WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
    AND NULLIF(TRIM(b.SERIAL_NUMBER__C), '') IS NULL
    AND UPPER(TRIM(b.CASE_STATUS)) IN UNNEST(%s)
)
SELECT d AS Disposition, COUNT(f.disposition) AS Cases
FROM UNNEST(%s) AS d WITH OFFSET AS o
LEFT JOIN flagged f ON f.disposition = d
GROUP BY d, o
ORDER BY o
""", snapshot_date, FORMAT('%T', flagged_dispositions), FORMAT('%T', flagged_dispositions));
