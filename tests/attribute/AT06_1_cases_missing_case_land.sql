-- =====================================================================================
-- Test ID:      AT06.1
-- Test name:    Cases missing case land (client list)
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #112 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     STT06_clientlist
-- Produces:     MLRS_Cases_Missing_Case_Lands_<snapshot_date>.xlsx, TWO sheets:
--                 "ACTIVE or AUTHORIZED"    Disposition_Bucket = 'ACTIVE_OR_AUTHORIZED'
--                 "All Other Dispositions"  Disposition_Bucket = 'OTHER'
--               Disposition_Bucket drives the split (it is not written to the sheets).
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- Client-requested column set (2026-08-24): ID, Case Type Group / Subgroup, disposition,
-- BLM product + product code, serial number, CREATEDDATE, each header naming its source field.
-- First delivered on 20260802 as MLRS_Cases_Missing_Case_Lands_20260818.xlsx.
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   * `snap` renamed `snapshot_date`; lookup table passed as `lookup_table`; tables
--     fully qualified (S6).
--   * blm_product deduplicated with ROW_NUMBER instead of ANY_VALUE (S3) -- ANY_VALUE was not
--     deterministic.
--   * Unclassified rows labelled '(Unclassified)' with the code breakout, not 'UNCLASSIFIED' (S4).
--
-- WHY THE SPLIT MATTERS
--   A landless case in DRAFT or PENDING is the intake pipeline caught mid-flight - the
--   client's serialization rule says a case is not complete until a customer,
--   a case land and an action exist. A landless ACTIVE or AUTHORIZED case is a live, in-force
--   authorisation with no land attached. Sheet 1 is the actionable population. Mining Claims
--   use ACTIVE where other case types use AUTHORIZED - both are in Sheet 1 deliberately.
--
-- POPULATION
--   Real cases only (S1): excludes Bond (0123d0000004QFQAA2) - structurally landless BY
--   DESIGN - and Status (0123d0000005ISFAA2). Matches STT06 exactly.
--
-- CASE_LAND DEFENSIVE FILTER
--   case_land carries ~12,887 rows with a null/blank BLM_CASE - orphan land records. They cannot create a false "missing" (a null never matches) but are filtered out
--   so this query states the table honestly.
--
-- THE TWO LINK COLUMNS
--   Case_Land_Link / Case_Land_Record_Count come from a LEFT JOIN to the child table, so the
--   spreadsheet DEMONSTRATES the absence (NULL / 0) rather than asserting it. NULL is shown
--   as "(empty)" in the delivered sheet.
--
-- CREATEDDATE is RAW, not parsed (MM/DD/YYYY HH:MM:SS) -- parsing would drop the time and hide
--   the multi-format problem in the source. Beware the 1999 sentinel dates (millions of cases share a few days in March 1999).
--
-- Edit only `snapshot_date`.
-- =====================================================================================
DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

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
),
lands_n AS (
  SELECT BLM_CASE, COUNT(*) AS n
  FROM `xentity-sandbox-huy.blm_seta_dqimp.case_land_%s`
  WHERE BLM_CASE IS NOT NULL AND TRIM(BLM_CASE) <> ''
  GROUP BY BLM_CASE
)
SELECT
  b.ID                                              AS ID,
  IFNULL(tx.case_type_group, '(Unclassified)')      AS Case_Type_Group,
  CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
       WHEN dp.CASE_TYPE_CODE IS NOT NULL
         THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
       ELSE '(no case type code / not in blm_product)' END AS Case_Type_Subgroup,
  b.CASE_STATUS                                     AS Disposition,
  cl.BLM_CASE                                       AS Case_Land_Link,
  IFNULL(cl.n, 0)                                   AS Case_Land_Record_Count,
  dp.NAME                                           AS BLM_Product,
  dp.CASE_TYPE_CODE                                 AS BLM_Product_Code,
  b.SERIAL_NUMBER__C                                AS Serial_Number,
  b.CREATEDDATE                                     AS CREATEDDATE,
  IF(UPPER(IFNULL(TRIM(b.CASE_STATUS), '')) IN ('ACTIVE', 'AUTHORIZED'),
     'ACTIVE_OR_AUTHORIZED', 'OTHER')               AS Disposition_Bucket
FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
LEFT JOIN dedup_product dp ON b.BLM_PRODUCT = dp.ID
LEFT JOIN taxonomy      tx ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tx.product_code
LEFT JOIN lands_n       cl ON cl.BLM_CASE = b.ID
WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
  AND cl.BLM_CASE IS NULL
ORDER BY Disposition_Bucket, Case_Type_Group, Case_Type_Subgroup, Serial_Number
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);
