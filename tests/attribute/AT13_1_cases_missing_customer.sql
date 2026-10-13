-- =====================================================================================
-- Test ID:      AT13.1
-- Test name:    Cases missing a customer (client list)
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #113 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     STT13_clientlist
-- Produces:     MLRS_Cases_Missing_Customer_<snapshot_date>.xlsx, TWO sheets:
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
-- First delivered on 20260802 as MLRS_Cases_Missing_Customer_20260818.xlsx. Structurally
-- identical to AT06.1, so the two lists can be diffed.
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   * `snap` renamed `snapshot_date`; lookup table passed as `lookup_table`; tables
--     fully qualified (S6).
--   * blm_product deduplicated with ROW_NUMBER instead of ANY_VALUE (S3) -- ANY_VALUE was not
--     deterministic.
--   * Unclassified rows labelled '(Unclassified)' with the code breakout, not 'UNCLASSIFIED' (S4).
--
-- WHY THE SPLIT MATTERS
--   A customerless case in DRAFT or PENDING is the intake pipeline caught mid-flight; a
--   customerless ACTIVE or AUTHORIZED case is a live authorisation with nobody attached.
--   Expect Sheet 1 small and Sheet 2 dominant (75 of 789 on 20260802).
--
-- POPULATION
--   Real cases only (S1): excludes Bond (0123d0000004QFQAA2) and Status (0123d0000005ISFAA2).
--   Matches STT13 exactly.
--   !! THE BOND EXCLUSION IS WEAKER HERE THAN FOR CASE LANDS. A bond is posted BY a customer,
--   so a Bond case plausibly SHOULD have one; only ~8 of ~14,053 Bond cases lack one. Bond is excluded so all the lists share one population. To review Bond, drop
--   0123d0000004QFQAA2 from the NOT IN - it adds ~8 rows.
--
-- case_customer was clean on 20260802 (zero null/blank BLM_CASE, zero orphans);
-- the null guard below is belt-and-braces, kept so the two client lists stay identical.
--
-- THE TWO LINK COLUMNS come from a LEFT JOIN to the child table, so the spreadsheet
-- DEMONSTRATES the absence (NULL / 0). NULL is shown as "(empty)" in the delivered sheet.
-- CREATEDDATE is RAW, not parsed. Beware the 1999 sentinel dates (millions of cases share a few days in March 1999).
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
custs_n AS (
  SELECT BLM_CASE, COUNT(*) AS n
  FROM `xentity-sandbox-huy.blm_seta_dqimp.case_customer_%s`
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
  cc.BLM_CASE                                       AS Customer_Link,
  IFNULL(cc.n, 0)                                   AS Customer_Record_Count,
  dp.NAME                                           AS BLM_Product,
  dp.CASE_TYPE_CODE                                 AS BLM_Product_Code,
  b.SERIAL_NUMBER__C                                AS Serial_Number,
  b.CREATEDDATE                                     AS CREATEDDATE,
  IF(UPPER(IFNULL(TRIM(b.CASE_STATUS), '')) IN ('ACTIVE', 'AUTHORIZED'),
     'ACTIVE_OR_AUTHORIZED', 'OTHER')               AS Disposition_Bucket
FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
LEFT JOIN dedup_product dp ON b.BLM_PRODUCT = dp.ID
LEFT JOIN taxonomy      tx ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = tx.product_code
LEFT JOIN custs_n       cc ON cc.BLM_CASE = b.ID
WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
  AND cc.BLM_CASE IS NULL
ORDER BY Disposition_Bucket, Case_Type_Group, Case_Type_Subgroup, Serial_Number
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);
