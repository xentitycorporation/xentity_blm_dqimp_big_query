-- =====================================================================================
-- Test ID:      AT14.1
-- Test name:    Cases missing a case action (client list)
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #115 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Cases_Missing_CaseAction_<snapshot_date>.xlsx, sheet "Cases Missing Action"
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table, dispositions
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09; Case Action Link / Record Count columns added 2026-10-09 and re-run on 20261004
-- Added:        2026-10-13
-- =====================================================================================
-- Written 2026-09-14 at Mark's request (first delivered on 20260802 as
-- MLRS_Cases_Missing_CaseAction_09142026.xlsx). Companion to AT06.1 (case land) and AT13.1 (customer).
--
-- STANDARDISED 2026-10-09 to the shared conventions S1-S6 in tests/README_newTests.md:
--   * `snap` renamed `snapshot_date`; lookup table passed as `lookup_table`.
--   * Disposition still FILTERED on UPPER(TRIM(...)) but now DISPLAYED as stored (S2).
--   * Placeholders '(no serial)' / '(no product)' / '(blank)' removed -- missing values are
--     NULL (S5). Unclassified rows use the shared '(Unclassified)' labels (S4).
-- 2026-10-09 (Mark): Case_Action_Link and Case_Action_Record_Count added after Disposition --
--   the two shaded "relationship" columns the case-land and customer lists already carry, so
--   the sheet shows the missing action rather than asserting it.
--
-- WHAT IS LISTED
--   All real cases with no case action, restricted to dispositions Closed, Interim,
--   Authorized and Active: serial number, ID, case type group and subgroup, disposition,
--   BLM product and product code, admin state.
--
-- POPULATION
--   * Real cases only: RECORDTYPEID NOT IN Bond (0123d0000004QFQAA2) / Status
--     (0123d0000005ISFAA2). Same as STT06 / STT13 / STT14.
--   * "Missing a case action" = NOT EXISTS any case_action row for the case.
--     *** case_action MUST be read with EXISTS, never a join *** -- ~7.36 rows per case.
--     A join would multiply every case by its action count.
--
-- EXPECTED ON 20260802 -- ACTIVE 3, AUTHORIZED 1, INTERIM 0, CLOSED 10, TOTAL 14.
--   PART 2 emits a row for every requested disposition INCLUDING zero, so a missing
--   INTERIM row can never be mistaken for "not checked". PART 2 feeds the subtitle line.
--
-- FIELD NOTES
--   * Admin State is blm_case.BLM_ADMIN_STATE__C -- the 12 BLM state offices, Eastern
--     States as 'ES'.
--   * blm_product is duplicated per ID by the per-state extract and is
--     deduplicated before the join (S3).
--   * BLM_Product_Code is the zero-padded STRING -- keep it as text downstream.
--
-- Edit only `snapshot_date` and `dispositions`.
-- =====================================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';
DECLARE dispositions  ARRAY<STRING> DEFAULT ['ACTIVE', 'AUTHORIZED', 'INTERIM', 'CLOSED'];


-- =====================================================================================
-- PART 1 -- THE LIST. One row per case.
-- =====================================================================================
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
actionless AS (
  SELECT bc.*
  FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc
  WHERE bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
    AND UPPER(TRIM(bc.CASE_STATUS)) IN UNNEST(%s)
    AND NOT EXISTS (
      SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_action_%s` ca
      WHERE ca.BLM_CASE = bc.ID
    )
),
-- Counts per parent case. Used ONLY to render the link field and the row count on the
-- output, so the spreadsheet DEMONSTRATES the absence (the LEFT JOIN returns NULL / zero
-- because no child row exists) rather than asserting it -- same design as the case-land and
-- customer lists. Restricted to the listed cases so the GROUP BY stays cheap.
actions_n AS (
  SELECT ca.BLM_CASE, COUNT(*) AS n
  FROM `xentity-sandbox-huy.blm_seta_dqimp.case_action_%s` ca
  WHERE ca.BLM_CASE IN (SELECT ID FROM actionless)
  GROUP BY ca.BLM_CASE
)
SELECT
  a.SERIAL_NUMBER__C                                     AS Serial_Number,
  a.ID                                                   AS Case_ID,
  IFNULL(tx.case_type_group, '(Unclassified)')           AS Case_Type_Group,
  CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
       WHEN dp.CASE_TYPE_CODE IS NOT NULL
         THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
       ELSE '(no case type code / not in blm_product)' END AS Case_Type_Subgroup,
  a.CASE_STATUS                                          AS Disposition,
  -- THE FIELD THAT DEFINES THE RELATIONSHIP: case_action.BLM_CASE points at the case.
  -- Empty / zero on every row here, which is the point. Shown as "(empty)" in the sheet.
  an.BLM_CASE                                            AS Case_Action_Link,
  IFNULL(an.n, 0)                                        AS Case_Action_Record_Count,
  dp.NAME                                                AS BLM_Product,
  dp.CASE_TYPE_CODE                                      AS BLM_Product_Code,
  a.BLM_ADMIN_STATE__C                                   AS Admin_State
FROM actionless a
LEFT JOIN dedup_product dp ON dp.ID = a.BLM_PRODUCT
LEFT JOIN taxonomy      tx ON tx.product_code = SAFE_CAST(dp.CASE_TYPE_CODE AS INT64)
LEFT JOIN actions_n     an ON an.BLM_CASE = a.ID
ORDER BY
  CASE UPPER(TRIM(a.CASE_STATUS))
    WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'INTERIM' THEN 3 WHEN 'CLOSED' THEN 4 END,
  Admin_State, Case_Type_Group, Case_Type_Subgroup, Serial_Number
""",
  snapshot_date,                          -- blm_product
  lookup_table,
  snapshot_date,                          -- blm_case
  FORMAT('%T', dispositions),
  snapshot_date,                          -- case_action (NOT EXISTS)
  snapshot_date                           -- case_action (link / count columns)
);


-- =====================================================================================
-- PART 2 -- RECONCILIATION. One row per requested disposition, ALWAYS -- including zero --
--   plus a TOTAL. The quality columns count PART 1 rows with a NULL serial / admin state /
--   product, or an unclassified group.
-- =====================================================================================
EXECUTE IMMEDIATE FORMAT("""
WITH dedup_product AS (
  SELECT ID, CASE_TYPE_CODE FROM (
    SELECT ID, CASE_TYPE_CODE,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE, NAME) AS rn
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
  ) WHERE rn = 1
),
lookup_codes AS (
  SELECT DISTINCT SAFE_CAST(`BLM Product Code` AS INT64) AS product_code FROM `%s`
),
actionless AS (
  SELECT bc.ID, UPPER(TRIM(bc.CASE_STATUS)) AS disposition, bc.SERIAL_NUMBER__C,
         bc.BLM_ADMIN_STATE__C, bc.BLM_PRODUCT
  FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc
  WHERE bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
    AND UPPER(TRIM(bc.CASE_STATUS)) IN UNNEST(%s)
    AND NOT EXISTS (
      SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_action_%s` ca
      WHERE ca.BLM_CASE = bc.ID
    )
),
scored AS (
  SELECT a.disposition, a.ID,
         NULLIF(TRIM(a.SERIAL_NUMBER__C), '') IS NULL        AS no_serial,
         NULLIF(TRIM(a.BLM_ADMIN_STATE__C), '') IS NULL      AS no_admin_state,
         dp.ID IS NULL                                       AS no_product,
         lc.product_code IS NULL                             AS unclassified
  FROM actionless a
  LEFT JOIN dedup_product dp ON dp.ID = a.BLM_PRODUCT
  LEFT JOIN lookup_codes  lc ON lc.product_code = SAFE_CAST(dp.CASE_TYPE_CODE AS INT64)
),
per_disp AS (
  SELECT d AS Disposition,
         COUNT(s.ID)               AS Cases,
         COUNT(DISTINCT s.ID)      AS Distinct_Case_IDs,
         COUNTIF(s.no_serial)      AS No_Serial,
         COUNTIF(s.no_admin_state) AS No_Admin_State,
         COUNTIF(s.no_product)     AS No_Product,
         COUNTIF(s.unclassified)   AS Unclassified
  FROM UNNEST(%s) AS d
  LEFT JOIN scored s ON s.disposition = d
  GROUP BY d
)
SELECT * FROM per_disp
UNION ALL
SELECT 'TOTAL', SUM(Cases), SUM(Distinct_Case_IDs), SUM(No_Serial), SUM(No_Admin_State),
       SUM(No_Product), SUM(Unclassified)
FROM per_disp
ORDER BY CASE Disposition WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2
                          WHEN 'INTERIM' THEN 3 WHEN 'CLOSED' THEN 4 ELSE 5 END
""",
  snapshot_date, lookup_table, snapshot_date, FORMAT('%T', dispositions), snapshot_date,
  FORMAT('%T', dispositions)
);
