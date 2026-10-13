-- =====================================================================================
-- Test ID:      STT13.6
-- Test name:    Cases that lost all their customers
-- Test type:    Serial Trace (tests/serial_trace/) - a case's attributes tracked over time across monthly snapshots
-- Catalog:      Test Catalog v2 row #114 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Cases_Lost_Customer_<latest snapshot>.xlsx, sheet "Cases That Lost Customer(s)"
-- Parameters:   snapshots (ascending YYYYMMDD array, last = latest), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260104..20260802 reproduced the delivered spreadsheet value for value; run on 20260104..20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- Written 2026-09-14 at Mark's request; first delivered on 20260104..20260802 as
-- MLRS_Cases_Lost_Customer_09142026.xlsx. (The original header said it produced the
-- "Cases Missing Customer" sheet -- wrong; that sheet came from a different query.)
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   * Window extended to January..October 2026 (Mark, 2026-10-09). `snaps` renamed `snapshots`.
--   * Disposition DISPLAYED as stored, not upper-cased (S2).
--   * Placeholders '(no serial)' / '(no product)' / '(blank)' removed -- NULL instead (S5).
--   * Unclassified rows use the shared '(Unclassified)' labels (S4); dedup tie-broken (S3).
--
-- WHAT IS LISTED
--   A real case with ZERO customer records in the latest snapshot that held at least one
--   customer record in an earlier snapshot in the window. This is the definitive tier
--   (Mark, 2026-09-14): a case that ENDS at zero is a true data-quality problem.
--   Cases that dropped to zero and later regained a customer are NOT listed; they are kept in
--   a separate verification script (not in the repo).
--   No disposition filter. On 20260104..20260802 there was one case: AZ101872216, ACTIVE.
--
-- POPULATION
--   * Real cases only, judged on the LATEST snapshot (S1): Bond / Status excluded.
--   * "Zero customers" = NOT EXISTS any case_customer row for the case.
--
-- THE TWO MONTH COLUMNS
--   Last_Month_With_Customers = the latest earlier snapshot in which the case held >= 1
--     customer record. First_Month_With_Zero = the first snapshot after that one in which the
--     case exists with zero. The loss happened between the two; the data cannot date it more
--     closely. Month NAMES only -- correct while the window stays inside one calendar year.
--   20260202 is a public-extract month; it stays in the window because this test
--   only asks whether a customer record existed for the case that month.
--
-- PART 2 reconciles: Lost_All_Customers must equal PART 1's row count.
--
-- Edit only `snapshots`. Dates must stay ASCENDING; the LAST one is the "latest snapshot".
-- =====================================================================================

DECLARE snapshots ARRAY<STRING> DEFAULT [
  '20260104','20260202','20260303','20260405','20260503','20260607','20260705','20260802',
  '20260906','20261004'];
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';
DECLARE latest_ix INT64 DEFAULT ARRAY_LENGTH(snapshots) - 1;
DECLARE latest STRING DEFAULT snapshots[OFFSET(ARRAY_LENGTH(snapshots) - 1)];
DECLARE i INT64 DEFAULT 0;

-- Real cases with no customer in the latest snapshot.
EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE zero_now AS
  SELECT bc.ID AS CaseID
  FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc
  WHERE bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
    AND NOT EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_customer_%s` cc
                    WHERE cc.BLM_CASE = bc.ID)
""", latest, latest);

-- Their customer-record count in every snapshot, and whether the case existed that month.
CREATE TEMP TABLE hist (ix INT64, CaseID STRING, case_exists BOOL, n_cust INT64);
WHILE i < ARRAY_LENGTH(snapshots) DO
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO hist
    SELECT %d, z.CaseID, bc.ID IS NOT NULL, IFNULL(n.k, 0)
    FROM zero_now z
    LEFT JOIN `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc ON bc.ID = z.CaseID
    LEFT JOIN (SELECT BLM_CASE, COUNT(*) AS k
               FROM `xentity-sandbox-huy.blm_seta_dqimp.case_customer_%s`
               WHERE BLM_CASE IN (SELECT CaseID FROM zero_now)
               GROUP BY BLM_CASE) n ON n.BLM_CASE = z.CaseID
  """, i, snapshots[OFFSET(i)], snapshots[OFFSET(i)]);
  SET i = i + 1;
END WHILE;

-- Cases that held a customer earlier in the window, with the two boundary snapshots.
CREATE TEMP TABLE lost AS
WITH last_with AS (
  SELECT CaseID, MAX(ix) AS last_with_ix
  FROM hist WHERE n_cust > 0 AND ix < latest_ix
  GROUP BY CaseID
)
SELECT lw.CaseID, lw.last_with_ix,
       (SELECT MIN(h.ix) FROM hist h
        WHERE h.CaseID = lw.CaseID AND h.ix > lw.last_with_ix AND h.case_exists AND h.n_cust = 0) AS first_zero_ix
FROM last_with lw;


-- =====================================================================================
-- PART 1 -- THE LIST.
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
month_names AS (
  SELECT ix, FORMAT_DATE('%%B', PARSE_DATE('%%Y%%m%%d', s)) AS month_name
  FROM UNNEST(%T) AS s WITH OFFSET AS ix
)
SELECT
  b.SERIAL_NUMBER__C                                     AS Serial_Number,
  b.ID                                                   AS Case_ID,
  b.CASE_NAME__C                                         AS Case_Name,
  IFNULL(tx.case_type_group, '(Unclassified)')           AS Case_Type_Group,
  CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
       WHEN dp.CASE_TYPE_CODE IS NOT NULL
         THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
       ELSE '(no case type code / not in blm_product)' END AS Case_Type_Subgroup,
  b.CASE_STATUS                                          AS Disposition,
  dp.NAME                                                AS BLM_Product,
  dp.CASE_TYPE_CODE                                      AS BLM_Product_Code,
  b.BLM_ADMIN_STATE__C                                   AS Admin_State,
  mw.month_name                                          AS Last_Month_With_Customers,
  mz.month_name                                          AS First_Month_With_Zero
FROM lost l
JOIN `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b ON b.ID = l.CaseID
LEFT JOIN dedup_product dp ON dp.ID = b.BLM_PRODUCT
LEFT JOIN taxonomy      tx ON tx.product_code = SAFE_CAST(dp.CASE_TYPE_CODE AS INT64)
LEFT JOIN month_names   mw ON mw.ix = l.last_with_ix
LEFT JOIN month_names   mz ON mz.ix = l.first_zero_ix
ORDER BY
  CASE UPPER(TRIM(b.CASE_STATUS))
    WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'INTERIM' THEN 3 WHEN 'CLOSED' THEN 4 ELSE 5 END,
  Admin_State, Serial_Number
""", latest, lookup_table, snapshots, latest);


-- =====================================================================================
-- PART 2 -- RECONCILIATION. How the list sits inside the wider customerless population.
-- =====================================================================================
EXECUTE IMMEDIATE FORMAT("""
SELECT
  '%s'                                                          AS Latest_Snapshot,
  (SELECT COUNT(*) FROM zero_now)                               AS Real_Cases_With_No_Customer,
  (SELECT COUNT(*) FROM lost)                                   AS Lost_All_Customers,
  (SELECT COUNT(*) FROM zero_now) - (SELECT COUNT(*) FROM lost) AS Never_Had_Customer_In_Window
""", latest);
