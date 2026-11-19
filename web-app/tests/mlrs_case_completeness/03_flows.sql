-- =====================================================================================
-- Test ID:      WEB01 (step 3 of 5)
-- Test name:    MLRS Case Completeness - Web Experience table builder: Flows table
-- Test type:    Web app (web-app/tests/) - builds the tables behind the DQIMP Web Experience dashboards
-- Catalog:      Test Catalog v2 row #136 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Case_Completeness_Flows.csv -> hosted table "MLRS Case Completeness - Flows"
-- Parameters:   none; reads the session temp tables built by 01_build_state.sql
-- Conventions:  shared conventions S1-S7 in tests/README_newTests.md
-- Validated:    10 snapshots 20260104-20261004, built 2026-10-19 and again 2026-10-26: flows balance
--               on all 80 rows; Cases, Flows and Summary agree for every scope x snapshot x check;
--               independent BigQuery recount of 20261004 matched all four checks in both scopes
-- Added:        2026-10-19
-- =====================================================================================
-- The month-over-month changes behind the page's waterfall and headline numbers.
-- ONE ROW PER (scope, snapshot, check).
--
-- IT BALANCES BY CONSTRUCTION:
--   Current_Total = Prior_Total + Joined - Left
--   Joined        = New_Case + Lost_Record_Regression + Entered_Scope_Already_Failing
--   Left          = Fixed + Left_Scope_Still_Failing + Removed_From_MLRS
--   Balance_Check = 0 on every row, or the build is wrong (dqimp_webapp.py stops if not).
--
-- ONLY 'Fixed' IS A FIX. 'Left_Scope_Still_Failing' is a case that still fails but changed
-- disposition or record type; counting it as fixed overstated land remediation by ~30%.
-- 'Lost_Record_Regression' is a case that HAD the record last snapshot and lost it.
--
-- CHART COLUMNS (Chart_*): ArcGIS Dashboards has no waterfall chart, so these are the same
-- six counts signed for a stacked column chart, accounting style: cases joining the list
-- are positive (up, adding to the problem), cases leaving it negative (down). The unsigned
-- columns are kept for the headline numbers and tables.
-- =====================================================================================

WITH totals AS (
  SELECT Scope, snap_idx, Subject, COUNT(*) AS n FROM cc_fail GROUP BY Scope, snap_idx, Subject
),
joined AS (
  SELECT c.Scope, c.snap_idx, c.Subject,
    COUNTIF(ps.Case_ID IS NULL) AS New_Case,
    COUNTIF(ps.Case_ID IS NOT NULL AND
      CASE c.Subject WHEN '20' THEN ps.Fail_Serial WHEN '06' THEN ps.Fail_Land
                     WHEN '13' THEN ps.Fail_Customer ELSE ps.Fail_Action END)
                                AS Entered_Scope_Already_Failing,
    COUNTIF(ps.Case_ID IS NOT NULL AND NOT
      CASE c.Subject WHEN '20' THEN ps.Fail_Serial WHEN '06' THEN ps.Fail_Land
                     WHEN '13' THEN ps.Fail_Customer ELSE ps.Fail_Action END)
                                AS Lost_Record_Regression
  FROM cc_fail c
  LEFT JOIN cc_fail  p  ON p.Scope = c.Scope AND p.Subject = c.Subject AND p.Case_ID = c.Case_ID
                       AND p.snap_idx = c.snap_idx - 1
  LEFT JOIN cc_state ps ON ps.Case_ID = c.Case_ID AND ps.snap_idx = c.snap_idx - 1
  WHERE c.snap_idx > 0 AND p.Case_ID IS NULL
  GROUP BY c.Scope, c.snap_idx, c.Subject
),
left_ AS (
  SELECT p.Scope, p.snap_idx + 1 AS snap_idx, p.Subject,
    COUNTIF(cs.Case_ID IS NULL) AS Removed_From_MLRS,
    COUNTIF(cs.Case_ID IS NOT NULL AND
      CASE p.Subject WHEN '20' THEN cs.Fail_Serial WHEN '06' THEN cs.Fail_Land
                     WHEN '13' THEN cs.Fail_Customer ELSE cs.Fail_Action END)
                                AS Left_Scope_Still_Failing,
    COUNTIF(cs.Case_ID IS NOT NULL AND NOT
      CASE p.Subject WHEN '20' THEN cs.Fail_Serial WHEN '06' THEN cs.Fail_Land
                     WHEN '13' THEN cs.Fail_Customer ELSE cs.Fail_Action END)
                                AS Fixed
  FROM cc_fail p
  LEFT JOIN cc_fail  c  ON c.Scope = p.Scope AND c.Subject = p.Subject AND c.Case_ID = p.Case_ID
                       AND c.snap_idx = p.snap_idx + 1
  LEFT JOIN cc_state cs ON cs.Case_ID = p.Case_ID AND cs.snap_idx = p.snap_idx + 1
  WHERE c.Case_ID IS NULL AND p.snap_idx < (SELECT MAX(snap_idx) FROM cc_snaps)
  GROUP BY p.Scope, p.snap_idx, p.Subject
),
grid AS (
  SELECT sc.Scope, sn.snap_idx, sn.Snapshot, sn.Snapshot_Date, sn.Months_Back,
         t.Subject, t.Subject_Name, t.Subject_Order
  FROM cc_scopes sc CROSS JOIN cc_snaps sn CROSS JOIN cc_subjects t
),
counts AS (
  SELECT
    g.*,
    IF(g.snap_idx = 0, NULL, IFNULL(pt.n, 0))                            AS Prior_Total,
    IF(g.snap_idx = 0, NULL, IFNULL(a.New_Case, 0))                      AS New_Case,
    IF(g.snap_idx = 0, NULL, IFNULL(a.Lost_Record_Regression, 0))        AS Lost_Record_Regression,
    IF(g.snap_idx = 0, NULL, IFNULL(a.Entered_Scope_Already_Failing, 0)) AS Entered_Scope_Already_Failing,
    IF(g.snap_idx = 0, NULL, IFNULL(d.Fixed, 0))                         AS Fixed,
    IF(g.snap_idx = 0, NULL, IFNULL(d.Left_Scope_Still_Failing, 0))      AS Left_Scope_Still_Failing,
    IF(g.snap_idx = 0, NULL, IFNULL(d.Removed_From_MLRS, 0))             AS Removed_From_MLRS,
    IFNULL(ct.n, 0)                                                      AS Current_Total
  FROM grid g
  LEFT JOIN totals ct ON ct.Scope = g.Scope AND ct.snap_idx = g.snap_idx     AND ct.Subject = g.Subject
  LEFT JOIN totals pt ON pt.Scope = g.Scope AND pt.snap_idx = g.snap_idx - 1 AND pt.Subject = g.Subject
  LEFT JOIN joined a  ON a.Scope  = g.Scope AND a.snap_idx  = g.snap_idx     AND a.Subject  = g.Subject
  LEFT JOIN left_  d  ON d.Scope  = g.Scope AND d.snap_idx  = g.snap_idx     AND d.Subject  = g.Subject
)
SELECT
  Snapshot_Date,
  Snapshot,
  Months_Back,
  Scope,
  Subject_Name,
  Subject_Order,
  Prior_Total,
  New_Case,
  Lost_Record_Regression,
  Entered_Scope_Already_Failing,
  Fixed,
  Left_Scope_Still_Failing,
  Removed_From_MLRS,
  Current_Total,
  Current_Total - Prior_Total                                            AS Net_Change,
  New_Case                                                               AS Chart_New_Case,
  Lost_Record_Regression                                                 AS Chart_Lost_Record,
  Entered_Scope_Already_Failing                                          AS Chart_Entered_Scope,
  -Fixed                                                                 AS Chart_Fixed,
  -Left_Scope_Still_Failing                                              AS Chart_Left_Scope,
  -Removed_From_MLRS                                                     AS Chart_Removed,
  Prior_Total + New_Case + Lost_Record_Regression + Entered_Scope_Already_Failing
    - Fixed - Left_Scope_Still_Failing - Removed_From_MLRS - Current_Total AS Balance_Check
FROM counts
ORDER BY Scope, Subject_Order, Snapshot;
