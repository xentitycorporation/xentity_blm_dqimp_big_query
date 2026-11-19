-- =====================================================================================
-- Test ID:      WEB01 (step 4 of 5)
-- Test name:    MLRS Case Completeness - Web Experience table builder: Summary table
-- Test type:    Web app (web-app/tests/) - builds the tables behind the DQIMP Web Experience dashboards
-- Catalog:      Test Catalog v2 row #136 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Case_Completeness_Summary.csv -> hosted table "MLRS Case Completeness - Summary"
-- Parameters:   none; reads the session temp tables built by 01_build_state.sql
-- Conventions:  shared conventions S1-S7 in tests/README_newTests.md
-- Validated:    10 snapshots 20260104-20261004, built 2026-10-19 and again 2026-10-26: flows balance
--               on all 80 rows; Cases, Flows and Summary agree for every scope x snapshot x check;
--               independent BigQuery recount of 20261004 matched all four checks in both scopes
-- Added:        2026-10-19
-- =====================================================================================
-- The counts behind the page's trend chart and breakdowns.
-- ONE ROW PER (scope, snapshot, check, disposition, origin, mining claim, case type group,
-- subgroup, BLM product) where at least one case exists. Cases is the denominator;
-- Failing_Cases is how many of them fail the check, which may be 0. Keeping the zero rows
-- gives every filter combination its denominator: failure rate = sum(Failing_Cases) / sum(Cases).
-- Core rows appear under both scopes; PENDING rows only under 'Core + Pending'.
--
-- DO NOT SUM Failing_Cases ACROSS CHECKS OR SCOPES and call it a number of cases: a case
-- failing two checks is counted once per check. Always filter on Scope and Subject_Name.
-- =====================================================================================

SELECT
  sn.Snapshot_Date,
  sn.Snapshot,
  sn.Months_Back,
  sc.Scope,
  t.Subject_Name,
  t.Subject_Order,
  p.Disposition,
  CASE p.Disposition WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'INTERIM' THEN 3
                     WHEN 'CLOSED' THEN 4 ELSE 5 END AS Disposition_Rank,
  p.Origin,
  p.Mining_Claim,
  p.Case_Type_Group,
  p.Case_Type_Subgroup,
  p.BLM_Product,
  p.BLM_Product_Code,
  p.Cases,
  CASE t.Subject WHEN '20' THEN p.Fail_Serial WHEN '06' THEN p.Fail_Land
                 WHEN '13' THEN p.Fail_Customer ELSE p.Fail_Action END AS Failing_Cases
FROM cc_pop p
JOIN cc_snaps sn ON sn.snap_idx = p.snap_idx
JOIN cc_scopes sc ON p.Disposition <> 'PENDING' OR NOT sc.Core_Only
CROSS JOIN cc_subjects t
ORDER BY sn.Snapshot, sc.Scope, t.Subject_Order, Disposition_Rank,
  p.Case_Type_Group, p.Case_Type_Subgroup, p.BLM_Product_Code, p.Origin, p.Mining_Claim;
