-- =====================================================================================
-- Test ID:      WEB01 (step 5 of 5)
-- Test name:    MLRS Case Completeness - Web Experience table builder: Changes table
-- Test type:    Web app (web-app/tests/) - builds the tables behind the DQIMP Web Experience dashboards
-- Catalog:      Test Catalog v2 row #136 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Case_Completeness_Changes.csv -> hosted table "MLRS Case Completeness - Changes"
-- Parameters:   none; reads the session temp tables built by 01_build_state.sql
-- Conventions:  shared conventions S1-S7 in tests/README_newTests.md
-- Validated:    built 2026-10-26 on 20260104-20261004: totals equal the Flows table's six change
--               columns for every scope x snapshot x check x change type (7,080 changes); filtered by
--               Disposition, Mining_Claim, Case_Type_Group or Origin, equal the Cases table's change rows
-- Added:        2026-10-26
-- =====================================================================================
-- The month-to-month changes, counted by the same groups as the Summary and Cases tables, so
-- every dashboard filter (disposition, case type group and subgroup, product, origin, mining
-- claim) applies to the monthly-changes chart too. The Flows table carries the same totals but
-- only by population and check; it still feeds the headline indicators.
--
-- ONE ROW PER (scope, snapshot, check, change type, disposition, origin, mining claim, case type
-- group, subgroup, BLM product) where at least one case changed. Only changes: the cases that
-- joined the list (New case, Lost record (regression), Entered scope, already failing) and the
-- cases that left it (Fixed, Left scope, still failing, Removed from MLRS). Baseline and Still
-- failing rows are not changes and are not here.
--
--   Cases         how many cases made this change (always positive)
--   Signed_Cases  the same count, signed for the chart: + joined the list, - left it
--
-- The rows are the Cases table's change rows, counted: for any filter, the sum of Cases here
-- equals the number of matching change rows in the Cases table, and the totals by scope,
-- snapshot, check and change type equal the six change columns of the Flows table.
-- A case that left the list is grouped by its details from the previous snapshot (its
-- disposition, case type, ... when it was last on the list), as in the Cases table.
-- =====================================================================================

SELECT
  sn.Snapshot_Date,
  sn.Snapshot,
  sn.Months_Back,
  r.Scope,
  t.Subject_Name,
  t.Subject_Order,
  r.Change_Direction,
  r.Change_Type,
  d.Disposition,
  CASE d.Disposition WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'INTERIM' THEN 3
                     WHEN 'CLOSED' THEN 4 ELSE 5 END AS Disposition_Rank,
  d.Origin,
  d.Mining_Claim,
  d.Case_Type_Group,
  d.Case_Type_Subgroup,
  d.BLM_Product,
  d.BLM_Product_Code,
  COUNT(*) AS Cases,
  IF(r.Change_Direction = 'Left the list', -COUNT(*), COUNT(*)) AS Signed_Cases
FROM cc_rows r
JOIN cc_snaps    sn ON sn.snap_idx = r.snap_idx
JOIN cc_subjects t  ON t.Subject = r.Subject
JOIN cc_detail   d  ON d.Case_ID = r.Case_ID AND d.snap_idx = r.detail_idx
WHERE r.Change_Direction IN ('Joined the list', 'Left the list')
GROUP BY sn.Snapshot_Date, sn.Snapshot, sn.Months_Back, r.Scope, t.Subject_Name, t.Subject_Order,
  r.Change_Direction, r.Change_Type, d.Disposition, d.Origin, d.Mining_Claim,
  d.Case_Type_Group, d.Case_Type_Subgroup, d.BLM_Product, d.BLM_Product_Code
ORDER BY sn.Snapshot, r.Scope, t.Subject_Order, r.Change_Direction, r.Change_Type,
  Disposition_Rank, d.Case_Type_Group, d.Case_Type_Subgroup, d.BLM_Product_Code, d.Origin, d.Mining_Claim;
