-- =====================================================================================
-- Test ID:      WEB01 (step 2 of 5)
-- Test name:    MLRS Case Completeness - Web Experience table builder: Cases table
-- Test type:    Web app (web-app/tests/) - builds the tables behind the DQIMP Web Experience dashboards
-- Catalog:      Test Catalog v2 row #136 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     MLRS_Case_Completeness_Cases.csv -> hosted table "MLRS Case Completeness - Cases"
-- Parameters:   none; reads the session temp tables built by 01_build_state.sql
-- Conventions:  shared conventions S1-S7 in tests/README_newTests.md
-- Validated:    10 snapshots 20260104-20261004, built 2026-10-19 and again 2026-10-26: flows balance
--               on all 80 rows; Cases, Flows and Summary agree for every scope x snapshot x check;
--               independent BigQuery recount of 20261004 matched all four checks in both scopes
-- Added:        2026-10-19
-- =====================================================================================
-- ONE ROW PER (scope, snapshot, check, case) for:
--   * every case FAILING the check in that scope and snapshot, and
--   * every case that STOPPED failing it in that scope since the previous snapshot
--     (Change_Direction = 'Left the list'), so a fixed case is listed in the month it was fixed.
--   A case failing two checks appears once per check, and a core case once per scope: always
--   filter on Scope and Subject_Name.
--
-- CHANGE_TYPE, against the previous snapshot, within the row's scope:
--   Baseline                        first snapshot, nothing to compare with
--   Still failing                   was on the list last snapshot too
--   New case                        not in blm_case last snapshot
--   Lost record (regression)        existed last snapshot and PASSED this check
--   Entered scope, already failing  failed the check last snapshot but was outside the scope
--                                   (another disposition, or a Bond/Status record type)
--   Fixed                           now passes the check -- the only real fix
--   Left scope, still failing       still fails, but moved outside the scope: NOT a fix
--   Removed from MLRS               no longer in blm_case
-- A case that left the list shows its display fields from the previous snapshot, the last
-- time it was on the list.
-- =====================================================================================

-- The rows and their Change_Type come from cc_rows (01_build_state.sql), shared with 05_changes.sql.
SELECT
  sn.Snapshot_Date,
  sn.Snapshot,
  sn.Months_Back,
  r.Scope,
  t.Subject_Name,
  t.Subject_Order,
  r.Change_Direction,
  r.Change_Type,
  r.Currently_Failing,
  d.Case_ID,
  d.Serial_Number,
  d.Disposition,
  CASE d.Disposition WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'INTERIM' THEN 3
                     WHEN 'CLOSED' THEN 4 ELSE 5 END AS Disposition_Rank,
  d.Origin,
  d.Mining_Claim,
  d.Case_Type_Group,
  d.Case_Type_Subgroup,
  d.BLM_Product,
  d.BLM_Product_Code,
  d.Admin_State,
  d.Created_Date,
  d.Last_Modified_Date
FROM cc_rows r
JOIN cc_snaps    sn ON sn.snap_idx = r.snap_idx
JOIN cc_subjects t  ON t.Subject = r.Subject
JOIN cc_detail   d  ON d.Case_ID = r.Case_ID AND d.snap_idx = r.detail_idx
ORDER BY sn.Snapshot, r.Scope, t.Subject_Order, Disposition_Rank,
  d.Case_Type_Group, d.Case_Type_Subgroup, d.Serial_Number, d.Case_ID, r.Change_Type;
