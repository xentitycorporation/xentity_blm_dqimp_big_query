-- =====================================================================================
-- Test ID:      WEB01 (step 1 of 5)
-- Test name:    MLRS Case Completeness - Web Experience table builder: build state
-- Test type:    Web app (web-app/tests/) - builds the tables behind the DQIMP Web Experience dashboards
-- Catalog:      Test Catalog v2 row #136 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     session temp tables only; 02_cases.sql to 05_changes.sql read them
-- Parameters:   snapshot_dates (ascending YYYYMMDD array, set by dqimp_webapp.py from webapp_config.json),
--               lookup_table
-- Conventions:  shared conventions S1-S7 in tests/README_newTests.md
-- Validated:    10 snapshots 20260104-20261004, built 2026-10-19 and again 2026-10-26: flows balance
--               on all 80 rows; Cases, Flows and Summary agree for every scope x snapshot x check;
--               independent BigQuery recount of 20261004 matched all four checks in both scopes
-- Added:        2026-10-19
-- =====================================================================================
-- Run all five files in ONE BigQuery session (dqimp_webapp.py does this). The temp tables do
-- not exist outside that session, so 02-05 cannot run on their own.
--
-- WHAT IS TESTED: the four things every real case must have, one pass per case.
--   Subject 20  Missing serial number  SERIAL_NUMBER__C null or blank
--   Subject 06  Missing case land      no case_land row
--   Subject 13  Missing customer       no case_customer row
--   Subject 14  Missing case action    no case_action row
--   The subject numbers are the ones in the test IDs (AT06.1, AT13.1, AT14.1, AT20/AT20.1 list
--   the same cases for one snapshot). They are internal: the published tables carry only
--   Subject_Name, never an ID.
--
-- SCOPE
--   Real cases only (S1). Two populations, chosen on the dashboard with one Population switch:
--     'Core'            ACTIVE / AUTHORIZED / INTERIM / CLOSED
--     'Core + Pending'  the same plus PENDING
--   A row can carry only one Scope value, so each core case is emitted under both scopes.
--   Month-to-month changes are worked out separately in each scope: a PENDING -> ACTIVE case
--   has "entered" the Core list but is "still failing" in Core + Pending.
--
-- TEMP TABLES
--   cc_snaps     one row per snapshot, ascending (snap_idx 0 = earliest), with Months_Back
--   cc_scopes    the two populations
--   cc_subjects  the four checks, with their display names
--   cc_state     EVERY blm_case row per snapshot (Bond/Status and every disposition included)
--                with its four fail flags. The changes need a case's state even when it is
--                outside scope, to tell a fix from a case that just left the population.
--   cc_detail    display fields, only for in-scope cases that fail at least one check
--   cc_pop       in-scope case counts and fail counts by disposition / group / product /
--                origin / mining claim
--   cc_fail      one row per (scope, snapshot, case, subject) failing inside that scope
--   cc_rows      cc_fail plus the cases that left the list, each with its change against the
--                previous snapshot (read by 02_cases.sql and 05_changes.sql)
--
-- FIELD RULES
--   * blm_product is deduplicated with ROW_NUMBER (S3); the taxonomy is joined on
--     SAFE_CAST(... AS INT64) on both sides (S4). BLM_Product_Code stays zero-padded text.
--   * Child tables are read with EXISTS, never joined: case_action has ~7 rows per case.
--   * Missing values are NULL (S5). Disposition is stored upper case already, so the
--     normalised value used for filtering is also the stored value (S2).
--   * Created_Date / Last_Modified_Date are parsed from the stored text as-is. Millions of
--     cases carry bulk-load dates (four days in March 1999); do not read them as case ages.
--   * Origin = 'Legacy' when LEGACY_SERIAL_NUMBER is filled (the case came from LR2000),
--     else 'New'; Mining_Claim = 'Yes' for record type 012t00000008bvNAAQ (S7).
--
-- SNAPSHOTS: dqimp_webapp.py replaces the line marked @@SNAPSHOTS@@ from webapp_config.json.
-- To run by hand, edit it here (ascending).
-- =====================================================================================

DECLARE snapshot_dates ARRAY<STRING> DEFAULT ['20260104','20260202','20260303','20260405','20260503','20260607','20260705','20260802','20260906','20261004'];  -- @@SNAPSHOTS@@
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';
DECLARE i INT64 DEFAULT 0;
DECLARE snap STRING;

CREATE OR REPLACE TEMP TABLE cc_snaps AS
SELECT off AS snap_idx, s AS Snapshot, PARSE_DATE('%Y%m%d', s) AS Snapshot_Date,
       ARRAY_LENGTH(snapshot_dates) - 1 - off AS Months_Back
FROM UNNEST(snapshot_dates) AS s WITH OFFSET off;

CREATE OR REPLACE TEMP TABLE cc_scopes AS
SELECT * FROM UNNEST([
  STRUCT('Core' AS Scope, TRUE AS Core_Only),
  STRUCT('Core + Pending', FALSE)
]);

CREATE OR REPLACE TEMP TABLE cc_subjects AS
SELECT * FROM UNNEST([
  STRUCT('06' AS Subject, 'Missing case land' AS Subject_Name, 1 AS Subject_Order),
  STRUCT('13', 'Missing customer',      2),
  STRUCT('14', 'Missing case action',   3),
  STRUCT('20', 'Missing serial number', 4)
]);

CREATE OR REPLACE TEMP TABLE cc_state (
  snap_idx INT64, Case_ID STRING, Disposition STRING, In_Scope BOOL, In_Core BOOL,
  Fail_Serial BOOL, Fail_Land BOOL, Fail_Customer BOOL, Fail_Action BOOL
);

CREATE OR REPLACE TEMP TABLE cc_detail (
  snap_idx INT64, Case_ID STRING, Serial_Number STRING, Disposition STRING,
  Origin STRING, Mining_Claim STRING,
  Case_Type_Group STRING, Case_Type_Subgroup STRING, BLM_Product STRING, BLM_Product_Code STRING,
  Admin_State STRING, Created_Date DATETIME, Last_Modified_Date DATETIME
);

CREATE OR REPLACE TEMP TABLE cc_pop (
  snap_idx INT64, Disposition STRING, Origin STRING, Mining_Claim STRING,
  Case_Type_Group STRING, Case_Type_Subgroup STRING, BLM_Product STRING, BLM_Product_Code STRING,
  Cases INT64, Fail_Serial INT64, Fail_Land INT64, Fail_Customer INT64, Fail_Action INT64
);

WHILE i < ARRAY_LENGTH(snapshot_dates) DO
  SET snap = snapshot_dates[OFFSET(i)];

  -- Every case, every disposition, with its four flags.
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO cc_state
    SELECT
      %d,
      bc.ID,
      UPPER(TRIM(bc.CASE_STATUS)),
      bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
        AND UPPER(TRIM(bc.CASE_STATUS)) IN ('ACTIVE','AUTHORIZED','INTERIM','CLOSED','PENDING'),
      bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
        AND UPPER(TRIM(bc.CASE_STATUS)) IN ('ACTIVE','AUTHORIZED','INTERIM','CLOSED'),
      NULLIF(TRIM(bc.SERIAL_NUMBER__C), '') IS NULL,
      NOT EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_land_%s` cl
                  WHERE cl.BLM_CASE = bc.ID),
      NOT EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_customer_%s` cc
                  WHERE cc.BLM_CASE = bc.ID),
      NOT EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_action_%s` ca
                  WHERE ca.BLM_CASE = bc.ID)
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc
  """, i, snap, snap, snap, snap);

  -- Display fields for in-scope cases, then the failing ones and the counts.
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE cc_enriched AS
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
      s.snap_idx, s.Case_ID, s.Disposition,
      s.Fail_Serial, s.Fail_Land, s.Fail_Customer, s.Fail_Action,
      NULLIF(TRIM(bc.SERIAL_NUMBER__C), '')                              AS Serial_Number,
      IF(NULLIF(TRIM(bc.LEGACY_SERIAL_NUMBER), '') IS NOT NULL, 'Legacy', 'New') AS Origin,
      IF(bc.RECORDTYPEID = '012t00000008bvNAAQ', 'Yes', 'No')           AS Mining_Claim,
      IFNULL(tx.case_type_group, '(Unclassified)')                       AS Case_Type_Group,
      CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
           WHEN dp.CASE_TYPE_CODE IS NOT NULL
             THEN CONCAT('BLM Product Code ', dp.CASE_TYPE_CODE, ' (not in taxonomy)')
           ELSE '(no case type code / not in blm_product)' END           AS Case_Type_Subgroup,
      dp.NAME                                                            AS BLM_Product,
      dp.CASE_TYPE_CODE                                                  AS BLM_Product_Code,
      NULLIF(TRIM(bc.BLM_ADMIN_STATE__C), '')                            AS Admin_State,
      SAFE.PARSE_DATETIME('%%m/%%d/%%Y %%H:%%M:%%S', TRIM(bc.CREATEDDATE))      AS Created_Date,
      SAFE.PARSE_DATETIME('%%m/%%d/%%Y %%H:%%M:%%S', TRIM(bc.LASTMODIFIEDDATE)) AS Last_Modified_Date
    FROM cc_state s
    JOIN `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc ON bc.ID = s.Case_ID
    LEFT JOIN dedup_product dp ON dp.ID = bc.BLM_PRODUCT
    LEFT JOIN taxonomy      tx ON tx.product_code = SAFE_CAST(dp.CASE_TYPE_CODE AS INT64)
    WHERE s.snap_idx = %d AND s.In_Scope
  """, snap, lookup_table, snap, i);

  INSERT INTO cc_detail
  SELECT snap_idx, Case_ID, Serial_Number, Disposition, Origin, Mining_Claim,
         Case_Type_Group, Case_Type_Subgroup, BLM_Product, BLM_Product_Code,
         Admin_State, Created_Date, Last_Modified_Date
  FROM cc_enriched
  WHERE Fail_Serial OR Fail_Land OR Fail_Customer OR Fail_Action;

  INSERT INTO cc_pop
  SELECT snap_idx, Disposition, Origin, Mining_Claim,
         Case_Type_Group, Case_Type_Subgroup, BLM_Product, BLM_Product_Code,
         COUNT(*), COUNTIF(Fail_Serial), COUNTIF(Fail_Land), COUNTIF(Fail_Customer), COUNTIF(Fail_Action)
  FROM cc_enriched
  GROUP BY snap_idx, Disposition, Origin, Mining_Claim,
           Case_Type_Group, Case_Type_Subgroup, BLM_Product, BLM_Product_Code;

  SET i = i + 1;
END WHILE;

DROP TABLE cc_enriched;

-- One row per (scope, snapshot, case, subject) that FAILS the check inside that scope.
-- Core cases appear under both scopes; PENDING cases only under 'Core + Pending'.
CREATE OR REPLACE TEMP TABLE cc_fail AS
SELECT sc.Scope, s.snap_idx, s.Case_ID, t.Subject
FROM cc_state s
CROSS JOIN UNNEST([
  STRUCT('20' AS Subject, s.Fail_Serial AS f),
  STRUCT('06', s.Fail_Land),
  STRUCT('13', s.Fail_Customer),
  STRUCT('14', s.Fail_Action)
]) t
CROSS JOIN cc_scopes sc
WHERE t.f AND (s.In_Core OR (s.In_Scope AND NOT sc.Core_Only));

-- One row per (scope, snapshot, check, case) that is on the list now or left it this snapshot,
-- with its change against the previous snapshot (meanings in 02_cases.sql). 02_cases.sql lists
-- these rows; 05_changes.sql counts them. Keeping the classification here means both tables
-- always agree.
CREATE OR REPLACE TEMP TABLE cc_rows AS
WITH cur AS (   -- on the list now, classified by how it arrived
  SELECT
    f.Scope, f.snap_idx, f.Subject, f.Case_ID,
    CASE
      WHEN f.snap_idx = 0 THEN 'Baseline'
      WHEN pf.Case_ID IS NOT NULL THEN 'Still failing'
      WHEN ps.Case_ID IS NULL THEN 'New case'
      WHEN CASE f.Subject WHEN '20' THEN ps.Fail_Serial WHEN '06' THEN ps.Fail_Land
                          WHEN '13' THEN ps.Fail_Customer ELSE ps.Fail_Action END
        THEN 'Entered scope, already failing'
      ELSE 'Lost record (regression)'
    END AS Change_Type,
    IF(f.snap_idx = 0 OR pf.Case_ID IS NOT NULL, 'Still on the list', 'Joined the list') AS Change_Direction,
    'Yes' AS Currently_Failing,
    f.snap_idx AS detail_idx
  FROM cc_fail f
  LEFT JOIN cc_fail  pf ON pf.Scope = f.Scope AND pf.Subject = f.Subject AND pf.Case_ID = f.Case_ID
                       AND pf.snap_idx = f.snap_idx - 1
  LEFT JOIN cc_state ps ON ps.Case_ID = f.Case_ID AND ps.snap_idx = f.snap_idx - 1
),
gone AS (       -- on the list last snapshot, off it now
  SELECT
    p.Scope, p.snap_idx + 1 AS snap_idx, p.Subject, p.Case_ID,
    CASE
      WHEN cs.Case_ID IS NULL THEN 'Removed from MLRS'
      WHEN CASE p.Subject WHEN '20' THEN cs.Fail_Serial WHEN '06' THEN cs.Fail_Land
                          WHEN '13' THEN cs.Fail_Customer ELSE cs.Fail_Action END
        THEN 'Left scope, still failing'
      ELSE 'Fixed'
    END AS Change_Type,
    'Left the list' AS Change_Direction,
    'No' AS Currently_Failing,
    p.snap_idx AS detail_idx
  FROM cc_fail p
  LEFT JOIN cc_fail  cf ON cf.Scope = p.Scope AND cf.Subject = p.Subject AND cf.Case_ID = p.Case_ID
                       AND cf.snap_idx = p.snap_idx + 1
  LEFT JOIN cc_state cs ON cs.Case_ID = p.Case_ID AND cs.snap_idx = p.snap_idx + 1
  WHERE cf.Case_ID IS NULL
    AND p.snap_idx < (SELECT MAX(snap_idx) FROM cc_snaps)
)
SELECT * FROM cur UNION ALL SELECT * FROM gone;

-- Canary: one row per snapshot so the caller can see the build ran on every date.
SELECT sn.Snapshot, COUNT(s.Case_ID) AS Cases_In_Scope
FROM cc_snaps sn LEFT JOIN cc_state s ON s.snap_idx = sn.snap_idx AND s.In_Scope
GROUP BY sn.Snapshot ORDER BY sn.Snapshot;
