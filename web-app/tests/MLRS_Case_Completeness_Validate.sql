-- =====================================================================================
-- MLRS CASE COMPLETENESS -- single-snapshot VALIDATION of the merged four-truths logic
-- =====================================================================================
-- Staged 2026-09-21 for the DQIMP Web Experience "MLRS Case Completeness" page.
--
-- PURPOSE
--   One pass per case computes all four truths of a valid case:
--     Has_Serial    SERIAL_NUMBER__C populated              (Serial test)
--     Has_Land      EXISTS a case_land row                  (STT06)
--     Has_Customer  EXISTS a case_customer row              (STT13)
--     Has_Action    EXISTS a case_action row                (STT14)
--   Before this logic feeds the Web Experience, it must reproduce the numbers the four
--   separate scripts already delivered. This file checks exactly that; it writes nothing.
--
-- MERGED FROM (logic reused, not rewritten)
--   STT06_Landless_Case_List_With_Case_Dates.sql            population, joins, fields
--   STT13_Customerless_Case_List_Closed_Interim_Auth_Active.sql
--   STT14_Actionless_Case_List_Closed_Interim_Auth_Active.sql
--   Cases_Without_Serial_Number_Detail.sql
--
-- POPULATION
--   Real cases: RECORDTYPEID not Bond (0123d0000004QFQAA2) or Status (0123d0000005ISFAA2).
--   Web Experience scope: disposition ACTIVE / AUTHORIZED / INTERIM / CLOSED, plus PENDING
--   carried as a filterable value (Mark, 2026-09-21).
--
-- EXPECTED ON 20260802
--   PART 1 (four dispositions, from the 09/14 client lists):
--     Missing land      ACTIVE 19   AUTHORIZED 340  INTERIM 89  CLOSED 18,565  = 19,013
--     Missing customer  ACTIVE 5    AUTHORIZED 70   INTERIM 5   CLOSED 181     =    261
--     Missing action    ACTIVE 3    AUTHORIZED 1    INTERIM 0   CLOSED 10      =     14
--   PART 2 (ALL real cases, every disposition):
--     5,448,213 real cases; missing serial 3,071 / land 38,991 / customer 789 / action 3,305;
--     all four present 5,405,872.
--
-- FIELD RULES (carried from the source scripts)
--   * blm_product is duplicated per ID -> ROW_NUMBER dedup before the join.
--   * Group/subgroup join via SAFE_CAST(... AS INT64) on BOTH sides; a string join drops
--     leading zeros (e.g. 01 vs 1). Product code stays zero-padded text.
--   * Child tables are read with EXISTS, never joined (case_action ~7.36 rows per case).
--
-- HOW TO RUN
--   bq query --use_legacy_sql=false --format=pretty < MLRS_Case_Completeness_Validate.sql
-- =====================================================================================

DECLARE snap STRING DEFAULT '20260802';                                   -- <<< CHANGE THIS
DECLARE dispositions ARRAY<STRING> DEFAULT ['ACTIVE', 'AUTHORIZED', 'INTERIM', 'CLOSED', 'PENDING'];

EXECUTE IMMEDIATE FORMAT("""
CREATE TEMP TABLE flags AS
SELECT
  bc.ID                                                      AS Case_ID,
  UPPER(TRIM(bc.CASE_STATUS))                                AS Disposition,
  NULLIF(TRIM(bc.SERIAL_NUMBER__C), '') IS NOT NULL          AS Has_Serial,
  bc.SERIAL_NUMBER__C IS NOT NULL                            AS Has_Serial_NotNull,
  EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_land_%s` cl
          WHERE cl.BLM_CASE = bc.ID)                         AS Has_Land,
  EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_customer_%s` cc
          WHERE cc.BLM_CASE = bc.ID)                         AS Has_Customer,
  EXISTS (SELECT 1 FROM `xentity-sandbox-huy.blm_seta_dqimp.case_action_%s` ca
          WHERE ca.BLM_CASE = bc.ID)                         AS Has_Action
FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` bc
WHERE bc.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
""", snap, snap, snap, snap);


-- PART 1 -- per disposition, the Web Experience scope. Compare with EXPECTED above.
SELECT
  'PART 1 - by disposition' AS Part,
  d AS Disposition,
  COUNT(f.Case_ID)                                  AS Cases,
  COUNTIF(NOT f.Has_Serial)                         AS Missing_Serial,
  COUNTIF(NOT f.Has_Serial_NotNull)                 AS Missing_Serial_NullOnly,
  COUNTIF(NOT f.Has_Land)                           AS Missing_Land,
  COUNTIF(NOT f.Has_Customer)                       AS Missing_Customer,
  COUNTIF(NOT f.Has_Action)                         AS Missing_Action,
  COUNTIF(NOT (f.Has_Serial AND f.Has_Land AND f.Has_Customer AND f.Has_Action))
                                                    AS Fails_Any
FROM UNNEST(dispositions) AS d
LEFT JOIN flags f ON f.Disposition = d
GROUP BY d
ORDER BY CASE d WHEN 'ACTIVE' THEN 1 WHEN 'AUTHORIZED' THEN 2 WHEN 'INTERIM' THEN 3
                WHEN 'CLOSED' THEN 4 ELSE 5 END;


-- PART 2 -- ALL real cases, every disposition. Compare with EXPECTED PART 2 above.
SELECT
  'PART 2 - all real cases' AS Part,
  COUNT(*)                                          AS Real_Cases,
  COUNTIF(NOT Has_Serial_NotNull)                   AS Missing_Serial_NullOnly,
  COUNTIF(NOT Has_Serial)                           AS Missing_Serial_NullOrBlank,
  COUNTIF(NOT Has_Land)                             AS Missing_Land,
  COUNTIF(NOT Has_Customer)                         AS Missing_Customer,
  COUNTIF(NOT Has_Action)                           AS Missing_Action,
  COUNTIF(Has_Serial_NotNull AND Has_Land AND Has_Customer AND Has_Action)
                                                    AS All_Four_Present
FROM flags;
