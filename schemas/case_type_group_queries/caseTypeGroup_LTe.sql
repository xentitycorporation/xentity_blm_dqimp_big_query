-- ============================================================================
-- UPDATED 2026-08-10 -- DUAL-CLASSIFICATION RESOLVED. Mark's final call.
-- Removed '262400' from the hardcoded IN-list. 262400 (SCHOOL SELECT PATENTS) now
-- belongs to Land Transfer / Land Transfer ONLY. Matching removal made the same day in
-- caseTypeGroup_LTe_grants.sql (subgroup level).
--
-- 262400 carries CASE_RECORD_TYPES = 'Land_Transfer', so once it is out of this
-- unconditioned IN-list the NOT LIKE 'land_transfer%' condition below excludes it.
--
-- '256301' is deliberately RETAINED here. It remains claimed by both this file and
-- caseTypeGroup_Ltr.sql, but its per-case evidence is unanimous for Land Tenure
-- (2,230 of 2,230 cases read "Land Tenure - Occupancy and Use"), so the Land Tenure
-- claim is the correct one. Its presence in caseTypeGroup_Ltr.sql is still an open
-- item pending the BLM/AFS report. See KB §5.3.2, §5.7.0.
-- ============================================================================

-- FIXED 2026-07-24: 'land transfer%%' corrected to 'land_transfer%%' -- the field format is always underscore (Land_Transfer), space never occurs in the data (0 vs 46 rows, verified against blm_product_20260705). Original filter was a no-op. Confirmed and applied.

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE
        CASE_TYPE_CODE IN ('000445', '007500', '186500', '254100', '256301', '386200', '386210', '386300', '386310', '386400', '386403', '386501')
        OR CASE_TYPE_CODE LIKE '23%%' 
        OR (
            (
                CASE_TYPE_CODE LIKE '00%%' OR CASE_TYPE_CODE LIKE '16%%' OR CASE_TYPE_CODE LIKE '17%%' OR 
                CASE_TYPE_CODE LIKE '18%%' OR CASE_TYPE_CODE LIKE '20%%' OR CASE_TYPE_CODE LIKE '21%%' OR 
                CASE_TYPE_CODE LIKE '22%%' OR CASE_TYPE_CODE LIKE '24%%' OR CASE_TYPE_CODE LIKE '25%%' OR 
                CASE_TYPE_CODE LIKE '26%%' OR CASE_TYPE_CODE LIKE '27%%' OR CASE_TYPE_CODE LIKE '50%%' OR 
                CASE_TYPE_CODE LIKE '54%%' OR CASE_TYPE_CODE LIKE '55%%' OR CASE_TYPE_CODE LIKE '60%%' OR 
                CASE_TYPE_CODE LIKE '65%%' OR CASE_TYPE_CODE LIKE '82%%' OR CASE_TYPE_CODE LIKE '83%%' OR 
                CASE_TYPE_CODE LIKE '85%%'
            ) 
            AND (LOWER(CASE_RECORD_TYPES) NOT LIKE 'land_transfer%%' OR CASE_RECORD_TYPES IS NULL)
        )
    ORDER BY NAME
""", snapshot_date);