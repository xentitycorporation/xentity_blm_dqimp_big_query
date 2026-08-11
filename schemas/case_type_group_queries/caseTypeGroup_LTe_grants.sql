-- FIXED 2026-07-24: 'land transfer%%' corrected to 'land_transfer%%' -- the field format is always underscore (Land_Transfer), space never occurs in the data (0 vs 46 rows, verified against blm_product_20260705). Original filter was a no-op. Confirmed and applied.

-- ============================================================================
-- UPDATED 2026-08-10 -- DUAL-CLASSIFICATION RESOLVED. Mark's final call.
-- Removed the hardcoded `CASE_TYPE_CODE = '262400'` bypass. 262400 (SCHOOL SELECT
-- PATENTS) now belongs to Land Transfer / Land Transfer ONLY.
--
-- Why removing the hardcode is sufficient: 262400 carries CASE_RECORD_TYPES =
-- 'Land_Transfer' on all 9 blm_product rows, so the existing
-- NOT LIKE 'land_transfer%' condition below now excludes it on its own. No new
-- exclusion entry was needed. Verified against blm_product_20260802.
--
-- Known cost of this decision, accepted: 262400's live NLSDB cases genuinely split
-- 238 "Land Transfer - Grants" (81.5%) / 54 "Land Tenure - Grants" (18.5%), because
-- BLM's own ArcGIS services route these cases by land status through the Alaska State
-- selection pipeline, not by product code. Those 54 cases will now classify as Land
-- Transfer. For the precise per-case answer use
-- caseTypeGroup_PerCase_256301_262400.sql. See KB §5.1.4 and §5.7.0.
--
-- The matching removal was made in caseTypeGroup_LTe.sql (group level) the same day.
-- 256301 is deliberately NOT changed -- it stays in Land Tenure, which its per-case
-- evidence supports unanimously.
-- ============================================================================

-- FIXED 2026-07-24: 'land transfer%%' corrected to 'land_transfer%%' -- the field format is always underscore (Land_Transfer), space never occurs in the data (0 vs 46 rows, verified against blm_product_20260705). Original filter was a no-op. Confirmed and applied.

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE
        (
            CASE_TYPE_CODE LIKE '26%%'
        )
        AND (LOWER(CASE_RECORD_TYPES) NOT LIKE 'land_transfer%%' OR CASE_RECORD_TYPES IS NULL)
    ORDER BY NAME
""", snapshot_date);
