-- Land Transfer / Land Transfer (self-referential name, client decision 2026-07) --
-- subgroup-level query for the 31 codes committed in the "Case Type Group and Subgroup"
-- sheet. Note this is a subset of the broader 50-code group-level match in
-- caseTypeGroup_Ltr.sql (CASE_TYPE_CODE IN (...) AND CASE_RECORD_TYPES LIKE
-- 'Land_Transfer%') -- the remaining ~19 group-level codes are not yet committed to this
-- (or any) subgroup in the taxonomy.
-- UPDATED 2026-07-30: added 262400 (School Select Patents) -- resolved to Land Transfer
-- per client decision following the per-case REC_TYPE_CSE_GRP investigation (81.5% of its
-- live NLSDB cases are "Land Transfer - Grants" vs 18.5% "Land Tenure - Grants"; the
-- more precise per-case split, when needed, is available via
-- caseTypeGroup_PerCase_256301_262400.sql in this same folder).
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '256100', '262400', '262710', '262711', '262712', '262713', '262714', '262720',
        '262722', '262730', '262740', '265102', '265200', '265201', '265202',
        '265203', '265204', '265206', '265208', '265209', '265210', '265301',
        '265302', '265303', '265304', '265306', '265308', '265400', '265502',
        '265600', '265800'
    )
    ORDER BY NAME
""", snapshot_date);
