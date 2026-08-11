-- Case Type Group: Survey (new 7th group, added 2026-07-30 following client taxonomy
-- update). Supersedes the earlier "Admin" placeholder for these 2 codes -- see
-- caseTypeGroup_Admin.sql for retirement notes. Explicit code list used (not a '91%'
-- prefix pattern) since no established BLM prefix convention for this group has been
-- confirmed -- both codes were previously undocumented by any official
-- CaseTypeGroupQueries pattern (91xxxx prefix matches nothing else in the taxonomy).
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '918000', '918300'
    )
    ORDER BY NAME
""", snapshot_date);
