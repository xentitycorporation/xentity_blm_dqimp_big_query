-- Survey / Survey (self-referential name, client decision 2026-07-30) -- the only
-- subgroup in the new Survey group; identical scope to caseTypeGroup_Survey.sql since
-- the group has no further subdivision (same one-subgroup pattern as
-- caseTypeGroup_SM_solidminerals.sql).
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
