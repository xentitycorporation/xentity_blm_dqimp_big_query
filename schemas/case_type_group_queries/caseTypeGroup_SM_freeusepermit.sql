-- Solid Minerals / Free Use Permit -- new subgroup, client decision 2026-07. Code 362313
-- was previously carved out of caseTypeGroup_SM_saleables.sql's exclusion list with no
-- subgroup query of its own; this file is that query.
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '362313'
    )
    ORDER BY NAME
""", snapshot_date);
