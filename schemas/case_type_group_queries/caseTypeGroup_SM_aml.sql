-- Solid Minerals / AML (Abandoned Mine Lands) -- new subgroup, client decision 2026-07.
-- All 3 codes were previously carved out of caseTypeGroup_SM_locatables.sql's exclusion
-- list with no subgroup query of their own; this file is that query.
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '372000', '372001', '372002'
    )
    ORDER BY NAME
""", snapshot_date);
