-- UPDATED 2026-07-30: removed 310070/320070 from the exclusion list. The spreadsheet
-- now assigns both to Fluid Minerals / Fluid Minerals (self-referential subgroup, see
-- caseTypeGroup_FM_fluidminerals.sql) -- they need to match the Fluid Minerals GROUP
-- query, just not any of the other FM subgroup queries (which still correctly exclude
-- them).
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (CASE_TYPE_CODE LIKE '31%%' OR CASE_TYPE_CODE LIKE '32%%')
      AND CASE_TYPE_CODE NOT IN ('310220', '312020', '313100', '313110', '313140', '313141', '313240')
    ORDER BY NAME
""", snapshot_date);