-- Fluid Minerals / Fluid Minerals (self-referential name, client decision 2026-07) --
-- catch-all subgroup for 2 codes that don't fit any other Fluid Minerals subgroup.
-- Both were previously carved out of caseTypeGroup_FM.sql's own exclusion list (and out
-- of caseTypeGroup_FM_oilandgas.sql / caseTypeGroup_FM_geotherm.sql) with no subgroup
-- query of their own; this file is that query. Companion fix: caseTypeGroup_FM.sql's
-- exclusion list no longer excludes these 2 codes at the group level (2026-07-30).
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '310070', '320070'
    )
    ORDER BY NAME
""", snapshot_date);
