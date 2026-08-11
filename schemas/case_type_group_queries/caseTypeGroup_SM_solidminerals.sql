-- Solid Minerals / Solid Minerals (self-referential name, client decision 2026-07) --
-- catch-all subgroup for 3 codes that don't fit any other Solid Minerals subgroup.
-- None of the 3 appear in any other subgroup query's inclusion or exclusion list.
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '360050', '360099', '391000'
    )
    ORDER BY NAME
""", snapshot_date);
