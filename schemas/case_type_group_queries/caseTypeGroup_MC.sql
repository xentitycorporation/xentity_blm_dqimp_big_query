DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '384101', '384103', '384201', '384203', '384301', '384303', '384401', '384403'
    )
    ORDER BY NAME
""", snapshot_date);