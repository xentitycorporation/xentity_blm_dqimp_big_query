DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (
        CASE_TYPE_CODE LIKE '35%%'
    )
    AND CASE_TYPE_CODE NOT IN ('355701', '355702', '355703', '359010')
    ORDER BY NAME
""", snapshot_date);
