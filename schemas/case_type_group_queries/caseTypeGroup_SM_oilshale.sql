DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (
        CASE_TYPE_CODE IN ('355701', '355702', '355703', '392001', '392002', '392003', '393001')
    )
    ORDER BY NAME
""", snapshot_date);
