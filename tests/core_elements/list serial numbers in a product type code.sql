DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        c.ID AS ID,
        c.NAME,
        c.SERIAL_NUMBER__C,
        c.LEGACY_SERIAL_NUMBER,
        p.CASE_TYPE_CODE,
        p.NAME AS PRODUCT_NAME
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS c
    JOIN `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s` AS p
        ON c.BLM_PRODUCT = p.ID
    WHERE p.CASE_TYPE_CODE = '8518'
    ORDER BY c.ID
""", snapshot_date, snapshot_date);