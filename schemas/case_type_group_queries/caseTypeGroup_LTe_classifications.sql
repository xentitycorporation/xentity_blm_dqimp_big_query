DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE 
        (
            CASE_TYPE_CODE LIKE '24%%'
        ) 
        AND (LOWER(CASE_RECORD_TYPES) NOT LIKE 'land transfer%%' OR CASE_RECORD_TYPES IS NULL)
    ORDER BY NAME
""", snapshot_date);

