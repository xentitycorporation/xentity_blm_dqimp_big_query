DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE 
        CASE_TYPE_CODE IN ('231118', '231131', '231132', '231133') 
        AND CASE_RECORD_TYPES LIKE 'Land Transfer%%'
    ORDER BY NAME
""", snapshot_date);