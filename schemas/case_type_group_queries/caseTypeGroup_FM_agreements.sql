DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '315100', '318110', '318120', '318130', '318210', '318220', 
        '318240', '318250', '318260', '318290', '318310', '318320', 
        '318330', '318410', '318510', '320901', '328110', '328120', 
        '328200', '328210', '328240', '328300', '328500', '313700', 
        '313710', '315200'
    )
    ORDER BY NAME
""", snapshot_date);