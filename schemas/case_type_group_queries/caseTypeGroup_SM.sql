DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (
        CASE_TYPE_CODE LIKE '34%%' 
        OR CASE_TYPE_CODE LIKE '35%%' 
        OR CASE_TYPE_CODE LIKE '36%%' 
        OR CASE_TYPE_CODE LIKE '37%%' 
        OR CASE_TYPE_CODE LIKE '38%%' 
        OR CASE_TYPE_CODE LIKE '39%%'
    ) 
    AND CASE_TYPE_CODE NOT IN (
        '380800', '383300', '384101', '384103', '384201', '384203', 
        '384301', '384303', '384401', '384403', '386000', '386200', 
        '386210', '386300', '386310', '386400', '386403', '386501'
    )
    ORDER BY NAME
""", snapshot_date);