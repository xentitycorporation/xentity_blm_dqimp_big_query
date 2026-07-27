DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (
        CASE_TYPE_CODE IN (
            '360511', '360512', '360513', '360411', '360412', '360413',
            '361311', '361312', '361313', '361321', '361322', '361323',
            '360200', '360211', '360212', '360213',
            '361000', '361111', '361112', '361113'
        )
        OR (
            CASE_TYPE_CODE LIKE '362%%'
            AND CASE_TYPE_CODE NOT IN ('362313')
        )
    )
    ORDER BY NAME
""", snapshot_date);
