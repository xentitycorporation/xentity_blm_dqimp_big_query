DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT 
        CASE_STATUS,
        COUNT(*) as case_count
    FROM `blm_seta_dqimp.blm_case_%s`
    GROUP BY 
        CASE_STATUS
    ORDER BY 
        case_count DESC
""", snapshot_date);