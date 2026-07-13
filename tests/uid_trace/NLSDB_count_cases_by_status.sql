DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT 
        CSE_DISP,
        COUNT(*) as case_count
    FROM `blm_seta_dqimp.nlsdb_case_%s`
    GROUP BY 
        CSE_DISP
    ORDER BY 
        case_count DESC
""", snapshot_date);