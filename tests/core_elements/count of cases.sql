DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT 
        COUNT(DISTINCT SERIAL_NUMBER__C) as total_unique_cases
    FROM `blm_seta_dqimp.blm_case_%s`
""", snapshot_date);
