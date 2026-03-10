DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT 
        SERIAL_NUMBER__C,
        COUNT(*) OVER() as total_cases
    FROM `blm_seta_dqimp.blm_case_%s`
""", snapshot_date);
