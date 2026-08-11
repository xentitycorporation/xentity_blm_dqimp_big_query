-- FIXED 2026-07-24: 'land transfer%%' corrected to 'land_transfer%%' -- the field format is always underscore (Land_Transfer), space never occurs in the data (0 vs 46 rows, verified against blm_product_20260705). Original filter was a no-op. Confirmed and applied.

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE 
        CASE_TYPE_CODE LIKE '27%%' 
        AND CASE_TYPE_CODE NOT IN ('274002', '274003', '274004', '274005', '274006', '274200') 
        AND (LOWER(CASE_RECORD_TYPES) NOT LIKE 'land_transfer%%' OR CASE_RECORD_TYPES IS NULL)
    ORDER BY NAME
""", snapshot_date);