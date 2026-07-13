DECLARE snapshot_date STRING DEFAULT '20250901';
-- Retained in case you need to join it back in later for subgroup context

EXECUTE IMMEDIATE FORMAT("""
  WITH mlrs AS (
    SELECT DISTINCT 
      SERIAL_NUMBER__C,
      CASE_STATUS
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s`
    WHERE CASE_STATUS != 'STATUS RECORD'
      AND SERIAL_NUMBER__C IS NOT NULL
  ),
  
  nlsdb AS (
    SELECT DISTINCT 
      CSE_NR,
      CSE_DISP
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s`
    WHERE CSE_NR IS NOT NULL
  )
  
  SELECT 
    nlsdb.CSE_NR AS missing_from_mlrs,
    nlsdb.CSE_DISP AS missing_from_mlrs_status,
    mlrs.SERIAL_NUMBER__C AS missing_from_nlsdb,
    mlrs.CASE_STATUS AS missing_from_nlsdb_status
  FROM mlrs
  FULL OUTER JOIN nlsdb 
    ON mlrs.SERIAL_NUMBER__C = nlsdb.CSE_NR
  -- This WHERE clause isolates only the exceptions (mismatches)
  WHERE mlrs.SERIAL_NUMBER__C IS NULL 
     OR nlsdb.CSE_NR IS NULL
""", snapshot_date, snapshot_date);