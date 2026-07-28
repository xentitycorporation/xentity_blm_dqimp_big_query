DECLARE snapshot_date STRING DEFAULT '20250901';
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

EXECUTE IMMEDIATE FORMAT("""
  WITH dedup_product AS (
    -- Deduplicates the product table to ensure a 1-to-1 join
    SELECT ID, CASE_TYPE_CODE
    FROM (
      SELECT 
        ID,
        CASE_TYPE_CODE,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) AS rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    )
    WHERE rn = 1
  ),
  
  group_lookup AS (
    -- Extracts the distinct mapping of product codes to case groups
    SELECT DISTINCT
      CAST(`BLM Product Code` AS STRING) AS product_code,
      `Case Type Group`,
      `Case Type Subgroup`
    FROM `%s`
  ),
  
  mlrs AS (
    SELECT DISTINCT 
      bc.SERIAL_NUMBER__C,
      bc.CASE_STATUS,
      lk.`Case Type Group` AS case_type_group,
      lk.`Case Type Subgroup` AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS bc
    LEFT JOIN dedup_product AS dp 
      ON bc.BLM_PRODUCT = dp.ID
    LEFT JOIN group_lookup AS lk 
      ON dp.CASE_TYPE_CODE = lk.product_code
    WHERE bc.CASE_STATUS != 'STATUS RECORD'
      AND bc.SERIAL_NUMBER__C IS NOT NULL
  ),
  
  nlsdb AS (
    SELECT DISTINCT 
      nc.CSE_NR,
      nc.CSE_DISP,
      lk.`Case Type Group` AS case_type_group,
      lk.`Case Type Subgroup` AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` AS nc
    -- Linking through the BLM case table to get product types for NLSDB cases
    LEFT JOIN `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS bc
      ON nc.CSE_NR = bc.SERIAL_NUMBER__C
    LEFT JOIN dedup_product AS dp 
      ON bc.BLM_PRODUCT = dp.ID
    LEFT JOIN group_lookup AS lk 
      ON dp.CASE_TYPE_CODE = lk.product_code
    WHERE nc.CSE_NR IS NOT NULL
  )
  
  SELECT 
    nlsdb.CSE_NR AS missing_from_mlrs,
    nlsdb.CSE_DISP AS missing_from_mlrs_status,
    nlsdb.case_type_group AS missing_from_mlrs_case_type_group,
    nlsdb.case_type_subgroup AS missing_from_mlrs_case_type_subgroup,
    
    mlrs.SERIAL_NUMBER__C AS missing_from_nlsdb,
    mlrs.CASE_STATUS AS missing_from_nlsdb_status,
    mlrs.case_type_group AS missing_from_nlsdb_case_type_group,
    mlrs.case_type_subgroup AS missing_from_nlsdb_case_type_subgroup
  FROM mlrs
  FULL OUTER JOIN nlsdb 
    ON mlrs.SERIAL_NUMBER__C = nlsdb.CSE_NR
  -- This WHERE clause isolates only the exceptions (mismatches)
  WHERE mlrs.SERIAL_NUMBER__C IS NULL 
     OR nlsdb.CSE_NR IS NULL
""", 
snapshot_date, 
lookup_table, 
snapshot_date, 
snapshot_date, 
snapshot_date);
