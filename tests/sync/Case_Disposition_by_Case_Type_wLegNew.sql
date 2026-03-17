DECLARE snapshot_date STRING DEFAULT '20250901';
DECLARE blm_case_table STRING;
DECLARE nlsdb_case_table STRING;
DECLARE blm_product_table STRING;
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Codes_in_Case_Type_Groups';

-- 1. Construct dynamic table names
SET blm_case_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.blm_case_', snapshot_date);
SET nlsdb_case_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_', snapshot_date);
SET blm_product_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.blm_product_', snapshot_date); 

-- 2. Execute the consolidated query with Scaffold technique
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    WITH 
    -- Step 1: Define your 7 explicit Case Types
    ExpectedCaseTypes AS (
      SELECT 'Mining Claims' AS Case_Type UNION ALL
      SELECT 'Fluid Minerals' UNION ALL
      SELECT 'Solid Minerals' UNION ALL
      SELECT 'Land Use Authorizations' UNION ALL
      SELECT 'Land Tenure' UNION ALL
      SELECT 'Land Transfer' UNION ALL
      SELECT 'Surveys'
    ),
    
    -- Step 2: Define your 2 Legacy Statuses
    LegacyStatuses AS (
      SELECT 'Legacy' AS Legacy_Status UNION ALL
      SELECT 'Non-Legacy' AS Legacy_Status
    ),
    
    -- Step 3: CROSS JOIN them to build the perfect 14-row scaffold
    Scaffold AS (
      SELECT ct.Case_Type, ls.Legacy_Status
      FROM ExpectedCaseTypes ct
      CROSS JOIN LegacyStatuses ls
    ),
    
    -- Step 4: Gather actual mismatched records and dynamically label their Legacy Status
    CaseMismatches AS (
      SELECT 
        b.ID, 
        IF(b.LEGACY_SERIAL_NUMBER IS NOT NULL, 'Legacy', 'Non-Legacy') AS Legacy_Status,
        lu.`Case Type` AS Case_Type
      FROM `%s` b
      JOIN `%s` n 
        ON b.ID = n.SF_ID
      LEFT JOIN `%s` p 
        ON b.BLM_PRODUCT = p.ID
      LEFT JOIN `%s` lu 
        ON p.CASE_TYPE_CODE = lu.`Product Code`
      WHERE LOWER(IFNULL(TRIM(b.CASE_STATUS), '')) IS DISTINCT FROM LOWER(IFNULL(TRIM(n.CSE_DISP), ''))
    )

    -- Step 5: LEFT JOIN mismatched data onto the scaffold to ensure 0s are counted
    SELECT 
      s.Case_Type,
      s.Legacy_Status,
      COUNT(DISTINCT cm.ID) AS Error_Count
    FROM Scaffold s
    LEFT JOIN CaseMismatches cm 
      ON s.Case_Type = cm.Case_Type 
      AND s.Legacy_Status = cm.Legacy_Status
    GROUP BY 
      s.Case_Type, 
      s.Legacy_Status
    ORDER BY 
      s.Case_Type, 
      s.Legacy_Status;
  """, blm_case_table, nlsdb_case_table, blm_product_table, lookup_table);
END;