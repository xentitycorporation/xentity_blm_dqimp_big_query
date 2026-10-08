DECLARE snapshot_date STRING DEFAULT '20260802';
DECLARE blm_case_table STRING;
DECLARE nlsdb_case_table STRING;
DECLARE blm_product_table STRING;
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

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
      SELECT 'Survey'
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
        lu.`Case Type Group` AS Case_Type
      FROM `%s` b
      JOIN `%s` n 
        ON b.ID = n.SF_ID
      LEFT JOIN (
        -- blm_product carries ~5 identical rows per ID (4,926 rows / 962 IDs on 20260802).
        -- Dedup here so the join cannot fan out; matches the pattern used by the SYT queries.
        SELECT ID, CASE_TYPE_CODE, NAME
        FROM `%s`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) = 1
      ) p
        ON b.BLM_PRODUCT = p.ID
      LEFT JOIN `%s` lu 
        ON p.CASE_TYPE_CODE = lu.`BLM Product Code`
      -- UPDATED LOGIC: Testing Case Type Code (MLRS) vs Case Type Number (NLSDB)
      WHERE SAFE_CAST(p.CASE_TYPE_CODE AS INT64) IS DISTINCT FROM SAFE_CAST(n.CSE_TYPE_NR AS INT64)
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