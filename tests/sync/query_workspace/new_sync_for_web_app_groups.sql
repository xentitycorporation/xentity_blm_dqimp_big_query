DECLARE snapshot_date STRING DEFAULT '20260201'; 
DECLARE blm_case_table STRING; 
DECLARE nlsdb_case_table STRING;
DECLARE blm_product_table STRING; 
DECLARE ca_table STRING;
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Codes_in_Case_Type_Groups'; 

SET blm_case_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.blm_case_', snapshot_date); 
SET nlsdb_case_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_', snapshot_date); 
SET blm_product_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.blm_product_', snapshot_date); 
SET ca_table = CONCAT('xentity-sandbox-huy.blm_seta_dqimp.case_action_', snapshot_date);

BEGIN
  -- SYT04 First Test: Mismatched Case Status Grouped by Case_Type and Legacy_Status
  EXECUTE IMMEDIATE FORMAT("""
    WITH ExpectedCaseTypes AS (
      SELECT 'Mining Claims' AS Case_Type UNION ALL 
      SELECT 'Fluid Minerals' UNION ALL 
      SELECT 'Solid Minerals' UNION ALL 
      SELECT 'Land Use Authorizations' UNION ALL 
      SELECT 'Land Tenure' UNION ALL 
      SELECT 'Land Transfer' UNION ALL 
      SELECT 'Surveys'
    ),
    LegacyStatuses AS (
      SELECT 'Legacy Serial Number IS NOT NULL' AS Legacy_Status UNION ALL
      SELECT 'Legacy Serial Number IS NULL' AS Legacy_Status
    ),
    ExpectedCategories AS (
      SELECT ct.Case_Type, ls.Legacy_Status 
      FROM ExpectedCaseTypes ct 
      CROSS JOIN LegacyStatuses ls
    ),
    CaseData AS (
      SELECT 
        CASE 
          WHEN b.LEGACY_SERIAL_NUMBER IS NOT NULL THEN 'Legacy Serial Number IS NOT NULL'
          ELSE 'Legacy Serial Number IS NULL'
        END AS Legacy_Status,
        COALESCE(lk.Case_Type_Group, 'Unknown') AS Case_Type
      FROM `%s` b
      JOIN `%s` n ON b.ID = n.SF_ID
      -- UPDATE THE COLUMN NAME ON THE LINE BELOW --
      LEFT JOIN `%s` p ON b.ID = p.YOUR_PRODUCT_TO_CASE_COLUMN
      LEFT JOIN `%s` lk ON p.PRODUCT_CODE = lk.Product_Code
      WHERE LOWER(IFNULL(TRIM(b.CASE_STATUS), '')) IS DISTINCT FROM LOWER(IFNULL(TRIM(n.CSE_DISP), ''))
    )
    SELECT 
      cats.Case_Type, 
      cats.Legacy_Status, 
      '%s' AS Snapshot, 
      COUNT(t.Case_Type) AS RowCount
    FROM ExpectedCategories cats
    LEFT JOIN CaseData t 
      ON t.Case_Type = cats.Case_Type 
      AND t.Legacy_Status = cats.Legacy_Status
    GROUP BY 
      cats.Case_Type, 
      cats.Legacy_Status
    ORDER BY 
      cats.Case_Type, 
      cats.Legacy_Status
  """, blm_case_table, nlsdb_case_table, blm_product_table, lookup_table, snapshot_date);

  -- SYT04 Second Test: Missing Action Date Offenders Grouped by Case_Type
  EXECUTE IMMEDIATE FORMAT("""
    WITH offenders AS (
      SELECT
        bc.ID AS blm_case_id,
        COALESCE(lk.Case_Type_Group, 'Unknown') AS Case_Type
      FROM `%s` ca
      JOIN `%s` bc ON ca.BLM_CASE = bc.ID
      -- UPDATE THE COLUMN NAME ON THE LINE BELOW --
      LEFT JOIN `%s` p ON bc.ID = p.YOUR_PRODUCT_TO_CASE_COLUMN
      LEFT JOIN `%s` lk ON p.PRODUCT_CODE = lk.Product_Code
      WHERE ca.ACTION_DATE IS NULL
      GROUP BY bc.ID, Case_Type
    )
    SELECT
      Case_Type,
      COUNT(DISTINCT blm_case_id) AS blm_case_count
    FROM offenders
    GROUP BY Case_Type
    ORDER BY blm_case_count DESC;
  """, ca_table, blm_case_table, blm_product_table, lookup_table);

END;