DECLARE snapshot_date STRING DEFAULT '20250901';
DECLARE bc_table STRING;
DECLARE bp_table STRING;
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Codes_in_Case_Type_Groups';

SET bc_table = CONCAT('blm_seta_dqimp.blm_case_', snapshot_date);
SET bp_table = CONCAT('blm_seta_dqimp.blm_product_', snapshot_date);

-- SYT04
BEGIN
EXECUTE IMMEDIATE FORMAT("""
  WITH dedup_product AS (
    SELECT ID, CASE_TYPE_CODE
    FROM (
      SELECT ID, CASE_TYPE_CODE,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) AS rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    )
    WHERE rn = 1
  ),
  group_lookup AS (
    SELECT DISTINCT `Product Code`, `Case Type`
    FROM `%s`
  ),
  case_type_groups AS (
    SELECT DISTINCT `Case Type` AS case_type FROM group_lookup
  ),
  mismatches AS (
    SELECT
      lk.`Case Type` AS case_type,
      CASE WHEN b.LEGACY_SERIAL_NUMBER IS NOT NULL THEN 1 ELSE 0 END AS is_legacy
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
    JOIN `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` n ON b.ID = n.SF_ID
    LEFT JOIN dedup_product dp ON b.BLM_PRODUCT = dp.ID
    LEFT JOIN group_lookup lk ON dp.CASE_TYPE_CODE = lk.`Product Code`
    WHERE LOWER(IFNULL(TRIM(b.CASE_STATUS), '')) IS DISTINCT FROM LOWER(IFNULL(TRIM(n.CSE_DISP), ''))
  )
  SELECT
    CONCAT('SYT04_', REPLACE(REPLACE(ctg.case_type, ' ', ''), '_', '')) AS QueryName,
    '%s' AS Snapshot,
    COUNTIF(m.is_legacy = 1) AS Legacy_Count,
    COUNTIF(m.is_legacy = 0) AS NonLegacy_Count
  FROM case_type_groups ctg
  LEFT JOIN mismatches m ON m.case_type = ctg.case_type
  GROUP BY ctg.case_type
  ORDER BY ctg.case_type
""", snapshot_date, lookup_table, snapshot_date, snapshot_date, snapshot_date);

-- SYT04 (blm_case counts by Case Type)
EXECUTE IMMEDIATE FORMAT("""
WITH dedup_product AS (
  SELECT ID, CASE_TYPE_CODE
  FROM (
    SELECT ID, CASE_TYPE_CODE,
      ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) AS rn
    FROM `%s`
  )
  WHERE rn = 1
),
group_lookup AS (
  SELECT DISTINCT `Product Code`, `Case Type`
  FROM `%s`
)
SELECT
  lk.`Case Type` AS case_type,
  COUNT(DISTINCT bc.ID) AS blm_case_count
FROM `%s` bc
LEFT JOIN dedup_product dp ON bc.BLM_PRODUCT = dp.ID
LEFT JOIN group_lookup lk ON dp.CASE_TYPE_CODE = lk.`Product Code`
GROUP BY lk.`Case Type`
ORDER BY blm_case_count DESC;
""", bp_table, lookup_table, bc_table);

END;