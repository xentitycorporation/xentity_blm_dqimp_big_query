DECLARE snapshot_date STRING DEFAULT '20250901'; 

-- SYT04
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    SELECT cats.QueryName, '%s' AS Snapshot, COUNT(t.QueryName) AS RowCount
    FROM (
      SELECT CASE
        WHEN b.LEGACY_SERIAL_NUMBER IS NOT NULL AND b.RECORDTYPEID = '012t00000008bvNAAQ' THEN 'SYT04a12'
        WHEN b.LEGACY_SERIAL_NUMBER IS NOT NULL AND b.RECORDTYPEID != '012t00000008bvNAAQ' THEN 'SYT04a22'
        WHEN b.LEGACY_SERIAL_NUMBER IS NULL  AND b.RECORDTYPEID = '012t00000008bvNAAQ' THEN 'SYT04b12'
        WHEN b.LEGACY_SERIAL_NUMBER IS NULL  AND b.RECORDTYPEID != '012t00000008bvNAAQ' THEN 'SYT04b22'
      END AS QueryName
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
      JOIN `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` n ON b.ID = n.SF_ID
      WHERE LOWER(IFNULL(TRIM(b.CASE_STATUS), '')) IS DISTINCT FROM LOWER(IFNULL(TRIM(n.CSE_DISP), ''))
    ) t
    RIGHT JOIN (SELECT * FROM UNNEST(['SYT04a12','SYT04a22','SYT04b12','SYT04b22']) AS QueryName) cats
      ON t.QueryName = cats.QueryName
    GROUP BY cats.QueryName
  """, snapshot, snapshot, snapshot);

  -- SYT04
DECLARE ca_table STRING;
DECLARE bc_table STRING;
DECLARE rt_table STRING;

SET ca_table = CONCAT('blm_seta_dqimp.case_action_', snapshot_date);
SET bc_table = CONCAT('blm_seta_dqimp.blm_case_', snapshot_date);
SET rt_table = CONCAT('blm_seta_dqimp.record_type_', snapshot_date);

EXECUTE IMMEDIATE FORMAT("""
WITH dedup_record_type AS (
  SELECT ID, NAME
  FROM (
    SELECT
      ID,
      NAME,
      ROW_NUMBER() OVER (PARTITION BY ID ORDER BY NAME) AS rn
    FROM `%s`
  )
  WHERE rn = 1
),
offenders AS (
  SELECT
    bc.ID AS blm_case_id,
    rt.NAME AS record_type_name
  FROM `%s` ca
  JOIN `%s` bc
    ON ca.BLM_CASE = bc.ID
  LEFT JOIN dedup_record_type rt
    ON bc.RECORDTYPEID = rt.ID
  WHERE ca.ACTION_DATE IS NULL
  GROUP BY bc.ID, rt.NAME
)
SELECT
  record_type_name,
  COUNT(DISTINCT blm_case_id) AS blm_case_count
FROM offenders
GROUP BY record_type_name
ORDER BY blm_case_count DESC;
""", rt_table, ca_table, bc_table);

END;