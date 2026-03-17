-- Step 1: Declare the snapshot dates
DECLARE snapshot_dates ARRAY<STRING> DEFAULT ['20250301', '20250401','20250501'];  
DECLARE i INT64 DEFAULT 0;
DECLARE num_snapshots INT64;
DECLARE snapshot STRING;

-- Step 2: Compute how many snapshots
SET num_snapshots = ARRAY_LENGTH(snapshot_dates);

-- Step 3: Create a temp table to store results
CREATE TEMP TABLE row_counts (
  QueryName STRING,
  Snapshot  STRING,
  RowCount  INT64
);

-- Step 4: Loop through each snapshot
WHILE i < num_snapshots DO
  SET snapshot = snapshot_dates[ORDINAL(i + 1)];

  --SYTCL01
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL01' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              n.SF_CL_ID,
              n.CSE_LND_NR,
              n.SF_ID,
              n.CSE_NR,
              b.ID,
              b.SERIAL_NUMBER__C
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
              ON n.SF_ID = b.ID
          WHERE b.ID IS NOT NULL
              AND IFNULL(TRIM(n.CSE_NR), '') IS DISTINCT FROM IFNULL(TRIM(b.SERIAL_NUMBER__C), '')
          )
  """, snapshot, snapshot, snapshot);

  --SYTCL02
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL02' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              n.SF_CL_ID,
              n.CSE_LND_NR,
              n.SF_ID,
              n.LEG_CSE_NR,
              b.ID,
              b.LEGACY_SERIAL_NUMBER
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
              ON n.SF_ID = b.ID
          WHERE b.ID IS NOT NULL
              AND IFNULL(TRIM(n.LEG_CSE_NR), '') IS DISTINCT FROM IFNULL(TRIM(b.LEGACY_SERIAL_NUMBER), '')
          )
  """, snapshot, snapshot, snapshot);

  --SYTCL03
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL03' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          WITH blm_enriched AS (
          SELECT
              b.ID,
              ARRAY_AGG(rt.NAME IGNORE NULLS LIMIT 1)[OFFSET(0)] AS RECORD_TYPE_NAME,
              ARRAY_AGG(cg.DESCRIPTION IGNORE NULLS LIMIT 1)[OFFSET(0)] AS CASE_GROUP_DESCRIPTION
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` b
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.record_type_%s` rt ON b.RECORDTYPEID = rt.ID
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_group_d_%s` cg ON b.CASE_GROUP = cg.CASE_GROUP_CD
          GROUP BY b.ID
          ),
          joined AS (
          SELECT
              n.ID,
              n.SF_ID,
              n.SF_CL_ID,
              n.CSE_NR,
              n.CSE_LND_NR,
              n.REC_TYPE_CSE_GRP,
              blm.RECORD_TYPE_NAME,
              blm.CASE_GROUP_DESCRIPTION,
              CONCAT(blm.RECORD_TYPE_NAME, ' - ', blm.CASE_GROUP_DESCRIPTION) AS rec_type_cse_grp_sf
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN blm_enriched blm ON n.SF_ID = blm.ID
          )
          SELECT *
          FROM joined
          WHERE NOT (
          REC_TYPE_CSE_GRP = rec_type_cse_grp_sf
          OR (REC_TYPE_CSE_GRP IS NULL AND rec_type_cse_grp_sf IS NULL)
          )
      )
  """, snapshot, snapshot, snapshot, snapshot, snapshot);

  --SYTCL04
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL04' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              l.SF_ID,
              l.CSE_LND_NR,
              c.ID AS BLM_CASE_ID,
              c.BLM_PRODUCT,
              p.ID AS BLM_PRODUCT_ID,
              p.NAME AS BLM_PRODUCT_NAME,
              l.BLM_PROD AS NLSDB_BLM_PROD
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS l
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS c
              ON l.SF_ID = c.ID
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_product_%s` AS p
              ON c.BLM_PRODUCT = p.ID
          WHERE
              LOWER(IFNULL(TRIM(p.NAME), '')) IS DISTINCT FROM LOWER(IFNULL(TRIM(l.BLM_PROD), ''))
      )
  """, snapshot, snapshot, snapshot, snapshot);

  --SYTCL05
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL05' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              l.SF_ID,
              l.CSE_LND_NR,
              c.ID AS BLM_CASE_ID,
              c.BLM_PRODUCT,
              p.ID AS BLM_PRODUCT_ID,
              p.CASE_TYPE_CODE AS BLM_CASE_TYPE_CODE,
              l.CSE_TYPE_NR AS NLSDB_CSE_TYPE_NR
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS l
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS c
              ON l.SF_ID = c.ID
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_product_%s` AS p
              ON c.BLM_PRODUCT = p.ID
          WHERE
              SAFE_CAST(REGEXP_REPLACE(p.CASE_TYPE_CODE, r'^0+', '') AS STRING) IS DISTINCT FROM
              SAFE_CAST(REGEXP_REPLACE(l.CSE_TYPE_NR, r'^0+', '') AS STRING)
      )
  """, snapshot, snapshot, snapshot, snapshot);

  --SYTCL06
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL06' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              l.SF_ID,
              l.SF_CL_ID,
              l.CSE_LND_STATUS AS NLSDB_CSE_LND_STATUS,
              cl.ID AS CASE_LAND_ID,
              cl.LAND_STATUS AS CASE_LAND_STATUS
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS l
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` AS cl
              ON l.SF_CL_ID = cl.ID
          WHERE
              LOWER(IFNULL(TRIM(l.CSE_LND_STATUS), '')) IS DISTINCT FROM
              LOWER(IFNULL(TRIM(cl.LAND_STATUS), ''))
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL07
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL07' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              l.SF_ID,
              l.SF_CL_ID,
              l.CSE_LND_STATUS_DT AS NLSDB_CSE_LND_STATUS_DT,
              cl.ACTION_EFFECTIVE_DATE AS CASE_LAND_EFFECTIVE_DATE
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS l
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` AS cl
              ON l.SF_CL_ID = cl.ID
          WHERE
              SAFE.PARSE_DATETIME('%%m/%%d/%%Y %%H:%%M:%%S', cl.ACTION_EFFECTIVE_DATE) IS NOT NULL
              AND DATE(l.CSE_LND_STATUS_DT) IS NOT NULL
              AND DATE(l.CSE_LND_STATUS_DT) IS DISTINCT FROM DATE(SAFE.PARSE_DATETIME('%%m/%%d/%%Y %%H:%%M:%%S', cl.ACTION_EFFECTIVE_DATE))
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL08
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL08' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              l.SF_ID,
              l.SF_CL_ID,
              l.CSE_LND_NR,
              cl.NAME
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS l
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` AS cl
              ON l.SF_CL_ID = cl.ID
          WHERE
              IFNULL(TRIM(l.CSE_LND_NR), '') IS DISTINCT FROM IFNULL(TRIM(cl.NAME), '')
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL09
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL09' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              l.SF_ID,
              l.SF_CL_ID,
              l.US_RIGHTS AS NLSDB_US_RIGHTS,
              u.US_RIGHTS_RESERVED AS US_RIGHTS_RESERVED
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` AS l
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.us_right_case_land_%s` AS u
              ON l.SF_CL_ID = u.CASE_LAND
          WHERE (
              ARRAY(
              SELECT TRIM(x) FROM UNNEST(SPLIT(IFNULL(l.US_RIGHTS, ''), ',')) AS x
              WHERE TRIM(x) NOT IN (
                  SELECT TRIM(y) FROM UNNEST(SPLIT(IFNULL(u.US_RIGHTS_RESERVED, ''), ',')) AS y
              )
              ) IS NOT NULL
              OR
              ARRAY(
              SELECT TRIM(y) FROM UNNEST(SPLIT(IFNULL(u.US_RIGHTS_RESERVED, ''), ',')) AS y
              WHERE TRIM(y) NOT IN (
                  SELECT TRIM(x) FROM UNNEST(SPLIT(IFNULL(l.US_RIGHTS, ''), ',')) AS x
              )
              ) IS NOT NULL
          )
          AND ARRAY_LENGTH(SPLIT(IFNULL(l.US_RIGHTS, ''), ',')) > 1
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL10
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL10' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              n.SF_ID,
              n.DOC_TYPE,
              b.ID,
              b.DOCUMENT_CATEGORY
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` b
              ON n.SF_ID = b.ID
          WHERE
              LOWER(IFNULL(TRIM(n.DOC_TYPE), '')) IS DISTINCT FROM
              LOWER(IFNULL(TRIM(b.DOCUMENT_CATEGORY), ''))
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL11
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL11' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
              n.SF_ID,
              n.DOC_NR,
              b.ID,
              b.DOCUMENT_NUMBER
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` b
              ON n.SF_ID = b.ID
          WHERE
              LOWER(IFNULL(TRIM(n.DOC_NR), '')) IS DISTINCT FROM
              LOWER(IFNULL(TRIM(b.DOCUMENT_NUMBER), ''))
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL13
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL13' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          WITH parsed_actions AS (
          SELECT *,
              SAFE.PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 1, 10)) AS parsed_dt
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
          WHERE ACTION_DECISION_DT IS NOT NULL
          ),
          latest_action_per_case AS (
          SELECT AS VALUE ARRAY_AGG(pa ORDER BY pa.parsed_dt DESC LIMIT 1)[OFFSET(0)]
          FROM parsed_actions pa
          GROUP BY BLM_CASE
          ),
          with_derived AS (
          SELECT *,
              CASE
              WHEN ACTION_CODE = '552' THEN 'ALL'
              ELSE MINERAL_SEGREGATION
              END AS minsegsfderived
          FROM latest_action_per_case
          ),
          joined_all AS (
          SELECT
              n.ID AS nlsdb_id,
              n.SF_ID,
              n.SEG_MIN,
              d.minsegsfderived,
              b.ID AS blm_case_id
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` b
              ON n.SF_ID = b.ID
          LEFT JOIN with_derived d
              ON b.ID = d.BLM_CASE
          )
          SELECT *
          FROM joined_all
          WHERE NOT (
          (SEG_MIN IS NULL AND minsegsfderived IS NULL)
          OR (SEG_MIN = minsegsfderived)
          )
      )
  """, snapshot, snapshot, snapshot, snapshot);

  --SYTCL14
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL14' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          WITH parsed_actions AS (
          SELECT *,
              SAFE.PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 1, 10)) AS parsed_dt
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
          WHERE ACTION_DECISION_DT IS NOT NULL
          ),
          latest_action_per_case AS (
          SELECT AS VALUE ARRAY_AGG(pa ORDER BY pa.parsed_dt DESC LIMIT 1)[OFFSET(0)]
          FROM parsed_actions pa
          GROUP BY BLM_CASE
          ),
          with_derived AS (
          SELECT *,
              CASE
              WHEN ACTION_CODE = '552' THEN 'ALL'
              ELSE SURFACE_SEGREGATION
              END AS surfsegsfderived
          FROM latest_action_per_case
          ),
          joined_all AS (
          SELECT
              n.ID AS nlsdb_id,
              n.SF_ID,
              n.SEG_SUR,
              d.surfsegsfderived,
              b.ID AS blm_case_id
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` b
              ON n.SF_ID = b.ID
          LEFT JOIN with_derived d
              ON b.ID = d.BLM_CASE
          )
          SELECT *
          FROM joined_all
          WHERE NOT (
          (SEG_SUR IS NULL AND surfsegsfderived IS NULL)
          OR (SEG_SUR = surfsegsfderived)
          )
      )
  """, snapshot, snapshot, snapshot, snapshot);

  --SYTCL15
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL15' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
          n.ID AS nlsdb_id,
          n.SF_ID,
          n.LND_SELECTED_BY,
          c.ALASKA_LAND_SELECTION
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` c
          ON n.SF_CL_ID = c.ID
          WHERE NOT (
          (n.LND_SELECTED_BY IS NULL AND c.ALASKA_LAND_SELECTION IS NULL)
          OR (n.LND_SELECTED_BY = c.ALASKA_LAND_SELECTION)
          )
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL16
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL16' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
          n.ID AS nlsdb_id,
          n.SF_ID,
          n.PRIORITY AS nlsdb_priority,
          c.PRIORITY AS case_land_priority
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` c
          ON n.SF_CL_ID = c.ID
          WHERE NOT (
          (n.PRIORITY IS NULL AND c.PRIORITY IS NULL)
          OR (n.PRIORITY = c.PRIORITY)
          )
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL17
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL17' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
          n.ID AS nlsdb_id,
          n.SF_ID,
          n.CSE_LND_ACRS,
          SAFE_CAST(c.ACRES AS FLOAT64) AS acres_cast,
          ABS(n.CSE_LND_ACRS - SAFE_CAST(c.ACRES AS FLOAT64)) AS abs_difference
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` c
          ON n.SF_CL_ID = c.ID
          WHERE NOT (
          (n.CSE_LND_ACRS IS NULL AND SAFE_CAST(c.ACRES AS FLOAT64) IS NULL)
          OR (n.CSE_LND_ACRS = SAFE_CAST(c.ACRES AS FLOAT64))
          )
      )
  """, snapshot, snapshot, snapshot);

  --SYTCL18
  EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO row_counts
      SELECT 'SYTCL18' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
      FROM (
          SELECT
          n.ID AS nlsdb_id,
          n.SF_ID,
          n.CSE_LND_ID,
          c.ALIS_LEGACY_ID,
          CAST(CAST(SAFE_CAST(c.ALIS_LEGACY_ID AS FLOAT64) AS INT64) AS STRING) AS legacy_id_str
          FROM `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_land_%s` n
          LEFT JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.case_land_%s` c
          ON n.SF_CL_ID = c.ID
          WHERE NOT (
          (n.CSE_LND_ID IS NULL AND c.ALIS_LEGACY_ID IS NULL)
          OR (n.CSE_LND_ID = CAST(CAST(SAFE_CAST(c.ALIS_LEGACY_ID AS FLOAT64) AS INT64) AS STRING))
          )
      )
  """, snapshot, snapshot, snapshot);

  SET i = i + 1;
END WHILE;

-- Step 5: Pivot to wide format
SELECT
  QueryName,
  MAX(IF(Snapshot = '20250301', RowCount, NULL)) AS `20250301`,
  MAX(IF(Snapshot = '20250401', RowCount, NULL)) AS `20250401`,
  MAX(IF(Snapshot = '20250501', RowCount, NULL)) AS `20250501`,
FROM row_counts
GROUP BY QueryName;
