-- Step 1: Declare the snapshot dates
DECLARE snapshot_dates ARRAY<STRING> DEFAULT ['20250501'];-- '20250201', '20250301', '20250401', 
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

  -- SYT01
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT01' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
      SELECT b.ID
      FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
      JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
      WHERE b.BLM_ADMIN_STATE__C IS DISTINCT FROM n.ADMIN_STATE
    )
  """, snapshot, snapshot, snapshot);

  -- SYT03
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT03' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
      SELECT c.ID
      FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS c
      JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_product_%s` AS p
        ON c.BLM_PRODUCT = p.ID
      JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON c.ID = n.SF_ID
      WHERE LOWER(IFNULL(TRIM(p.NAME), '')) IS DISTINCT FROM LOWER(IFNULL(TRIM(n.BLM_PROD), ''))
    )
  """, snapshot, snapshot, snapshot, snapshot);

-- SYT04
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT04' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
      SELECT
        b.ID,
        b.SERIAL_NUMBER__C,
        b.CASE_STATUS,
        n.SF_ID,
        n.CSE_DISP
      FROM
        `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
      JOIN
        `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
      ON
        b.ID = n.SF_ID
      WHERE
        LOWER(IFNULL(TRIM(b.CASE_STATUS), '')) IS DISTINCT FROM
        LOWER(IFNULL(TRIM(n.CSE_DISP), ''))
    )
  """, snapshot, snapshot, snapshot);

  -- SYT05
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT05' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
            SELECT
                c.ID,
                c.BLM_PRODUCT,
                p.CASE_TYPE_CODE AS SF_BLM_PRODUCT_CASE_TYPE_CODE,
                n.SF_ID,
                n.CSE_TYPE_NR AS NLSDB_CSE_TYPE_NR
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS c
            JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_product_%s` AS p
                ON c.BLM_PRODUCT = p.ID
            JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
                ON c.ID = n.SF_ID
            WHERE SAFE_CAST(p.CASE_TYPE_CODE AS INT64) IS DISTINCT FROM SAFE_CAST(n.CSE_TYPE_NR AS INT64)
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT06
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT06' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
            SELECT
                b.ID,
                b.SERIAL_NUMBER__C,
                n.SF_ID,
                n.CSE_NR
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
            JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
            ON b.ID = n.SF_ID
            WHERE
                LOWER(IFNULL(TRIM(b.SERIAL_NUMBER__C), '')) IS DISTINCT FROM
                LOWER(IFNULL(TRIM(n.CSE_NR), ''))
    )
  """, snapshot, snapshot, snapshot);

  -- SYT07
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT07' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.LEGACY_SERIAL_NUMBER,
            n.SF_ID,
            n.LEG_CSE_NR
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
            LOWER(IFNULL(TRIM(b.LEGACY_SERIAL_NUMBER), '')) IS DISTINCT FROM
            LOWER(IFNULL(TRIM(n.LEG_CSE_NR), ''))
    )
  """, snapshot, snapshot, snapshot);

  -- SYT08
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT08' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.CASE_NAME__C,
            n.SF_ID,
            n.CSE_NAME
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
            LOWER(IFNULL(TRIM(b.CASE_NAME__C), '')) IS DISTINCT FROM
            LOWER(IFNULL(TRIM(n.CSE_NAME), ''))
            )
  """, snapshot, snapshot, snapshot);

  -- SYT09
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT09' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.COMMODITY,
            n.SF_ID,
            n.CMMDTY
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
            LOWER(IFNULL(TRIM(b.COMMODITY), '')) IS DISTINCT FROM
            LOWER(IFNULL(TRIM(n.CMMDTY), ''))
    )
  """, snapshot, snapshot, snapshot);

  -- SYT10
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT10' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.EFFECTIVE_DATE,
            n.SF_ID,
            n.EFF_DT
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
        -- uncomment below to get only those with a true mismatch
            -- b.EFFECTIVE_DATE IS NOT NULL AND
            -- n.EFF_DT IS NOT NULL AND
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(b.EFFECTIVE_DATE, 0, 10)) IS DISTINCT FROM
            DATE(n.EFF_DT)
    )
  """, snapshot, snapshot, snapshot);

  -- SYT11
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT11' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.EXPIRATION_DATE,
            n.SF_ID,
            n.EXP_DT
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
        -- uncomment below to get only those with a true mismatch
            -- b.EXPIRATION_DATE IS NOT NULL AND
            -- n.EXP_DT IS NOT NULL AND
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(b.EXPIRATION_DATE, 0, 10)) IS DISTINCT FROM
            DATE(n.EXP_DT)
    )
  """, snapshot, snapshot, snapshot);

  --SYT12
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT12' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.PRODUCTION_STATUS,
            n.SF_ID,
            n.PRDCNG
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
        -- uncomment below to see true mismatches
            -- b.PRODUCTION_STATUS IS NOT NULL AND
            -- n.PRDCNG IS NOT NULL AND
            LOWER(IFNULL(TRIM(b.PRODUCTION_STATUS), '')) IS DISTINCT FROM
            LOWER(IFNULL(TRIM(n.PRDCNG), ''))
    )
  """, snapshot, snapshot, snapshot);

  -- SYT14
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT14' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.DISPOSITION_DATE,
            n.SF_ID,
            n.CSE_DISP_DT
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
        -- uncomment below to get only those with a true non-null mismatches
            -- b.DISPOSITION_DATE IS NOT NULL AND
            -- n.CSE_DISP_DT IS NOT NULL AND
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(b.DISPOSITION_DATE, 0, 10)) IS DISTINCT FROM
            DATE(n.CSE_DISP_DT)
    )
  """, snapshot, snapshot, snapshot);

  -- SYT15
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT15' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.COST_CENTER_CODE,
            n.SF_ID,
            n.CSE_JURIS_CD
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
        --uncomment to see true non-null mismatches
            -- b.COST_CENTER_CODE IS NOT NULL AND
            -- n.CSE_JURIS_CD IS NOT NULL AND
            IFNULL(b.COST_CENTER_CODE, '-') IS DISTINCT FROM
            IFNULL(n.CSE_JURIS_CD, '-')
    )
  """, snapshot, snapshot, snapshot);

  -- SYT16
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT16' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.BLM_OFFICE_DESCR,
            n.SF_ID,
            n.CSE_JURIS_DESC
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE
        --uncomment to see true non-null mismatches
            -- b.COST_CENTER_CODE IS NOT NULL AND
            -- n.CSE_JURIS_CD IS NOT NULL AND
            LOWER(IFNULL(b.BLM_OFFICE_DESCR, '-')) IS DISTINCT FROM
            LOWER(IFNULL(n.CSE_JURIS_DESC, '-'))
    )
  """, snapshot, snapshot, snapshot);

  -- SYT17
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT17' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.WIDTH,
            n.SF_ID,
            n.CSE_WIDTH,
            ABS(SAFE_CAST(b.WIDTH AS FLOAT64) - SAFE_CAST(n.CSE_WIDTH AS FLOAT64)) AS WIDTH_DIFF
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE SAFE_CAST(b.WIDTH AS FLOAT64) IS DISTINCT FROM SAFE_CAST(n.CSE_WIDTH AS FLOAT64)
    )
  """, snapshot, snapshot, snapshot);

  -- SYT18
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT18' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        SELECT
            b.ID,
            b.SERIAL_NUMBER__C,
            b.LNGTH,
            n.SF_ID,
            n.CSE_LGTH,
            ABS(SAFE_CAST(b.LNGTH AS FLOAT64) - SAFE_CAST(n.CSE_LGTH AS FLOAT64)) AS LNGTH_DIFF
        FROM `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` AS b
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` AS n
        ON b.ID = n.SF_ID
        WHERE SAFE_CAST(b.LNGTH AS FLOAT64) IS DISTINCT FROM SAFE_CAST(n.CSE_LGTH AS FLOAT64)
    )
  """, snapshot, snapshot, snapshot);

  -- SYT19
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT19' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH filtered_actions AS (
            SELECT
            *,
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 0, 10)) AS parsed_dt
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
            WHERE SAFE_CAST(ACTION_CODE AS INT64) = 271
            AND ACTION_DECISION_DT IS NOT NULL
        ),
        latest_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY parsed_dt DESC LIMIT 1)[OFFSET(0)]
            FROM filtered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.ACTION_CODE,
            ca.ACTION_DECISION_DT,
            ca.ACTION_REMARKS,
            nc.PAT_NR
        FROM latest_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            LOWER(IFNULL(SPLIT(ca.ACTION_REMARKS, ';')[OFFSET(0)], '')) IS DISTINCT FROM
            LOWER(IFNULL(nc.PAT_NR, ''))
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT20
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT20' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH filtered_actions AS (
            SELECT
            *,
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 0, 10)) AS parsed_dt
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
            WHERE SAFE_CAST(ACTION_CODE AS INT64) = 271
            AND ACTION_DECISION_DT IS NOT NULL
        ),
        latest_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY parsed_dt DESC LIMIT 1)[OFFSET(0)]
            FROM filtered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.ACTION_CODE,
            ca.ACTION_DECISION_DT,
            nc.PAT_ISS_DT,
            nc.PAT_NR,
            ca.ACTION_REMARKS,
        FROM latest_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ca.ACTION_DECISION_DT, 0, 10)) IS DISTINCT FROM
            DATE(nc.PAT_ISS_DT)
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT21
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT21' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH ordered_actions AS (
            SELECT *
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
        ),
        first_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY MINERAL_SEGREGATION IS NULL ASC LIMIT 1)[OFFSET(0)]
            FROM ordered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.MINERAL_SEGREGATION,
            nc.SEG_MIN
        FROM first_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            LOWER(IFNULL(ca.MINERAL_SEGREGATION, '')) IS DISTINCT FROM LOWER(IFNULL(nc.SEG_MIN, ''))
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT22
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT22' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH ordered_actions AS (
            SELECT *
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
        ),
        first_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY SURFACE_SEGREGATION IS NULL ASC LIMIT 1)[OFFSET(0)]
            FROM ordered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.SURFACE_SEGREGATION,
            nc.SEG_SUR
        FROM first_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            LOWER(IFNULL(ca.SURFACE_SEGREGATION, '')) IS DISTINCT FROM LOWER(IFNULL(nc.SEG_SUR, ''))
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT23
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT23' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH filtered_actions AS (
            SELECT
            *,
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 0, 10)) AS parsed_dt
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
            WHERE SAFE_CAST(ACTION_CODE AS INT64) = 610
            AND ACTION_DECISION_DT IS NOT NULL
        ),
        latest_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY parsed_dt DESC LIMIT 1)[OFFSET(0)]
            FROM filtered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.ACTION_CODE,
            ca.ACTION_DECISION_DT,
            nc.PUB_DT,
        FROM latest_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ca.ACTION_DECISION_DT, 0, 10)) IS DISTINCT FROM
            DATE(nc.PUB_DT)
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT24
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT24' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH filtered_actions AS (
            SELECT
            *,
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 0, 10)) AS parsed_dt
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
            WHERE SAFE_CAST(ACTION_CODE AS INT64) = 610
            -- AND ACTION_DECISION_DT IS NOT NULL
        ),
        latest_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY parsed_dt DESC LIMIT 1)[OFFSET(0)]
            FROM filtered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.ACTION_CODE,
            ca.PUBLICATION_TYPE,
            nc.PUB_TYPE,
            nc.PUB_DT,
            ca.ACTION_DECISION_DT,
        FROM latest_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            LOWER(IFNULL(SPLIT(ca.PUBLICATION_TYPE, ';')[OFFSET(0)], '')) IS DISTINCT FROM
            LOWER(IFNULL(nc.PUB_TYPE, ''))
    )
  """, snapshot, snapshot, snapshot, snapshot);

  -- SYT26
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO row_counts
    SELECT 'SYT26' AS QueryName, '%s' AS Snapshot, COUNT(*) AS RowCount
    FROM (
        WITH filtered_actions AS (
            SELECT
            *,
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ACTION_DECISION_DT, 0, 10)) AS parsed_dt
            FROM `xentity-sandbox-huy.blm_dqimp_qaqc.case_action_%s`
            WHERE SAFE_CAST(ACTION_CODE AS INT64) = 865
            -- AND ACTION_DECISION_DT IS NOT NULL
        ),
        latest_per_case AS (
            SELECT
            AS VALUE ARRAY_AGG(fa ORDER BY parsed_dt DESC LIMIT 1)[OFFSET(0)]
            FROM filtered_actions fa
            GROUP BY fa.BLM_CASE
        )
        SELECT
            bc.ID,
            bc.SERIAL_NUMBER__C,
            ca.ACTION_CODE,
            ca.ACTION_DECISION_DT,
            nc.TITLE_ACC_DT,
        FROM latest_per_case ca
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.blm_case_%s` bc
            ON ca.BLM_CASE = bc.ID
        JOIN `xentity-sandbox-huy.blm_dqimp_qaqc.nlsdb_case_%s` nc
            ON bc.ID = nc.SF_ID
        WHERE
            PARSE_DATE('%%m/%%d/%%Y', SUBSTR(ca.ACTION_DECISION_DT, 0, 10)) IS DISTINCT FROM
            DATE(nc.TITLE_ACC_DT)
    )
  """, snapshot, snapshot, snapshot, snapshot);

  SET i = i + 1;
END WHILE;

-- Step 5: Pivot to wide format
SELECT
  QueryName,
  MAX(IF(Snapshot = '20250201', RowCount, NULL)) AS `20250201`,
  MAX(IF(Snapshot = '20250301', RowCount, NULL)) AS `20250301`,
  MAX(IF(Snapshot = '20250401', RowCount, NULL)) AS `20250401`,
  MAX(IF(Snapshot = '20250501', RowCount, NULL)) AS `20250501`,
FROM row_counts
GROUP BY QueryName;
