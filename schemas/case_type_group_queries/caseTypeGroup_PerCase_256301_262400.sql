-- Per-case classification for 256301 (Headquarters Site) and 262400 (School Select
-- Patents), honoring how BLM's own AFS ArcGIS services actually treat these two codes:
-- not one fixed Group per code, but per-CASE routing based on the case's own NLSDB
-- REC_TYPE_CSE_GRP value. Built 2026-07-30 following a Case Type Groups.xlsx /
-- MLRS_Web_Service_Information_20240723.xlsx review that found these codes appear,
-- unconditioned, in a Land Tenure hardcoded IN-list AND, conditioned on
-- REC_TYPE_CSE_GRP / CSE_LND_STATUS, across multiple dedicated Land Transfer services.
--
-- Why REC_TYPE_CSE_GRP and not CASE_RECORD_TYPES: blm_product.CASE_RECORD_TYPES is
-- fixed per PRODUCT CODE (one value for every case sharing a code), so it structurally
-- cannot drive per-case routing. nlsdb_case.REC_TYPE_CSE_GRP is a genuine per-CASE
-- field and already *is* BLM's final classification text -- no need to reverse-engineer
-- CSE_LND_STATUS-based map-layer logic when we can just read the answer directly.
--
-- Coverage confirmed 2026-07-30 (20260705): every real (non-Status-Record, non-Bond)
-- MLRS case for these 2 codes has a matching NLSDB record via ID = SF_ID -- 0 unmatched.
-- Results: 256301 is unanimous Land Tenure / Occupancy and Use (2,230 of 2,230 cases --
-- despite appearing in Land Transfer's ArcGIS queries, the per-case data shows no real
-- Land Transfer population for this code). 262400 is genuinely mixed: 238 of 292 (81.5%)
-- Land Transfer / Grants, 54 of 292 (18.5%) Land Tenure / Grants.

DECLARE snapshot_date STRING DEFAULT '20260705';

EXECUTE IMMEDIATE FORMAT("""
  WITH dedup_product AS (
    SELECT ID, CASE_TYPE_CODE FROM (
      SELECT ID, CASE_TYPE_CODE, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    ) WHERE rn = 1
  ),
  real_cases AS (
    SELECT b.ID, b.SERIAL_NUMBER__C, b.CASE_STATUS, dp.CASE_TYPE_CODE
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS b
    JOIN dedup_product AS dp ON b.BLM_PRODUCT = dp.ID
    WHERE dp.CASE_TYPE_CODE IN ('256301', '262400')
      AND b.RECORDTYPEID NOT IN ('0123d0000005ISFAA2', '0123d0000004QFQAA2')  -- Status, Bond
  ),
  nlsdb_classified AS (
    SELECT
      SF_ID, CSE_NR, CSE_DISP,
      REPLACE(TRIM(REC_TYPE_CSE_GRP), '\\n', '') AS rec_type_cse_grp,
      TRIM(SPLIT(REGEXP_REPLACE(REPLACE(TRIM(REC_TYPE_CSE_GRP), '\\n', ''), r'\\s*-\\s*', '-'), '-')[SAFE_OFFSET(0)]) AS case_type_group,
      TRIM(SPLIT(REGEXP_REPLACE(REPLACE(TRIM(REC_TYPE_CSE_GRP), '\\n', ''), r'\\s*-\\s*', '-'), '-')[SAFE_OFFSET(1)]) AS case_type_subgroup
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s`
    WHERE CSE_TYPE_NR IN ('256301', '262400')
  )
  SELECT
    rc.ID AS case_id,
    rc.SERIAL_NUMBER__C AS mlrs_serial_number,
    rc.CASE_STATUS AS mlrs_disposition,
    rc.CASE_TYPE_CODE AS product_code,
    nc.CSE_NR AS nlsdb_serial_number,
    nc.CSE_DISP AS nlsdb_disposition,
    nc.case_type_group,
    nc.case_type_subgroup
  FROM real_cases AS rc
  LEFT JOIN nlsdb_classified AS nc ON rc.ID = nc.SF_ID
  ORDER BY product_code, case_id
""", snapshot_date, snapshot_date, snapshot_date);
