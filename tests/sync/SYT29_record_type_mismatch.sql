-- =====================================================================================
-- Test ID:      SYT29
-- Test name:    Record type mismatch (client deliverable)
-- Test type:    Synchronization (tests/sync/) - a case in MLRS is also in NLSDB (and vice versa) and carries the same value in both
-- Catalog:      Test Catalog v2 row #110 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     SVT_RecordType_MLRS_NLSDB_Mismatch_A
-- Produces:     RecordType_MLRS_NLSDB_Mismatch_<snapshot_date>.xlsx, sheet "Record Type"
-- Parameters:   snapshot_date (YYYYMMDD), lookup_table
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- MLRS:  record_type.NAME (18-char truncation restored via CASE WHEN)
-- NLSDB: prefix of REC_TYPE_CSE_GRP (text before first " - " or "-")
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md):
--   S1 Real MLRS cases only -- Bond and Status record types now excluded (previously not).
--      On 20260802 two Status records joined to NLSDB, so at most 2 rows can change.
--   S3 blm_product dedup tie-broken on NAME so it is deterministic.
--   S4 Unclassified rows labelled '(Unclassified)' (previously blank).
--
-- UPDATED 2026-08-17: lookup repointed; taxonomy join SAFE_CAST(... AS INT64) both sides;
--   NLSDB serial, both IDs, both dispositions added; untrimmed-display bug fixed by
--   normalising ONCE in a CTE reused by both SELECT and WHERE.
--
-- Known: 12 NLSDB-only prefixes (e.g. TELEGRAPH, Coos Bay Wagon Road) match no MLRS
-- record type and always surface here. Expected, not noise.
--
-- '\\n' ESCAPING: inside EXECUTE IMMEDIATE FORMAT("""...""") the newline must be written
-- '\\n' so the generated SQL contains '\n'. Writing '\n' here breaks the string literal.
--
-- Edit only `snapshot_date`.
-- =========================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS
DECLARE lookup_table  STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

EXECUTE IMMEDIATE FORMAT("""
  WITH dedup_record_type AS (
    SELECT ID, NAME FROM (
      SELECT ID, NAME, ROW_NUMBER() OVER (PARTITION BY ID ORDER BY NAME) AS rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.record_type_%s`
    ) WHERE rn = 1
  ),
  dedup_product AS (
    SELECT ID, CASE_TYPE_CODE FROM (
      SELECT ID, CASE_TYPE_CODE,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE, NAME) AS rn
      FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
    ) WHERE rn = 1
  ),
  taxonomy AS (
    SELECT DISTINCT SAFE_CAST(`BLM Product Code` AS INT64) AS product_code,
      `Case Type Group` AS case_type_group, `Case Type Subgroup` AS case_type_subgroup
    FROM `%s`
  ),
  -- Normalise both sides ONCE, then reuse.
  normalised AS (
    SELECT
      b.SERIAL_NUMBER__C AS mlrs_serial,
      n.CSE_NR           AS nlsdb_serial,
      b.ID               AS mlrs_id,
      n.SF_ID            AS nlsdb_sf_id,
      b.CASE_STATUS      AS mlrs_disp,
      n.CSE_DISP         AS nlsdb_disp,
      dp.CASE_TYPE_CODE  AS product_code,
      TRIM(CASE rt.NAME
        WHEN 'Land Use Authoriza' THEN 'Land Use Authorizations'
        WHEN 'Compensatory Royal' THEN 'Compensatory Royalty Agreement'
        WHEN 'Unleased Lands Acc' THEN 'Unleased Lands Account'
        ELSE rt.NAME
      END) AS mlrs_record_type,
      TRIM(CASE
        WHEN STRPOS(REPLACE(n.REC_TYPE_CSE_GRP, '\\n', ''), ' - ') > 0
          THEN SUBSTR(REPLACE(n.REC_TYPE_CSE_GRP, '\\n', ''), 1,
                      STRPOS(REPLACE(n.REC_TYPE_CSE_GRP, '\\n', ''), ' - ') - 1)
        WHEN STRPOS(REPLACE(n.REC_TYPE_CSE_GRP, '\\n', ''), '-') > 0
          THEN SUBSTR(REPLACE(n.REC_TYPE_CSE_GRP, '\\n', ''), 1,
                      STRPOS(REPLACE(n.REC_TYPE_CSE_GRP, '\\n', ''), '-') - 1)
        ELSE REPLACE(n.REC_TYPE_CSE_GRP, '\\n', '')
      END) AS nlsdb_record_type
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` b
    JOIN `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` n
      ON b.ID = n.SF_ID
    LEFT JOIN dedup_record_type rt ON b.RECORDTYPEID = rt.ID
    LEFT JOIN dedup_product dp     ON b.BLM_PRODUCT  = dp.ID
    WHERE b.RECORDTYPEID NOT IN ('0123d0000004QFQAA2', '0123d0000005ISFAA2')
  )
  SELECT
    IFNULL(tx.case_type_group, '(Unclassified)') AS Case_Type_Group,
    CASE WHEN tx.case_type_group IS NOT NULL THEN tx.case_type_subgroup
         WHEN x.product_code IS NOT NULL
           THEN CONCAT('BLM Product Code ', x.product_code, ' (not in taxonomy)')
         ELSE '(no case type code / not in blm_product)' END AS Case_Type_Subgroup,
    x.mlrs_serial           AS MLRS_Serial_Number,
    x.nlsdb_serial          AS NLSDB_Serial_Number,
    x.mlrs_id               AS MLRS_ID,
    x.nlsdb_sf_id           AS NLSDB_SF_ID,
    x.mlrs_disp             AS MLRS_Case_Disposition,
    x.nlsdb_disp            AS NLSDB_Case_Disposition,
    x.mlrs_record_type      AS MLRS_Record_Type,
    x.nlsdb_record_type     AS NLSDB_Record_Type
  FROM normalised x
  LEFT JOIN taxonomy tx
    ON SAFE_CAST(x.product_code AS INT64) = tx.product_code
  WHERE LOWER(IFNULL(x.mlrs_record_type, ''))
    IS DISTINCT FROM
    LOWER(IFNULL(x.nlsdb_record_type, ''))
  ORDER BY Case_Type_Group, Case_Type_Subgroup, MLRS_Serial_Number
""", snapshot_date, snapshot_date, lookup_table, snapshot_date, snapshot_date);
