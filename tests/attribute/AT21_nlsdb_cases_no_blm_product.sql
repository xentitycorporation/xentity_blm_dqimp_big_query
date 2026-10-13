-- =====================================================================================
-- Test ID:      AT21
-- Test name:    NLSDB cases with no matching BLM Product
-- Test type:    Attribute (tests/attribute/) - missing or incorrect information in one system, on a single snapshot
-- Catalog:      Test Catalog v2 row #111 (DQIMP_GitRepo_Test_Index_Update.xlsx)
-- Replaces:     none (new test)
-- Produces:     NLSDB_Cases_No_BLM_Product_Code_<snapshot_date>.xlsx, sheet "NLSDB Cases"
-- Parameters:   snapshot_date (YYYYMMDD)
-- Conventions:  shared conventions S1-S6 in tests/README_newTests.md
-- Validated:    re-run on 20260802 reproduced the delivered spreadsheet value for value; run on 20261004 on 2026-10-09
-- Added:        2026-10-13
-- =====================================================================================
-- Lists every NLSDB case whose CSE_TYPE_NR has no corresponding CASE_TYPE_CODE in blm_product
-- for the same snapshot -- i.e. legacy/NLSDB-only case types with no current BLM Product Code,
-- out of scope for the Case Type Group/Subgroup taxonomy, since that taxonomy is built on
-- blm_product.CASE_TYPE_CODE.
--
-- STANDARDISED 2026-10-09 (tests/README_newTests.md): fully-qualified table names; output columns renamed
-- to the spreadsheet headers; PART 2 added for the title line (case count, code count).
-- NLSDB-only test, so S1 (real MLRS cases) does not apply. The comparison stays a STRING
-- compare on purpose: an NLSDB '7502' against an MLRS '007502' is a genuine disagreement.
--
-- FIXED 2026-08-17 -- whitespace-only CSE_TYPE_NR was being counted as a product code (one
-- single-space pseudo-code carrying 8 cases). TRIM() before the empty-string test. Those rows
-- belong with the NULL/blank population, not the NLSDB-specific types.
--     20260802: 35 codes / 9,159 cases.
--
-- Edit only `snapshot_date`.
-- =====================================================================================

DECLARE snapshot_date STRING DEFAULT '20261004';                          -- <<< CHANGE THIS

-- PART 1 -- THE LIST
EXECUTE IMMEDIATE FORMAT("""
    SELECT
        n.CSE_NR           AS Serial_Number,
        n.CSE_DISP         AS Disposition,
        n.SF_ID            AS NLSDB_ID,
        n.CSE_TYPE_NR      AS Case_Type_Code,
        n.REC_TYPE_CSE_GRP AS Record_Type_Case_Group
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` n
    WHERE n.CSE_TYPE_NR IS NOT NULL
      AND TRIM(n.CSE_TYPE_NR) != ''
      AND n.CSE_TYPE_NR NOT IN (
        SELECT DISTINCT CASE_TYPE_CODE
        FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
        WHERE CASE_TYPE_CODE IS NOT NULL
      )
    ORDER BY n.CSE_TYPE_NR, n.CSE_NR
""", snapshot_date, snapshot_date);

-- PART 2 -- TITLE-LINE COUNTS
EXECUTE IMMEDIATE FORMAT("""
    SELECT COUNT(*) AS Cases, COUNT(DISTINCT n.CSE_TYPE_NR) AS Case_Type_Codes
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` n
    WHERE n.CSE_TYPE_NR IS NOT NULL
      AND TRIM(n.CSE_TYPE_NR) != ''
      AND n.CSE_TYPE_NR NOT IN (
        SELECT DISTINCT CASE_TYPE_CODE
        FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
        WHERE CASE_TYPE_CODE IS NOT NULL
      )
""", snapshot_date, snapshot_date);
