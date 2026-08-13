-- Solid Minerals / General Mining Laws -- new subgroup, client (SME) decision 2026-08-11.
--
-- Holds exactly one code: 380800 "ABANDONED MINE LAND INV".
--
-- WHY A SUBGROUP OF ITS OWN, FOR ONE CODE:
-- 380800 is new -- it first appears in blm_product_20260802 and in no snapshot before it
-- (2 cases, both Utah, both PENDING, created 2026-07-21). The SME placed it in Solid
-- Minerals / General Mining Laws deliberately, and said to KEEP IT ISOLATED for now
-- because she does not believe this product code should be appearing in MLRS at all --
-- but it plainly is. Holding it in its own subgroup keeps those cases visible instead of
-- absorbing them into a larger bucket where the anomaly would stop being noticeable.
--
-- So the single-code subgroup is the point, not an oversight. Do NOT "tidy" this by
-- folding 380800 into Locatables (where the other seven 380xxx codes sit) or into
-- Saleables (which its CASE_RECORD_TYPES = 'Solid_Mineral_Materials' would suggest)
-- without a new SME decision.
--
-- Corroborating evidence, for whoever revisits this:
--   * NLSDB already reports REC_TYPE_CSE_GRP = "Solid Minerals-General Mining Laws"
--   * MLRS blm_product.CASE_GROUP_DESCR    = "General Mining Laws", CASE_GROUP = 38
--   Both systems independently agree on the group; only the subgroup was ever in question.
--   Full briefing: DQIMP\Case_Type_Grouping\New_Product_Code_380800_Briefing.sql
--
-- Adding this file closes the gap verify_case_type_group_coverage.py reported on
-- 2026-08-11: the taxonomy CSV classified 380800, but no subgroup query claimed it, so
-- lookup-driven and query-driven reporting disagreed.
--
-- NOTE: 380800 returns zero rows on any snapshot before 20260802. That is expected for a
-- new code, not a broken query.
DECLARE snapshot_date STRING DEFAULT '20260802';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '380800'
    )
    ORDER BY NAME
""", snapshot_date);
