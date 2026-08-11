-- FIXED 2026-07-24: 'land transfer%%' corrected to 'land_transfer%%' -- the field format is always underscore (Land_Transfer), space never occurs in the data (0 vs 46 rows, verified against blm_product_20260705). Original filter was a no-op. Confirmed and applied.
-- UPDATED 2026-07-30: added an explicit bypass for 256301 (Headquarters Site). It
-- matches the '25%' pattern but its CASE_RECORD_TYPES = 'Land_Transfer', so it was being
-- silently excluded despite the spreadsheet assigning it to Land Tenure / Occupancy Use --
-- confirmed by a per-case check that 100% of its 2,230 live NLSDB cases carry
-- REC_TYPE_CSE_GRP = "Land Tenure - Occupancy and Use" with zero exceptions. Mirrors the
-- same unconditioned-bypass pattern caseTypeGroup_LTe.sql itself already uses for this code.

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE
        CASE_TYPE_CODE IN ('254100', '256301')
        OR (
            (
                CASE_TYPE_CODE LIKE '25%%'
            )
            AND (LOWER(CASE_RECORD_TYPES) NOT LIKE 'land_transfer%%' OR CASE_RECORD_TYPES IS NULL)
        )
    ORDER BY NAME
""", snapshot_date);
