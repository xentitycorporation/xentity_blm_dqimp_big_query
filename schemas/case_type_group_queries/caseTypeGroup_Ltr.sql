-- ============================================================================
-- UPDATED 2026-08-10 -- DUAL-CLASSIFICATION RESOLVED. Mark's final call.
-- Removed '256301' (HEADQUARTERS SITE) from this list. It belongs to
-- Land Tenure / Occupancy Use ONLY, as defined in the taxonomy spreadsheet.
--
-- Backed by unanimous per-case evidence: all 2,230 live NLSDB cases for this code
-- read REC_TYPE_CSE_GRP = "Land Tenure - Occupancy and Use", zero exceptions --
-- so unlike 262400 there is no genuine split population and no data cost to this
-- removal. It is picked up by caseTypeGroup_LTe_occupancyuse.sql, which carries an
-- explicit bypass for it (the code's CASE_RECORD_TYPES = 'Land_Transfer' would
-- otherwise exclude it from the Land Tenure side).
--
-- '262400' is deliberately RETAINED here -- that code went the other way on the same
-- day and is now Land Transfer only. See KB §5.7.0.
-- ============================================================================

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE
        CASE_TYPE_CODE IN ('000445', '000449', '007500', '007504', '007509',
        '186500', '254100', '256100', '256800', '256900', '262400',
        '262700', '262710', '262711', '262712', '262713', '262714', '262720', 
        '262722', '262730', '262740', '265008', '265101', '265102', '265199', 
        '265200', '265201', '265202', '265203', '265204', '265206', '265208', 
        '265209', '265210', '265211', '265213', '265301', '265302', '265303', 
        '265304', '265306', '265308', '265311', '265318', '265400', '265501', 
        '265502', '265600', '265800') 
        AND CASE_RECORD_TYPES LIKE 'Land_Transfer%%'
    ORDER BY NAME
""", snapshot_date);
