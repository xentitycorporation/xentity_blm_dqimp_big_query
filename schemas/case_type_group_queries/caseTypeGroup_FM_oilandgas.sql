-- UPDATED 2026-08-10 -- EXCLUSION LIST CHANGE: '311131' and '312081' REMOVED from the
-- NOT IN list, so they now fall inside Oil/Gas Leases via the '31%' pattern.
-- The client decision of 2026-07 (KB §5.1.3) stands and was recorded correctly; what was
-- wrong were the subgroup VALUES typed into the taxonomy spreadsheet, which read
-- "Oil/Gas Lease" (singular) and so appeared as a separate subgroup. Mark corrected the
-- spreadsheet on 2026-08-10: there is one "Oil/Gas Leases" subgroup, now 34 codes.
-- caseTypeGroup_FM_oilgaslease.sql is superseded.
-- Note: neither code is present in blm_product_20260802, so this removal changes no current
-- result. Paper trail for the exclusion-list change: KB §5.7.

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (CASE_TYPE_CODE LIKE '31%%')
      AND CASE_TYPE_CODE NOT IN ('310070', '313700', '313710', '315100', '315200', '318110', '318120', '318130', '318210', '318220', '318230', '318231', '318232', '318240', '318250', '318260', '318270', '318290', '318291', '318310', '318320', '318330', '318410', '318510', '310220', '312020', '313100', '313110', '313140', '313141', '313240')
    ORDER BY NAME
""", snapshot_date);
