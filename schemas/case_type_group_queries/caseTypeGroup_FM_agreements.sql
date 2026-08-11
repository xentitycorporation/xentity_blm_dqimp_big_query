-- UPDATED 2026-08-10: 328300 (COMMUNITIZATION AGREEMENT) added to the IN-list.
-- The client decision of 2026-07 (KB §5.1.3) stands and was recorded correctly; what was
-- wrong were the subgroup VALUES typed into the taxonomy spreadsheet, which read
-- "Agreement" (singular) and so appeared as a separate subgroup. Mark corrected the
-- spreadsheet on 2026-08-10: there is one "Agreements" subgroup, now 21 codes.
-- caseTypeGroup_FM_agreement.sql is superseded.
-- Note: 328300 is not present in blm_product_20260802, so this addition changes no current
-- result. It is carried because the taxonomy is a comprehensive historical code list.

DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE CASE_TYPE_CODE IN (
        '315100', '318110', '318120', '318130', '318210', '318220',
        '318240', '318250', '318260', '318290', '318310', '318320',
        '318330', '318410', '318510', '320901', '328110', '328120',
        '328200', '328210', '328240', '328300', '328500', '313700',
        '313710', '315200'
    )
    ORDER BY NAME
""", snapshot_date);