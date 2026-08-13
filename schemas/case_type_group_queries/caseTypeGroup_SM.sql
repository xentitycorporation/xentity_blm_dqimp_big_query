-- EXCLUSION-LIST CHANGE 2026-08-11: removed '380800' from the NOT IN list below.
--
-- 380800 "ABANDONED MINE LAND INV" is new (first appears 20260802) and the SME assigned it
-- to Solid Minerals / General Mining Laws on 2026-08-11. It was sitting in this group-level
-- exclusion list, so the group query disagreed with the taxonomy: the lookup table
-- classified its 2 cases as Solid Minerals while this query dropped them.
--
-- This is the exact failure mode KB Sec 5.7.0 exists to catch -- an exclusion list is
-- invisible in the taxonomy spreadsheet and can silently drop a code the spreadsheet says
-- belongs to the group. Logged here per that standing requirement.
--
-- The other 17 exclusions were checked against the taxonomy at the same time and are all
-- correct: 384101-384403 are Mining Claims, 386200-386501 are Land Tenure, and 383300 /
-- 386000 have no taxonomy row at all. 380800 was the only conflict.
DECLARE snapshot_date STRING DEFAULT '20250901';

EXECUTE IMMEDIATE FORMAT("""
    SELECT DISTINCT
        NAME,
        CASE_TYPE_CODE
    FROM `blm_seta_dqimp.blm_product_%s`
    WHERE (
        CASE_TYPE_CODE LIKE '34%%' 
        OR CASE_TYPE_CODE LIKE '35%%' 
        OR CASE_TYPE_CODE LIKE '36%%' 
        OR CASE_TYPE_CODE LIKE '37%%' 
        OR CASE_TYPE_CODE LIKE '38%%' 
        OR CASE_TYPE_CODE LIKE '39%%'
    ) 
    AND CASE_TYPE_CODE NOT IN (
        '383300', '384101', '384103', '384201', '384203',
        '384301', '384303', '384401', '384403', '386000', '386200',
        '386210', '386300', '386310', '386400', '386403', '386501'
    )
    ORDER BY NAME
""", snapshot_date);