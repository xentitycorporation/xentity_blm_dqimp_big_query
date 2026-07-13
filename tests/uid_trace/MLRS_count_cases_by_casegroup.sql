DECLARE snapshot_date STRING DEFAULT '20250901';
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_with_Case_Type_Subgroups';

EXECUTE IMMEDIATE FORMAT("""
    WITH dedup_product AS (
        -- Deduplicates the product table to ensure a 1-to-1 join
        SELECT ID, CASE_TYPE_CODE
        FROM (
            SELECT 
                ID,
                CASE_TYPE_CODE,
                ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CASE_TYPE_CODE) AS rn
            FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_product_%s`
        )
        WHERE rn = 1
    ),
    group_lookup AS (
        -- Extracts the distinct mapping of product codes to case groups
        SELECT DISTINCT
            CAST(`BLM Product Code` AS STRING) AS product_code,
            `Case Type Group`,
            `Case Type Subgroup`
        FROM `%s`
    )
    
    -- Main query: Joins the dynamically dated case table to the product and group lookups
    SELECT
        lk.`Case Type Group` AS case_type_group,
        lk.`Case Type Subgroup` AS case_type_subgroup,
        COUNT(DISTINCT bc.SERIAL_NUMBER__C) AS unique_case_count
    FROM `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS bc
    LEFT JOIN dedup_product AS dp 
        ON bc.BLM_PRODUCT = dp.ID
    LEFT JOIN group_lookup AS lk 
        ON dp.CASE_TYPE_CODE = lk.product_code
    WHERE 
        bc.CASE_STATUS != 'STATUS RECORD'
    GROUP BY 
        case_type_group, 
        case_type_subgroup
    ORDER BY 
        case_type_group ASC, 
        unique_case_count DESC;
""", snapshot_date, lookup_table, snapshot_date);