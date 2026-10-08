DECLARE snapshot_date STRING DEFAULT '20260802';
DECLARE lookup_table STRING DEFAULT 'xentity-sandbox-huy.blm_seta_dqimp.Product_Code_Case_Type_Group_Subgroup';

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
            -- Both sides of the product-code join are cast to INT64. The lookup column is STRING
            -- today, but a reload with schema auto-detect turns it INTEGER and strips leading
            -- zeros ('007500' -> 7500); a text join would then silently drop those codes.
            SAFE_CAST(`BLM Product Code` AS INT64) AS product_code,
            `Case Type Group`,
            `Case Type Subgroup`
        FROM `%s`
    )
    
    -- Main query: Counts NLSDB cases by linking through the BLM case table to get product types
    SELECT
        lk.`Case Type Group` AS case_type_group,
        lk.`Case Type Subgroup` AS case_type_subgroup,
        COUNT(DISTINCT nc.CSE_NR) AS unique_case_count
    FROM `xentity-sandbox-huy.blm_seta_dqimp.nlsdb_case_%s` AS nc
    LEFT JOIN `xentity-sandbox-huy.blm_seta_dqimp.blm_case_%s` AS bc
        ON nc.CSE_NR = bc.SERIAL_NUMBER__C
    LEFT JOIN dedup_product AS dp 
        ON bc.BLM_PRODUCT = dp.ID
    LEFT JOIN group_lookup AS lk 
        ON SAFE_CAST(dp.CASE_TYPE_CODE AS INT64) = lk.product_code
    GROUP BY 
        case_type_group, 
        case_type_subgroup
    ORDER BY 
        case_type_group ASC, 
        unique_case_count DESC;
""", snapshot_date, lookup_table, snapshot_date, snapshot_date);