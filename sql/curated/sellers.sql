SELECT
    seller_id,
    LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0') AS seller_zip_code_prefix,
    INITCAP(TRIM(seller_city))                           AS seller_city,
    UPPER(TRIM(seller_state))                            AS seller_state,
    @batch_id                                            AS batch_id,
    CURRENT_TIMESTAMP()                                  AS load_date,
    'olist_sellers_dataset.csv'                          AS source_file
FROM `{project}.raw.sellers`
WHERE seller_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) = 1
