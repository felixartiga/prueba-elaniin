SELECT
    customer_id,
    customer_unique_id,
    LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') AS customer_zip_code_prefix,
    INITCAP(TRIM(customer_city))                           AS customer_city,
    UPPER(TRIM(customer_state))                            AS customer_state,
    @batch_id                                              AS batch_id,
    CURRENT_TIMESTAMP()                                    AS load_date,
    'olist_customers_dataset.csv'                          AS source_file
FROM `{project}.raw.customers`
WHERE customer_id IS NOT NULL AND customer_unique_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) = 1
