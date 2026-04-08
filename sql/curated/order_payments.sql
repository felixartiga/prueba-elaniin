SELECT
    order_id,
    CAST(payment_sequential AS INT64)        AS payment_sequential,
    UPPER(TRIM(payment_type))                AS payment_type,
    CAST(payment_installments AS INT64)      AS payment_installments,
    ROUND(CAST(payment_value AS FLOAT64), 2) AS payment_value,
    @batch_id                                AS batch_id,
    CURRENT_TIMESTAMP()                      AS load_date,
    'olist_order_payments_dataset.csv'       AS source_file
FROM `{project}.raw.order_payments`
WHERE order_id IS NOT NULL AND payment_type IS NOT NULL AND payment_value IS NOT NULL AND CAST(payment_value AS FLOAT64) >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, payment_sequential ORDER BY CAST(payment_value AS FLOAT64) DESC) = 1
