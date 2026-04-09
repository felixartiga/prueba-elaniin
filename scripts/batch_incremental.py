import argparse
import logging
import os
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Clave incremental: order_purchase_timestamp
# Si no hay batch previo, se usa este cutoff por defecto
DEFAULT_CUTOFF = "2018-07-01 00:00:00"


def get_last_watermark(client, project):
    sql = f"""
        SELECT watermark_to FROM `{project}.ops.batch_logs`
        WHERE status = 'SUCCESS' AND watermark_to IS NOT NULL
        ORDER BY finished_at DESC LIMIT 1
    """
    try:
        rows = list(client.query(sql).result())
        if rows and rows[0].watermark_to:
            return rows[0].watermark_to.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return None


def get_max_timestamp(client, project):
    rows = list(client.query(f"""
        SELECT MAX(SAFE_CAST(order_purchase_timestamp AS TIMESTAMP)) as max_ts
        FROM `{project}.raw.orders`
    """).result())
    return rows[0].max_ts.strftime("%Y-%m-%d %H:%M:%S") if rows[0].max_ts else None


def merge_orders(client, project, batch_id, watermark_from):
    log.info("orders — MERGE desde %s", watermark_from)

    delta_count = list(client.query(f"""
        SELECT COUNT(*) as n FROM `{project}.raw.orders`
        WHERE SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{watermark_from}')
        AND order_id IS NOT NULL
    """).result())[0].n

    if delta_count == 0:
        log.info("Sin órdenes nuevas.")
        return {"table_name": "orders", "raw_count": 0, "new_count": 0, "updated_count": 0}

    updated_count = list(client.query(f"""
        SELECT COUNT(*) as n FROM `{project}.raw.orders` r
        INNER JOIN `{project}.curated.orders` c USING (order_id)
        WHERE SAFE_CAST(r.order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{watermark_from}')
    """).result())[0].n
    new_count = delta_count - updated_count

    client.query(f"""
        MERGE `{project}.curated.orders` T
        USING (
            SELECT
                order_id, customer_id,
                UPPER(TRIM(order_status)) AS order_status,
                SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
                SAFE_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
                SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
                SAFE_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
                SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
                '{batch_id}' AS batch_id,
                CURRENT_TIMESTAMP() AS load_date,
                'olist_orders_dataset.csv' AS source_file
            FROM `{project}.raw.orders`
            WHERE SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{watermark_from}')
            AND order_id IS NOT NULL AND customer_id IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) DESC) = 1
        ) S ON T.order_id = S.order_id
        WHEN MATCHED THEN UPDATE SET
            order_status = S.order_status,
            order_approved_at = S.order_approved_at,
            order_delivered_carrier_date = S.order_delivered_carrier_date,
            order_delivered_customer_date = S.order_delivered_customer_date,
            order_estimated_delivery_date = S.order_estimated_delivery_date,
            batch_id = S.batch_id, load_date = S.load_date
        WHEN NOT MATCHED THEN INSERT VALUES (
            S.order_id, S.customer_id, S.order_status, S.order_purchase_timestamp,
            S.order_approved_at, S.order_delivered_carrier_date, S.order_delivered_customer_date,
            S.order_estimated_delivery_date, S.batch_id, S.load_date, S.source_file
        )
    """).result()

    log.info("  nuevas=%d actualizadas=%d", new_count, updated_count)
    return {"table_name": "orders", "raw_count": delta_count, "new_count": new_count, "updated_count": updated_count}


def insert_related(client, project, batch_id, table, pk, columns, select_sql):
    log.info("%s — INSERT nuevos", table)
    cols_str = ", ".join(columns)
    client.query(f"""
        MERGE `{project}.curated.{table}` T
        USING ({select_sql}) S ON T.{pk} = S.{pk}
        WHEN NOT MATCHED THEN INSERT ({cols_str}) VALUES ({', '.join(f'S.{c}' for c in columns)})
    """).result()
    new_count = list(client.query(f"SELECT COUNT(*) as n FROM `{project}.curated.{table}` WHERE batch_id = '{batch_id}'").result())[0].n
    log.info("  nuevos=%d", new_count)
    return {"table_name": table, "raw_count": new_count, "new_count": new_count, "updated_count": 0}


def write_batch_log(client, project, batch_id, status, started_at, finished_at, watermark_from, watermark_to, tables_processed, total_rows, error_msg=None):
    client.insert_rows_json(f"{project}.ops.batch_logs", [{
        "batch_id": batch_id, "batch_type": "INCREMENTAL", "status": status,
        "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat(),
        "watermark_from": watermark_from, "watermark_to": watermark_to,
        "tables_processed": tables_processed, "total_rows_loaded": total_rows, "error_message": error_msg,
    }])


def write_table_metrics(client, project, batch_id, metrics_list):
    client.insert_rows_json(f"{project}.ops.table_metrics", [{
        "batch_id": batch_id, "table_name": m["table_name"],
        "source_file": f"olist_{m['table_name']}_dataset.csv",
        "raw_row_count": m["raw_count"],
        "loaded_row_count": m.get("new_count", 0) + m.get("updated_count", 0),
        "rejected_row_count": 0,
        "new_row_count": m.get("new_count", 0),
        "updated_row_count": m.get("updated_count", 0),
        "created_at": datetime.now(timezone.utc).isoformat(),
    } for m in metrics_list])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    parser.add_argument("--cutoff", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.project:
        raise ValueError("GCP_PROJECT no definido.")

    client = bigquery.Client(project=args.project)
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    watermark_from = f"{args.cutoff} 00:00:00" if args.cutoff else get_last_watermark(client, args.project) or DEFAULT_CUTOFF
    watermark_to = get_max_timestamp(client, args.project)

    log.info("Batch INCREMENTAL — batch_id=%s | from=%s | to=%s", batch_id, watermark_from, watermark_to)

    metrics_list = []
    status = "SUCCESS"
    error_msg = None

    try:
        metrics_list.append(merge_orders(client, args.project, batch_id, watermark_from))

        p = args.project
        wf = watermark_from

        metrics_list.append(insert_related(client, p, batch_id, "order_items", "order_id",
            ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value", "total_value", "batch_id", "load_date", "source_file"],
            f"""SELECT oi.order_id, CAST(oi.order_item_id AS INT64), oi.product_id, oi.seller_id,
                SAFE_CAST(oi.shipping_limit_date AS TIMESTAMP),
                ROUND(CAST(oi.price AS FLOAT64), 2), ROUND(CAST(oi.freight_value AS FLOAT64), 2),
                ROUND(CAST(oi.price AS FLOAT64) + CAST(oi.freight_value AS FLOAT64), 2),
                '{batch_id}', CURRENT_TIMESTAMP(), 'olist_order_items_dataset.csv'
                FROM `{p}.raw.order_items` oi
                INNER JOIN `{p}.raw.orders` o USING (order_id)
                WHERE SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{wf}')
                AND oi.order_id IS NOT NULL AND oi.price IS NOT NULL"""))

        metrics_list.append(insert_related(client, p, batch_id, "order_payments", "order_id",
            ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value", "batch_id", "load_date", "source_file"],
            f"""SELECT op.order_id, CAST(op.payment_sequential AS INT64), UPPER(TRIM(op.payment_type)),
                CAST(op.payment_installments AS INT64), ROUND(CAST(op.payment_value AS FLOAT64), 2),
                '{batch_id}', CURRENT_TIMESTAMP(), 'olist_order_payments_dataset.csv'
                FROM `{p}.raw.order_payments` op
                INNER JOIN `{p}.raw.orders` o USING (order_id)
                WHERE SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{wf}')
                AND op.order_id IS NOT NULL AND op.payment_value IS NOT NULL"""))

        metrics_list.append(insert_related(client, p, batch_id, "order_reviews", "review_id",
            ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp", "batch_id", "load_date", "source_file"],
            f"""SELECT r.review_id, r.order_id, CAST(r.review_score AS INT64),
                NULLIF(TRIM(r.review_comment_title), ''), NULLIF(TRIM(r.review_comment_message), ''),
                SAFE_CAST(r.review_creation_date AS TIMESTAMP), SAFE_CAST(r.review_answer_timestamp AS TIMESTAMP),
                '{batch_id}', CURRENT_TIMESTAMP(), 'olist_order_reviews_dataset.csv'
                FROM `{p}.raw.order_reviews` r
                INNER JOIN `{p}.raw.orders` o USING (order_id)
                WHERE SAFE_CAST(o.order_purchase_timestamp AS TIMESTAMP) > TIMESTAMP('{wf}')
                AND r.review_id IS NOT NULL AND CAST(r.review_score AS INT64) BETWEEN 1 AND 5
                QUALIFY ROW_NUMBER() OVER (PARTITION BY r.review_id ORDER BY SAFE_CAST(r.review_answer_timestamp AS TIMESTAMP) DESC) = 1"""))

    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        log.error("Pipeline fallido: %s", exc)
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        total_rows = sum(m.get("new_count", 0) + m.get("updated_count", 0) for m in metrics_list)
        if metrics_list:
            write_table_metrics(client, args.project, batch_id, metrics_list)
        write_batch_log(client, args.project, batch_id, status, started_at, finished_at,
                        watermark_from, watermark_to, len(metrics_list), total_rows, error_msg)

    log.info("Status=%s | Filas=%d | Duración=%.1fs", status, total_rows, (finished_at - started_at).total_seconds())


if __name__ == "__main__":
    main()
