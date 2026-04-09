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


def run_check(client, project, check_name, table_name, severity, sql):
    row = list(client.query(sql).result())[0]
    total, failed = row.total, row.failed or 0
    pass_rate = round(1.0 - (failed / total), 4) if total > 0 else 1.0
    status = "PASS" if failed == 0 else ("FAIL" if severity == "ERROR" else "WARNING")
    log.info("[%s] %s — %d/%d fallos (%.2f%%)", status, check_name, failed, total, pass_rate * 100)
    return {
        "check_name": check_name,
        "table_name": table_name,
        "severity": severity,
        "total_checked": total,
        "failed_count": failed,
        "pass_rate": pass_rate,
        "status": status,
        "details": f"{failed} registros fallaron el control.",
    }


def get_checks(project):
    p = project
    valid_statuses = "'DELIVERED','SHIPPED','CANCELED','UNAVAILABLE','INVOICED','PROCESSING','CREATED','APPROVED'"
    return [
        ("null_mandatory_fields_orders", "curated.orders", "ERROR",
         f"SELECT COUNT(*) AS total, COUNTIF(order_id IS NULL OR customer_id IS NULL OR order_status IS NULL) AS failed FROM `{p}.curated.orders`"),

        ("invalid_order_status", "curated.orders", "ERROR",
         f"SELECT COUNT(*) AS total, COUNTIF(order_status NOT IN ({valid_statuses})) AS failed FROM `{p}.curated.orders` WHERE order_status IS NOT NULL"),

        ("orphan_order_items", "curated.order_items", "ERROR",
         f"SELECT COUNT(*) AS total, COUNTIF(o.order_id IS NULL) AS failed FROM `{p}.curated.order_items` oi LEFT JOIN `{p}.curated.orders` o USING (order_id)"),

        ("duplicate_order_ids", "curated.orders", "WARNING",
         f"SELECT COUNT(*) AS total, SUM(IF(cnt > 1, cnt, 0)) AS failed FROM (SELECT order_id, COUNT(*) AS cnt FROM `{p}.curated.orders` GROUP BY order_id)"),

        ("invalid_date_sequence", "curated.orders", "ERROR",
         f"""SELECT COUNT(*) AS total,
             COUNTIF((order_delivered_customer_date IS NOT NULL AND order_delivered_customer_date < order_purchase_timestamp)
                  OR (order_approved_at IS NOT NULL AND order_approved_at < order_purchase_timestamp)) AS failed
             FROM `{p}.curated.orders` WHERE order_purchase_timestamp IS NOT NULL"""),

        ("zero_or_negative_payment", "curated.order_payments", "WARNING",
         f"SELECT COUNT(*) AS total, COUNTIF(payment_value <= 0) AS failed FROM `{p}.curated.order_payments` WHERE payment_value IS NOT NULL"),

        ("orphan_reviews", "curated.order_reviews", "WARNING",
         f"SELECT COUNT(*) AS total, COUNTIF(o.order_id IS NULL) AS failed FROM `{p}.curated.order_reviews` r LEFT JOIN `{p}.curated.orders` o USING (order_id)"),
    ]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    parser.add_argument("--batch-id", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.project:
        raise ValueError("GCP_PROJECT no definido.")

    client = bigquery.Client(project=args.project)
    batch_id = args.batch_id or str(uuid.uuid4())
    checked_at = datetime.now(timezone.utc).isoformat()

    log.info("Controles de calidad — batch_id=%s", batch_id)

    results = []
    for check_name, table_name, severity, sql in get_checks(args.project):
        try:
            result = run_check(client, args.project, check_name, table_name, severity, sql)
            results.append({**result, "batch_id": batch_id, "checked_at": checked_at})
        except Exception as exc:
            log.error("Error en %s: %s", check_name, exc)

    client.insert_rows_json(f"{args.project}.ops.quality_checks", results)

    passes = sum(1 for r in results if r["status"] == "PASS")
    log.info("Resultado: %d PASS / %d WARNING / %d FAIL",
             passes,
             sum(1 for r in results if r["status"] == "WARNING"),
             sum(1 for r in results if r["status"] == "FAIL"))


if __name__ == "__main__":
    main()
