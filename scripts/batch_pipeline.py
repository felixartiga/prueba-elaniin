import argparse
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent.parent / "sql" / "curated"

TABLES = ["customers", "sellers", "products", "orders", "order_items", "order_payments", "order_reviews"]


def get_row_count(client, table_ref):
    return list(client.query(f"SELECT COUNT(*) as n FROM `{table_ref}`").result())[0].n


def run_table(client, project, batch_id, table):
    log.info("Procesando: %s", table)

    raw_count = get_row_count(client, f"{project}.raw.{table}")
    sql = (SQL_DIR / f"{table}.sql").read_text().replace("{project}", project).replace("@batch_id", f"'{batch_id}'")

    job_config = bigquery.QueryJobConfig(
        destination=f"{project}.curated.{table}",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    client.query(sql, job_config=job_config).result()

    loaded_count = get_row_count(client, f"{project}.curated.{table}")
    log.info("  raw=%d curated=%d rechazadas=%d", raw_count, loaded_count, raw_count - loaded_count)

    return {"table_name": table, "raw_count": raw_count, "loaded_count": loaded_count, "rejected_count": raw_count - loaded_count}


def write_batch_log(client, project, batch_id, status, started_at, finished_at, tables_processed, total_rows, error_message=None):
    client.insert_rows_json(f"{project}.ops.batch_logs", [{
        "batch_id": batch_id,
        "batch_type": "FULL",
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "watermark_from": None,
        "watermark_to": None,
        "tables_processed": tables_processed,
        "total_rows_loaded": total_rows,
        "error_message": error_message,
    }])


def write_table_metrics(client, project, batch_id, metrics_list):
    rows = [{
        "batch_id": batch_id,
        "table_name": m["table_name"],
        "source_file": f"olist_{m['table_name']}_dataset.csv",
        "raw_row_count": m["raw_count"],
        "loaded_row_count": m["loaded_count"],
        "rejected_row_count": m["rejected_count"],
        "new_row_count": None,
        "updated_row_count": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    } for m in metrics_list]
    client.insert_rows_json(f"{project}.ops.table_metrics", rows)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.project:
        raise ValueError("GCP_PROJECT no definido.")

    client = bigquery.Client(project=args.project)
    batch_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    log.info("Batch FULL iniciado — batch_id=%s", batch_id)

    metrics_list = []
    status = "SUCCESS"
    error_msg = None

    try:
        for table in TABLES:
            metrics_list.append(run_table(client, args.project, batch_id, table))
    except Exception as exc:
        status = "FAILED"
        error_msg = str(exc)
        log.error("Pipeline fallido: %s", exc)
    finally:
        finished_at = datetime.now(timezone.utc)
        total_rows = sum(m["loaded_count"] for m in metrics_list)
        if metrics_list:
            write_table_metrics(client, args.project, batch_id, metrics_list)
        write_batch_log(client, args.project, batch_id, status, started_at, finished_at, len(metrics_list), total_rows, error_msg)

    log.info("Status=%s | Tablas=%d | Filas=%d | Duración=%.1fs",
             status, len(metrics_list), total_rows, (finished_at - started_at).total_seconds())

    if status == "FAILED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
