import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import Conflict

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

OPS_SCHEMAS = {
    "batch_logs": [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("batch_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("finished_at", "TIMESTAMP"),
        bigquery.SchemaField("watermark_from", "TIMESTAMP"),
        bigquery.SchemaField("watermark_to", "TIMESTAMP"),
        bigquery.SchemaField("tables_processed", "INTEGER"),
        bigquery.SchemaField("total_rows_loaded", "INTEGER"),
        bigquery.SchemaField("error_message", "STRING"),
    ],
    "table_metrics": [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source_file", "STRING"),
        bigquery.SchemaField("raw_row_count", "INTEGER"),
        bigquery.SchemaField("loaded_row_count", "INTEGER"),
        bigquery.SchemaField("rejected_row_count", "INTEGER"),
        bigquery.SchemaField("new_row_count", "INTEGER"),
        bigquery.SchemaField("updated_row_count", "INTEGER"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ],
    "quality_checks": [
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("check_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("severity", "STRING"),
        bigquery.SchemaField("total_checked", "INTEGER"),
        bigquery.SchemaField("failed_count", "INTEGER"),
        bigquery.SchemaField("pass_rate", "FLOAT"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("details", "STRING"),
        bigquery.SchemaField("checked_at", "TIMESTAMP"),
    ],
}

DATASET_DESCRIPTIONS = {
    "raw": "Datos cargados tal como vienen del origen.",
    "curated": "Datos transformados con campos de trazabilidad.",
    "ops": "Logs de ejecución, métricas y controles de calidad.",
}


def create_dataset(client, project, dataset_id, location):
    dataset = bigquery.Dataset(f"{project}.{dataset_id}")
    dataset.location = location
    dataset.description = DATASET_DESCRIPTIONS.get(dataset_id, "")
    try:
        client.create_dataset(dataset)
        log.info("Dataset creado: %s.%s", project, dataset_id)
    except Conflict:
        log.info("Dataset ya existe: %s.%s", project, dataset_id)


def create_ops_table(client, project, table_name, schema):
    table = bigquery.Table(f"{project}.ops.{table_name}", schema=schema)
    try:
        client.create_table(table)
        log.info("Tabla creada: ops.%s", table_name)
    except Conflict:
        log.info("Tabla ya existe: ops.%s", table_name)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    parser.add_argument("--location", default=os.getenv("BQ_LOCATION", "US"))
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.project:
        raise ValueError("GCP_PROJECT no definido.")

    client = bigquery.Client(project=args.project)

    for ds in ["raw", "curated", "ops"]:
        create_dataset(client, args.project, ds, args.location)

    for table_name, schema in OPS_SCHEMAS.items():
        create_ops_table(client, args.project, table_name, schema)

    log.info("Setup completado.")


if __name__ == "__main__":
    main()
