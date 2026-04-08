import argparse
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CSV_TABLE_MAP = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_translation",
}

# IDs y zip codes deben cargarse como string para preservar ceros a la izquierda
STRING_COLUMNS = {
    "olist_orders_dataset.csv": ["order_id", "customer_id"],
    "olist_order_items_dataset.csv": ["order_id", "product_id", "seller_id"],
    "olist_order_payments_dataset.csv": ["order_id"],
    "olist_order_reviews_dataset.csv": ["review_id", "order_id"],
    "olist_customers_dataset.csv": ["customer_id", "customer_unique_id", "customer_zip_code_prefix"],
    "olist_products_dataset.csv": ["product_id"],
    "olist_sellers_dataset.csv": ["seller_id", "seller_zip_code_prefix"],
    "olist_geolocation_dataset.csv": ["geolocation_zip_code_prefix"],
    "product_category_name_translation.csv": [],
}


def load_csv_to_raw(file_path, table_name, client, project):
    filename = file_path.name
    dtype_map = {col: str for col in STRING_COLUMNS.get(filename, [])}

    log.info("Leyendo %s", filename)
    df = pd.read_csv(file_path, dtype=dtype_map, low_memory=False)
    log.info("  %d filas, %d columnas", len(df), len(df.columns))

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, f"{project}.raw.{table_name}", job_config=job_config)
    job.result()

    log.info("  Cargado en raw.%s", table_name)
    return len(df)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT"))
    parser.add_argument("--data-dir", type=Path, default=Path(os.getenv("DATA_DIR", "data")))
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.project:
        raise ValueError("GCP_PROJECT no definido.")
    if not args.data_dir.exists():
        raise FileNotFoundError(f"Carpeta no encontrada: {args.data_dir}")

    client = bigquery.Client(project=args.project)
    total_rows = 0
    failed = []

    for filename, table_name in tqdm(CSV_TABLE_MAP.items(), desc="Cargando", unit="tabla"):
        file_path = args.data_dir / filename
        if not file_path.exists():
            log.warning("Archivo no encontrado: %s", filename)
            continue
        try:
            total_rows += load_csv_to_raw(file_path, table_name, client, args.project)
        except Exception as exc:
            log.error("Error en %s: %s", filename, exc)
            failed.append(filename)

    log.info("Tablas cargadas: %d/%d — Filas: %d", len(CSV_TABLE_MAP) - len(failed), len(CSV_TABLE_MAP), total_rows)
    if failed:
        log.warning("Errores en: %s", ", ".join(failed))


if __name__ == "__main__":
    main()
