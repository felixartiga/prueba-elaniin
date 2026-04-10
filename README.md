# Olist Pipeline — DE Assessment

Pipeline batch sobre el dataset Brazilian E-Commerce (Olist) usando BigQuery + Python.

## Estructura

```
scripts/
  setup_bigquery.py     # crea datasets y tablas ops
  load_raw.py           # carga CSVs a raw
  batch_pipeline.py     # full batch raw → curated
  batch_incremental.py  # carga incremental con MERGE
  data_quality.py       # controles de calidad

sql/curated/            # SQLs de transformación por tabla
```

## Capas en BigQuery

- `raw` — datos tal como vienen del CSV
- `curated` — transformaciones, deduplicación, trazabilidad
- `ops` — batch_logs, table_metrics, quality_checks

## Setup

```bash
pip install -r requirements.txt
```

Crear `.env` (ver `.env.example`) y autenticar:

```bash
gcloud auth application-default login --no-launch-browser
```

## Ejecución

```bash
# 1. Crear datasets y tablas
python3 scripts/setup_bigquery.py

# 2. Cargar CSVs a raw
python3 scripts/load_raw.py

# 3. Full batch
python3 scripts/batch_pipeline.py

# 4. Controles de calidad
python3 scripts/data_quality.py

# 5. Batch incremental (segunda corrida)
python3 scripts/batch_incremental.py
```

## Notas

- La carga incremental usa `order_purchase_timestamp` como watermark, obtenido del último batch exitoso en `ops.batch_logs`.
- Los registros de trazabilidad (`batch_id`, `load_date`, `source_file`) están en todas las tablas curated.
- Screenshots de evidencia en `screenshots/`.
