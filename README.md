# NutriChain Food Lakehouse

Batch data lakehouse that ingests [Open Food Facts](https://world.openfoodfacts.org/) product data, lands it on Databricks Delta Lake (Bronze → Silver → Gold), and serves analytics in Power BI — orchestrated with Apache Airflow, transformed with PySpark and dbt, and delivered through GitHub Actions.

## Overview

Open Food Facts exposes millions of product records as a paginated REST API. Analysts need stable tables, deduplicated keys, nutrition facts in typed columns, and a star schema — not raw JSON pages.

NutriChain closes that gap:

- **Ingest** paginated API pages from Airflow (outbound HTTP stays off Databricks serverless).
- **Land** JSON on a Unity Catalog Volume, then load **Bronze** Delta with run metadata.
- **Clean and enrich** in **Silver** with PySpark (dedup, casts, derived fields).
- **Model** **Gold** with dbt (dimensions, `fact_product_nutrition`, `pipeline_audit` for ops dashboards).
- **Validate** with pytest (fetch/upload) and dbt tests (schema + custom SQL checks).
- **Deploy** Databricks job code via Repos API sync on push to `main`.

Bounded pagination (`OPENFOOD_MAX_PAGES`) and a persisted page offset (`OPENFOOD_PAGINATION_STATE_PATH`) let each scheduled run advance through the catalog instead of re-fetching page 1 every time.

## Architecture

```
Open Food Facts API (paginated JSON)
              │
              ▼
┌─────────────────────────────────────────┐
│  Airflow (Docker Compose, local)      │
│  DAG: nutrichain_openfood_daily       │
│  schedule: every 3 hours (UTC)        │
└─────────────────┬───────────────────────┘
                  │
    fetch → upload → bronze job → silver job → dbt seed/run → dbt test
                  │
                  ▼
┌─────────────────────────────────────────┐
│  Databricks Unity Catalog               │
│  Volume  → Bronze Delta → Silver Delta  │
│            → Gold Delta (dbt)           │
└─────────────────┬───────────────────────┘
                  ▼
            Power BI (SQL / Import)
```

| Step | Owner | Output |
|------|--------|--------|
| `fetch_openfood_pages` | `src/openfood/fetch.py` | JSON under `/tmp/nutrichain_openfood/{run_date}/{batch_id}/` |
| `upload_to_volume` | `src/openfood/upload.py` | Files on UC Volume `raw_json_landing` |
| `trigger_bronze_job` | `databricks/bronze/bronze_ingestion.py` | `bronze.bronze_openfood_products_raw` |
| `trigger_silver_job` | `databricks/silver/silver_transform.py` | `silver.silver_openfood_products` |
| `run_dbt_gold_models` | `dbt/models/gold/*` | Star schema + `pipeline_audit` |
| `test_dbt_gold_models` | dbt tests | Silver sources + Gold models |

**Why Airflow calls the API:** Databricks Free/serverless often blocks outbound internet. Airflow runs on your machine in Docker; Databricks only reads Volume files and runs Spark/dbt SQL.

**Delivery:** On merge to `main`, GitHub Actions runs `pytest`, then `PATCH /api/2.0/repos/{DATABRICKS_REPO_ID}` to pull the Git-backed repo in Databricks (no legacy `/Shared` workspace import).

Diagram sources: [docs/architecture/architecture_openfood_nutrichain_lakehouse.drawio](docs/architecture/architecture_openfood_nutrichain_lakehouse.drawio), [docs/architecture/architecture_nutrichain_lakehouse.jpg](docs/architecture/architecture_nutrichain_lakehouse.jpg).

## Tech stack

| Layer | Tool | Role |
|-------|------|------|
| Ingestion | Python 3.12, `requests` | Paginated fetch, retries, polite delay |
| Config | `src/openfood/config.py` | Required env vars (defaults only in `env.example.txt`) |
| Orchestration | Apache Airflow 2.9 | DAG, XCom, Databricks job triggers, dbt BashOperators |
| Storage | Delta Lake (Unity Catalog) | Bronze / Silver / Gold tables |
| Silver compute | PySpark on Databricks | Dedup, typing, enrichment |
| Gold compute | dbt-databricks 1.8 | Star schema, seeds, tests |
| CI | GitHub Actions | `pytest` + Repos sync |
| BI | Power BI | `powerbi/pipeline-health-dashboard.pbix` |

Pinned versions: root `requirements.txt` (`dbt-core==1.8.8`, `apache-airflow==2.9.0`, etc.).

## Project structure

```
nutrichain-food-lakehouse/
├── airflow/
│   ├── docker-compose.yml          # Local Airflow + Postgres
│   ├── Dockerfile
│   └── dags/openfood_bronze_dag.py
├── src/openfood/
│   ├── config.py                   # OPENFOOD_* / AIRFLOW_* loaders
│   ├── fetch.py                    # API client
│   ├── pagination.py               # Page offset between runs
│   └── upload.py                   # Volume upload
├── databricks/
│   ├── bronze/bronze_ingestion.py
│   └── silver/silver_transform.py  # Gold is dbt-only (no PySpark gold job)
├── dbt/
│   ├── models/gold/                # dims, fact, pipeline_audit
│   ├── models/silver/              # ephemeral bridge + sources.yml
│   ├── seeds/                      # e.g. nutriscore_grade_lookup.csv
│   └── tests/                      # Custom SQL data quality tests
├── tests/                          # pytest (fetch, upload, pagination)
├── powerbi/
├── docs/architecture/
├── .github/workflows/ci_cd.yml
├── env.example.txt                 # Copy to .env (gitignored)
└── requirements.txt
```

## Getting started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Databricks workspace with Unity Catalog, SQL warehouse, and a **Databricks Repo** linked to this GitHub repo
- Databricks jobs created for Bronze and Silver notebooks (job IDs in `.env` and Airflow Variables)

### 1. Clone and configure

```bash
git clone https://github.com/Konstant-gk/nutrichain-food-lakehouse.git
cd nutrichain-food-lakehouse
cp env.example.txt .env
```

Edit `.env` with your Databricks host, token, catalog/schemas, Volume path, Postgres/Airflow admin credentials, and job IDs. All `OPENFOOD_*` and `AIRFLOW_TASK_*` keys are **required** at runtime — there are no hidden Python defaults.

Set Airflow Variables in the UI (Admin → Variables):

- `databricks_bronze_job_id`
- `databricks_silver_job_id`

Configure the Airflow connection `databricks_default` (host + token).

### 2. Start Airflow

```bash
cd airflow
docker compose up --build -d
```

UI: `http://localhost:8080` (user/password from `AIRFLOW_ADMIN_*` in `.env`).

### 3. Run the pipeline

Enable DAG `nutrichain_openfood_daily`, then trigger a run. Task order:

`fetch_openfood_pages` → `upload_to_volume` → `trigger_bronze_job` → `trigger_silver_job` → `run_dbt_gold_models` → `test_dbt_gold_models`

dbt receives `--vars '{"run_date": "<logical date>"}'` for snapshot dating on Gold facts.

### 4. Unit tests (host or CI)

```bash
pip install -r requirements.txt
pytest tests/ --cov=src --cov-report=term-missing -v
```

## Usage

### Pagination across runs

Each successful fetch advances `next_page_start` in `OPENFOOD_PAGINATION_STATE_PATH` (default under `/tmp/nutrichain_openfood/`). Docker Compose mounts a named volume `openfood_state` at that path so three-hourly runs do not restart at page 1.

`OPENFOOD_MAX_PAGES` caps how many pages one run requests; increase it for larger batches (watch API rate limits and Databricks cost).

### dbt locally (optional)

Run from the `dbt/` directory only. Use `dbt/profiles.yml` with `env_var()` for Databricks credentials (file is gitignored; see `dbt/profiles.yml` pattern in repo docs).

```bash
cd dbt
dbt seed --profiles-dir .
dbt run --profiles-dir . --select path:models/gold --vars '{"run_date": "2026-05-15"}'
dbt test --profiles-dir . --select source:silver path:models/gold
```

Artifacts: `dbt/logs/dbt.log`, `dbt/target/` (gitignored). For lineage UI, `dbt docs generate` then `dbt docs serve --port 8081` so Airflow can keep port 8080.

Use **dbt-core 1.8.x** from `requirements.txt`, not dbt Fusion 2.x.

### Power BI

Open `powerbi/pipeline-health-dashboard.pbix` and point to Gold tables (including `pipeline_audit` for run-level health metrics).

## Data model (Gold)

Star schema centered on `fact_product_nutrition` (one row per product per snapshot):

| Model | Purpose |
|-------|---------|
| `dim_product` | Product attributes, NOVA group |
| `dim_brand`, `dim_category`, `dim_country` | Conformed dimensions |
| `dim_nutriscore` | Grade A–E (seed-backed lookup) |
| `fact_product_nutrition` | Nutrition measures per 100g |
| `pipeline_audit` | Per-`ingest_run_id` volume and quality KPIs for monitoring |

Silver is **not** rebuilt by dbt; dbt declares `source('silver', 'silver_openfood_products')` and uses an ephemeral model to `ref()` into Gold.

## Testing and data quality

| Layer | Checks |
|-------|--------|
| Python | `tests/test_fetch.py`, `tests/test_upload.py`, `tests/test_pagination.py`, `tests/test_fetch_pagination.py` |
| dbt schema | `not_null`, `unique`, `relationships`, `accepted_values` on Gold (`dbt/models/gold/schema.yml`) |
| dbt custom | `dbt/tests/` — e.g. energy kcal range, sugar tier validity, barcode EAN share |
| CI | `pytest tests/` on every push/PR; Repos sync on `main` after tests pass |

## Deployment (CI/CD)

Workflow: [.github/workflows/ci_cd.yml](.github/workflows/ci_cd.yml)

1. **test** — Python 3.12, `pip install -r requirements.txt`, `pytest` with coverage on `src/`
2. **databricks_delivery** ( `main` only ) — `PATCH` Repos API with branch `main`

GitHub Actions secrets: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_REPO_ID`.

Databricks jobs must reference notebook paths under your **Repos** mount (e.g. `/Repos/<user>/nutrichain-food-lakehouse/databricks/bronze/...`), not `/Shared/...`.

## Documentation

| Path | Contents |
|------|----------|
| [docs/architecture/](docs/architecture/) | Draw.io and diagram assets for the lakehouse |
| [docs/plan/openfda-project-plan.md](docs/plan/openfda-project-plan.md) | Earlier portfolio plan artifact (historical) |
| `env.example.txt` | Full list of environment variables |

Local explanation guides under `docs/explanation/` may exist on disk but are gitignored in this repo.

## Success criteria

| Check | Expected |
|-------|----------|
| Bronze | Rows with `ingest_run_id` for the triggered batch |
| Silver | Unique `barcode` / product keys; enrichment columns populated |
| Gold | dbt run completes; tests pass |
| Airflow | DAG run success, no failed upstream tasks |
| Pagination | `pagination_state.json` shows increasing `next_page_start` across runs |

## Known constraints

- **Bounded ingest:** `OPENFOOD_MAX_PAGES` limits pages per run; full historical backfill is a separate operational exercise.
- **API etiquette:** Set `OPENFOOD_USER_AGENT` with contact info; tune `OPENFOOD_POLITE_DELAY_SECONDS` for rate limits.
- **Databricks Free tier:** Fair-use compute; larger page counts increase runtime and cost.
- **Secrets:** Never commit `.env` or `dbt/profiles.yml`; use Databricks secrets or GitHub Actions secrets in automation.

## License

Portfolio / educational use. Add a `LICENSE` file if you fork for redistribution.
