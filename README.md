# 🥦 NutriChain Food Lakehouse

> **Production-grade batch data lakehouse pipeline** — Open Food Facts API → Databricks Delta Lake (Bronze → Silver → Gold) → Power BI, orchestrated with Apache Airflow, transformed with dbt, containerised with Docker, and deployed via GitHub Actions CI/CD.

---

## 📌 The Problem This Solves

The [Open Food Facts](https://world.openfoodfacts.org/) dataset is one of the largest open food databases in the world — 3M+ product records across 170+ countries. But it is only available as a raw REST API. There are no analyst-ready tables, no stable SQL surface, and no lineage.

**NutriChain solves this.** It turns a messy, paginated JSON API into a trusted, queryable lakehouse on Databricks — with full audit trail, deduplication, and a star schema ready for Power BI dashboards.

---

## 🏗️ Architecture

```
Open Food Facts API
        │
        ▼
┌───────────────────┐
│   Apache Airflow  │  ← Daily batch DAG · Retries · Secrets management
│   (Docker)        │
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  BRONZE (Delta)   │  ← Raw JSON · Pagination metadata · Run ID · Hash keys
│  Databricks       │
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  SILVER (dbt)     │  ← Dedup · Type casting · Null handling · Schema checks
│  Databricks       │
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  GOLD (dbt)       │  ← Star schema · fact_product_nutrition · dim_brand
│  Databricks       │    dim_category · dim_nutriscore · dim_country
└────────┬──────────┘
         │
         ▼
     Power BI
  (Databricks SQL)
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + `requests` | Paginated API fetch with backoff |
| Orchestration | Apache Airflow 2.9 | DAG scheduling, retries, alerting |
| Containerisation | Docker + Docker Compose | Reproducible local and CI environment |
| Storage | Databricks Delta Lake | Bronze / Silver / Gold tables |
| Transformation | dbt-databricks 1.8 | Silver cleaning + Gold star schema |
| Compute | Apache Spark (Databricks) | Distributed processing |
| CI/CD | GitHub Actions | Lint, test, deploy on push |
| BI | Power BI (Databricks SQL) | Nutrition dashboards |
| Secrets | Databricks Secrets | API key — never committed to Git |

---

## 📂 Repository Structure

```
nutrichain-food-lakehouse/
│
├── airflow/
│   ├── Dockerfile                        # Airflow container (own requirements)
│   ├── requirements.txt                  # Airflow-only deps (no dbt conflict)
│   └── dags/
│       └── openfood_bronze_dag.py        # Daily batch DAG
│
├── src/
│   └── openfood/
│       ├── fetch.py                      # API client — pagination + backoff
│       └── upload.py                     # Spark write to Bronze Delta
│
├── dbt/
│   ├── Dockerfile                        # dbt container (own requirements)
│   ├── requirements.txt                  # dbt-databricks only
│   ├── dbt_project.yml
│   └── models/
│       ├── silver/
│       │   ├── sources.yml                 # UC Silver Delta (PySpark-owned)
│       │   └── silver_openfood_products.sql  # ephemeral bridge → ref() in Gold
│       └── gold/
│           ├── dim_product.sql
│           ├── fact_product_nutrition.sql
│           ├── dim_brand.sql
│           ├── dim_category.sql
│           ├── dim_country.sql
│           └── dim_nutriscore.sql
│
├── databricks/
│   ├── bronze/bronze_ingestion.py
│   └── silver/silver_transform.py        # Gold is built by dbt only (no PySpark gold job in repo)
│
├── tests/
│   ├── test_fetch.py
│   └── test_upload.py
│
├── .github/
│   └── workflows/
│       └── ci_cd.yml                     # Lint → Test → Deploy pipeline
│
├── docs/
│   └── nutrichain_project_plan.md
│
├── docker-compose.yml
├── .env.example                          # Safe env var template (no secrets)
└── README.md
```

---

## ⚙️ CI/CD Pipeline

Every push to `main` triggers a GitHub Actions workflow:

```
Push to main
     │
     ├── 1. Lint        (flake8 + dbt parse)
     ├── 2. Unit Tests  (pytest + pytest-cov)
     ├── 3. Build       (Docker Compose build)
     └── 4. Deploy      (Databricks CLI sync to workspace)
```

No code reaches Databricks without passing all gates.

---

## 📊 Gold Layer — Star Schema

```
                    ┌─────────────────────────┐
                    │  fact_product_nutrition  │
                    │  (grain: one row/product)│
                    └────────────┬────────────┘
                                 │
          ┌──────────┬───────────┼───────────┬──────────┐
          ▼          ▼           ▼           ▼          ▼
      dim_brand  dim_category  dim_country  dim_nutriscore
```

| Table | Description |
|---|---|
| `fact_product_nutrition` | One row per product · energy, fat, sugar, salt, protein values |
| `dim_brand` | Brand name, normalised |
| `dim_category` | Product category hierarchy |
| `dim_country` | Country of sale |
| `dim_nutriscore` | Nutri-Score grade (A → E) with label |

---

## 🚀 How to Run Locally

### Prerequisites
- Docker Desktop
- Databricks workspace (Free tier works)
- Open Food Facts API key (optional — higher rate limits)

### 1. Clone the repo
```bash
git clone https://github.com/Konstant-gk/nutrichain-food-lakehouse.git
cd nutrichain-food-lakehouse
```

### 2. Configure environment variables
```bash
cp .env.example .env
# Fill in your Databricks host, token, and catalog values
```

### 3. Start Airflow + dbt containers
```bash
docker compose up --build
```

### 4. Access Airflow UI
```
http://localhost:8080
user: airflow / password: airflow
```

### 5. Trigger the pipeline
Enable and trigger `nutrichain_openfood_daily` from the Airflow UI. The DAG runs Bronze and Silver on Databricks, then **dbt** builds Gold (`dbt run --select path:models/gold` with `run_date` = Airflow logical date).

### dbt logs, docs, and repo `docs/`

Full command and mental-model guide: [docs/explanation/dbt-cheatsheet-guide.md](docs/explanation/dbt-cheatsheet-guide.md).

| Path | What it is |
|------|------------|
| `docs/` (repo root) | Human-written project docs (plans, guides) — **not** the dbt docs website |
| `dbt/logs/dbt.log` | dbt CLI run log (only location after `log-path` in `dbt_project.yml`) |
| `dbt/target/` | Compiled SQL, `manifest.json`, `catalog.json` — inputs for the docs site |
| `airflow/logs/` | Airflow task/scheduler logs (DAG runs), gitignored |

`dbt docs generate` does **not** open a browser; it writes `dbt/target/manifest.json` and `dbt/target/catalog.json`. View lineage in a browser (use port **8081** so Airflow can keep **8080**):

**PowerShell (load root `.env` — dbt has no `--env-file` flag):**

```powershell
cd dbt
Get-Content ..\.env | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }
  $n, $v = $_ -split '=', 2
  Set-Item -Path "env:$($n.Trim())" -Value $v.Trim()
}
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8081
```

Open `http://localhost:8081`. Use **`dbt-core==1.8.8`** from `requirements.txt` in your venv — not **dbt Fusion** (CLI 2.x), which drops some commands.

After generate inside Docker (`/opt/airflow/dbt`), the same files appear on disk at `dbt/target/` because `../dbt` is bind-mounted.

Always run dbt from the **`dbt/`** directory. Never run dbt from `airflow/` (that creates `airflow/logs/dbt.log` and wrong `dbt_project.yml` paths).

**Test artifacts at repo root:** `.pytest_cache/` and `.coverage` are created by `pytest`; they are gitignored and safe to delete anytime (they come back on the next test run).

---

## ✅ What "Success" Looks Like

| Check | Definition |
|---|---|
| Bronze rows landed | At least 1 page of records written with `ingest_run_id` |
| No duplicate products in Silver | Uniqueness assertion on `product_id` passes |
| Gold row count ≥ Silver | No records lost in star schema join |
| All dbt tests green | Schema + not-null + unique tests pass |
| Airflow DAG status | `success` in UI with no skipped tasks |

---

## 📋 Data Dictionary (Key Columns)

| Column | Layer | Description |
|---|---|---|
| `ingest_run_id` | Bronze | UUID per pipeline run — links all rows to one execution |
| `ingested_at` | Bronze | UTC timestamp of API fetch |
| `raw_json` | Bronze | Full API response payload, unmodified |
| `product_id` | Silver | Deduplication business key (`code` from API) |
| `nutriscore_grade` | Silver / Gold | A–E nutritional quality score |
| `energy_kcal_100g` | Gold fact | Energy per 100g in kcal |

---

## ⚠️ Known Constraints

- **Bounded pagination:** `max_pages` is configurable per Airflow run. Full historical backfill is scoped as a future epic — this is documented, not hidden.
- **Databricks Free tier:** compute is fair-use; large `max_pages` values will increase DBU consumption.
- **API rate limits:** a Databricks Secret holds the API key for higher limits; anonymous access works for small runs.

---

## 🔗 Links

- **Portfolio:** [konstantinos-gkaravelos.com](https://konstantinos-gkaravelos.com)
- **Data source:** [Open Food Facts API](https://world.openfoodfacts.org/data)
- **Project plan:** [`docs/nutrichain_project_plan.md`](docs/nutrichain_project_plan.md)
