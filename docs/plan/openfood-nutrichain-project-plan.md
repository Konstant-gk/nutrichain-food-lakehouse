# NutriChain Open Food Lakehouse — Project Plan

**Stack:** Git · GitHub Actions · Apache Airflow (Docker) · Python · Open Food Facts API · Databricks (Unity Catalog, Delta Lake, PySpark) · dbt · Power BI  
**Pattern:** Medallion (Bronze → Silver → Gold) on Delta Lake  
**Consumption:** Power BI (Databricks SQL connector) on Gold Delta — four report pages for ops health, completeness, product mix, and sanity checks  
**Orchestration:** Airflow DAG `nutrichain_openfood_daily` (every 3 hours UTC) → Databricks Jobs → dbt in Airflow container → refresh Gold in Power BI

**Timeline:** 1.5 months
**Industry:** Food retail / nutrition intelligence  
**Core stack:** Python, PySpark, SQL (dbt), Delta Lake, Airflow, Databricks

---

## Overview

NutriChain Retail Intelligence serves supermarket chains and food brands that need trustworthy nutrition analytics across large product catalogs. The source is [Open Food Facts](https://world.openfoodfacts.org/) — a real, crowd-sourced API with millions of products — not a clean ERP export.

This project builds an end-to-end **batch lakehouse**: paginated API ingest, raw landing on a Unity Catalog Volume, Bronze/Silver Delta tables, a **Gold star schema** built with dbt, automated tests at ingest and model layers, and **Power BI reports** that import from Databricks to monitor pipeline health and explore product nutrition at category grain.

Bounded ingest (`OPENFOOD_MAX_PAGES`, `OPENFOOD_RECORDS_PER_PAGE`) and **persisted pagination** (`OPENFOOD_PAGINATION_STATE_PATH`) let each scheduled run advance through the catalog instead of re-fetching page 1 every time.

---

## Company

**NutriChain Retail Intelligence** is a data analytics company serving supermarket chains and private-label food brands.

**Mission:** Turn messy, crowd-sourced food product data into governed, analytics-ready nutrition intelligence that retail buyers, category managers, and regulatory teams can trust without opening JSON or spreadsheets.

**Clients need to:**

- Understand the nutritional profile of their product catalog and private-label ranges  
- Benchmark products against category peers (sugar, fat, salt, Nutri-Score)  
- Support EU-oriented labeling logic (Nutri-Score, traffic-light style tiers)  
- Flag mislabeled or ultra-processed items before they reach customer-facing reports

**Role as data engineer:** Own the pipeline from API → lakehouse → star schema → BI, with documented quality rules at every layer and no manual copy-paste in the critical path.

---

## Problem

Before this pipeline, nutrition reporting on Open Food Facts data was **slow and untrustworthy**. The API returns rich JSON, but the payload is inconsistent by design: mixed energy units (kJ vs kcal), salt without sodium, duplicate barcodes, free-text categories in many languages, and contributor errors on Nutri-Score grades.

- Energy in kJ vs kcal (sometimes mislabeled) - Energy comparisons and density metrics are wrong 
- Salt filled but sodium null - Sodium-based compliance views under-report 
- Duplicate barcodes (same product, different snapshots) - Rankings and counts are inflated 
- Nutri-Score grade inconsistent with score - False regulatory flags or missed mislabeling 
- Comma-separated categories/brands/countries - Cannot join or benchmark at category/brand grain 
- Sparse proteins / nutrition fields - Protein-density and health targeting breaks 
- Evolving nested JSON schema - Bronze append failures if raw structs are persisted blindly

**Symptom for the business:** delayed category health reports, disputed numbers between teams, and analysts spending time cleaning exports instead of answering “how does our SKU compare to the category?”

---

## Goal

Build a **centralized, automated data lakehouse** that:

1. Ingests Open Food Facts product pages on a schedule with polite rate limiting and resumable pagination
2. Lands **raw evidence** (Bronze), then **cleanses and enriches** (Silver), then **models for analytics** (Gold star schema)
3. Enforces **data quality as a gate** (pytest on ingest code, dbt tests on Silver sources and Gold models)
4. Delivers **Power BI–ready tables** with category benchmarks, health rankings, and pipeline audit KPIs
5. Publishes **Power BI dashboards** on top of Gold so ops and analysts see run health and data quality without writing SQL
6. Deploys Databricks job code via **Git-backed Repos sync** on merge to `main` — no manual workspace uploads

**Definition of done:** A full DAG run succeeds end-to-end; Gold tests pass; `pagination_state.json` advances; Gold tables refresh in Power BI; the four report pages show current `ingest_run_id` slices and sane nutrition distributions.

---

## Solution

A **medallion lakehouse** with clear layer ownership:


| Layer              | Tool                                        | Responsibility                                                                         |
| ------------------ | ------------------------------------------- | -------------------------------------------------------------------------------------- |
| **Extract & land** | Python in Airflow (`fetch.py`, `upload.py`) | Call public API, write JSON locally, upload to UC Volume                               |
| **Bronze**         | PySpark job (`bronze_ingestion.py`)         | Append page-level rows; preserve `raw_products_json`; add lineage metadata             |
| **Silver**         | PySpark job (`silver_transform.py`)         | Parse JSON, type casts, 14+ cleansing rules, dedup by barcode, MERGE into Silver Delta |
| **Gold**           | dbt on Databricks SQL (`dbt/models/gold/`)  | Dimensions, `fact_product_nutrition`, `pipeline_audit`; seeds for Nutri-Score lookup   |
| **Quality**        | pytest + dbt tests                          | Ingest/upload/pagination unit tests; schema, relationships, custom SQL checks          |
| **Orchestration**  | Airflow 2.9 (Docker Compose)                | Six-task chain with retries; dbt run/test in same image                                |
| **Serve (BI)**     | Power BI Desktop + Databricks SQL           | Import Gold tables; star-schema relationships; ops and analyst report pages            |
| **CI/CD**          | GitHub Actions                              | `pytest` on PR/push; Repos API sync on `main`                                          |


**Design choice — API fetch in Airflow, not Databricks:** Databricks Free/serverless often blocks outbound internet. Airflow runs locally in Docker with full egress; Databricks only reads Volume files and runs Spark/dbt SQL.

---

## Architecture

### Structure plan

End-to-end flow:

```
Open Food Facts API (paginated JSON)
        │
        ▼
┌───────────────────────────────────────┐
│  Airflow (Docker, local workstation)   │
│  DAG: nutrichain_openfood_daily        │
│  schedule: 0 */3 * * * (UTC)           │
└───────────────┬───────────────────────┘
                │
  fetch → upload → bronze job → silver job → dbt seed/run → dbt test
                │
                ▼
┌───────────────────────────────────────┐
│  Databricks Unity Catalog              │
│  Volume: raw_json_landing              │
│  Bronze Delta → Silver Delta → Gold    │
└───────────────┬───────────────────────┘
                │
                ▼
┌───────────────────────────────────────┐
│  Power BI (Databricks SQL connector)   │
│  Import / refresh Gold + relationships │
│  4 report pages (health, QA, mix, QC)  │
└───────────────────────────────────────┘
```

**Medallion contracts:**

- **Bronze** — raw evidence; no business rules; stable string JSON column to avoid schema merge failures on evolving OFF nested fields  
- **Silver** — trusted clean data; units, tiers, dedup, regulatory flags; no star-schema modeling  
- **Gold** — business-ready star schema + audit mart; window metrics and analyst-facing flags

Diagram assets: `docs/architecture/architecture_openfood_nutrichain_lakehouse.drawio`, `docs/architecture/architecture_nutrichain_lakehouse.jpg`.

### Star schema (Gold)

```
dim_nutriscore ─────────────────────────────────────────────┐
dim_brand ───────────────────────────────────────────────────┤
dim_category ─────────────────────────────────────────────────┤
dim_country ──────────────────────────────────────────────────┤
dim_product ──────────────────────────────────────────────────┤
                                                              ▼
                                                    fact_product_nutrition
         (product_key, brand_key, category_key, country_key, nutriscore_key,
          snapshot_date, nutrition measures, tiers, benchmarks, rankings, flags)

pipeline_audit  (per ingest_run_id — ops / Power BI health dashboard)
```


| Gold model                                 | Grain                             | Purpose                                                    |
| ------------------------------------------ | --------------------------------- | ---------------------------------------------------------- |
| `dim_product`                              | One row per barcode               | Product attributes, NOVA group                             |
| `dim_brand`, `dim_category`, `dim_country` | One row per conformed value       | Filter and group in BI                                     |
| `dim_nutriscore`                           | One row per grade A–E (+ unknown) | Seed-backed lookup                                         |
| `fact_product_nutrition`                   | Product × `snapshot_date`         | Measures, category benchmarks, rankings                    |
| `pipeline_audit`                           | One row per `ingest_run_id`       | Volume, completeness, mismatch %, NOVA/grade distributions |


---

## How it works

### Step 0 — Configuration and operational parameters

All runtime tuning lives in **environment variables** (see `env.example.txt`); Python loaders fail fast if required keys are missing.


| Parameter                                                   | Example / role                                                           |
| ----------------------------------------------------------- | ------------------------------------------------------------------------ |
| `OPENFOOD_MAX_PAGES`                                        | Caps pages per DAG run (cost and API etiquette)                          |
| `OPENFOOD_RECORDS_PER_PAGE`                                 | Page size for API requests                                               |
| `OPENFOOD_USER_AGENT`                                       | Identifies the client (contact email)                                    |
| `OPENFOOD_POLITE_DELAY_SECONDS`                             | Delay between page requests                                              |
| `OPENFOOD_PAGE_MAX_RETRIES`                                 | Resilience on transient API errors                                       |
| `OPENFOOD_PAGINATION_STATE_PATH`                            | Persists `next_page_start` between runs (Docker volume `openfood_state`) |
| `AIRFLOW_TASK_RETRIES` / `AIRFLOW_TASK_RETRY_DELAY_SECONDS` | Task-level retry policy                                                  |
| `DATABRICKS_*`                                              | Catalog, schemas, Volume path, job IDs, upload limits                    |


**Repo layout (high level):** `src/openfood/` (ingest), `airflow/` (DAG + Compose), `databricks/bronze|silver/`, `dbt/`, `tests/`, `powerbi/` (`.pbix` + portfolio screenshots), `.github/workflows/ci_cd.yml`.

---

### Step 1 — Extract and land (Airflow Tasks 1–2)

**Task `fetch_openfood_pages`** (`src/openfood/fetch.py`)

- Calls the Open Food Facts search API with pagination starting from persisted offset  
- Writes one JSON file per page under `/tmp/nutrichain_openfood/{run_date}/{batch_id}/`  
- Updates pagination state after a successful run  
- Returns metadata via XCom (`run_id`, `run_date`, `output_dir`, counts)

**Task `upload_to_volume`** (`src/openfood/upload.py`)

- Uploads the batch folder to Unity Catalog Volume  
- Path pattern: `/Volumes/{catalog}/bronze/raw_json_landing/{run_date}/{run_id}/*.json`  
- Retries with backoff; respects max file size and timeout env vars

**Why this step exists:** Databricks does not call the public internet in this setup; Airflow owns all outbound HTTP.

---

### Step 2 — Bronze layer: source ingestion

**Task `trigger_bronze_job`** → `databricks/bronze/bronze_ingestion.py`

Bronze is the **audit layer**. Page-level JSON is read from the Volume; **no cleansing** of nutrition values occurs here.

**What Bronze does:**

- Read `multiLine` JSON for the run’s Volume subfolder  
- Add lineage: `ingest_run_id`, `ingest_layer`, `ingested_at`, `source_system`, `source_url`, page metadata  
- Serialize the `products` array to `**raw_products_json` (string)** and drop the inferred struct column — prevents `DELTA_FAILED_TO_MERGE_FIELDS` when OFF adds nested fields between runs  
- **Append** to `bronze.bronze_openfood_products_raw` with optional schema merge  
- Fail fast if zero rows (upload or path mismatch)

**What Bronze does not do:** unit conversion, deduplication, or star-schema keys.

**Operational traceability:** Each run is keyed by `ingest_run_id` (batch id `YYYYMMDD_HH00` from Airflow logical date). Silver filters Bronze by this id.

---

### Step 3 — Silver layer: data cleansing and validation

**Task `trigger_silver_job`** → `databricks/silver/silver_transform.py`

Silver is where data becomes **trustworthy for joins**. PySpark reads Bronze for one `ingest_run_id`, explodes products from `raw_products_json` using a **fixed struct schema**, applies rules, deduplicates, and **MERGE**s into `silver.silver_openfood_products` on `barcode`.

#### Silver transformations applied


| #   | Column / rule                                            | Logic                                                          | Business reason                    |
| --- | -------------------------------------------------------- | -------------------------------------------------------------- | ---------------------------------- |
| 1   | Explode products                                         | `from_json` + `explode` on page payload                        | One row per product                |
| 2   | `energy_kcal_per_100g`                                   | Prefer plausible kcal; else kJ ÷ 4.184; cap >900 treated as kJ | OFF mixes kJ/kcal labels           |
| 3   | Energy band guard                                        | Null out values outside 0–900 kcal per 100g                    | Drop impossible contributor values |
| 4   | `sodium_corrected_100g`                                  | Use sodium if present; else `salt_100g / 2.5`                  | Salt-only rows are common          |
| 5   | `sugar_tier`                                             | <5g low, 5–12.5g medium, >12.5g high                           | EU-style traffic light bands       |
| 6   | `fat_tier`                                               | <3g / 3–17.5g / >17.5g                                         | Same                               |
| 7   | `salt_tier`                                              | <0.3g / 0.3–1.5g / >1.5g                                       | Same                               |
| 8   | `protein_density_score`                                  | `proteins_100g / energy_kcal_per_100g * 100`                   | Protein per calorie targeting      |
| 9   | `nova_group_label`                                       | Map 1–4 to readable labels                                     | Analyst-friendly processing class  |
| 10  | `ingredient_count`                                       | `size(split(ingredients_text, ","))`                           | Processing complexity proxy        |
| 11  | `primary_category` / `primary_brand` / `primary_country` | First token from comma lists                                   | Conformed join keys for Gold       |
| 12  | `nutriscore_grade_recalculated`                          | Score bands → grade a–e                                        | Detect mislabeled products         |
| 13  | `nutriscore_grade_mismatch`                              | Reported ≠ recalculated                                        | Regulatory / QA flag               |
| 14  | `row_hash`                                               | SHA-256(barcode | last_modified_unix)                          | Dedup support                      |
| 15  | `barcode_is_ean`                                         | Regex `^[0-9]{8,14}$`                                          | Monitoring (non-EAN kept)          |
| 16  | Dedup                                                    | `ROW_NUMBER` by barcode, keep latest `last_modified_unix`      | One current row per product        |
| 17  | Filter                                                   | Drop null barcode                                              | Enforce primary key                |


**Silver does not:** category window averages, healthiness rank, or surrogate keys for the star schema — those belong in Gold.

---

### Step 4 — Gold layer: business-ready star schema (dbt)

**Tasks `run_dbt_gold_models` and `test_dbt_gold_models`**

Gold is owned **only by dbt** (no PySpark Gold job). `dbt seed` loads `nutriscore_grade_lookup.csv`; `dbt run --select path:models/gold` builds Delta tables; `dbt test` gates the run.

`**fact_product_nutrition` — Gold business logic**


| Output                         | Logic                                                         | Used for                                                  |
| ------------------------------ | ------------------------------------------------------------- | --------------------------------------------------------- |
| Surrogate keys                 | `SHA2` on barcode, brand, category, country, nutriscore grade | Star joins; unknown brand/category → `'Unknown'` sentinel |
| `snapshot_date`                | Airflow `run_date` via dbt var                                | Slowly changing snapshot grain                            |
| Category averages              | `AVG(...) OVER (PARTITION BY primary_category)`               | Benchmark dashboards                                      |
| `healthiness_rank_in_category` | `RANK()` by `nutriscore_score_raw` ASC within category        | “Where does our SKU rank?”                                |
| `healthiness_percentile`       | Rank normalized by category size                              | Top/bottom % messaging                                    |
| `protein_density_tier`         | ≥10 high, ≥5 medium, else low                                 | Nutrition team segmentation                               |
| `is_above_avg_sugar/fat/salt`  | Product vs category average                                   | Peer comparison filters                                   |
| `nutriscore_grade_mismatch`    | Carried from Silver                                           | Regulatory QA                                             |


**Dimensions:** `dim_product`, `dim_brand`, `dim_category`, `dim_country`, `dim_nutriscore` — conformed attributes for Power BI relationships.

`**pipeline_audit`:** Aggregates Silver by `ingest_run_id` — product counts, mismatch %, missing nutrition %, invalid EAN %, NOVA and Nutri-Score distributions — primary source for the **Pipeline Health** Power BI page.

---

### Step 5 — Serve: Power BI on Gold (import from Databricks)

After Gold tables are built and `dbt test` passes, consumption happens in **Power BI Desktop** (reports under `powerbi/`). Data is loaded through the **Databricks SQL** connector (or equivalent warehouse endpoint) against catalog `nutrichain_lakehouse`, schemas `gold` (and `silver` only if a page needs raw completeness before Gold snapshot).

The lakehouse is not “done” when Delta tables exist — stakeholders need a repeatable way to see whether last night’s run succeeded and whether nutrition fields are usable. Power BI is the **operational and analyst front door** on top of the same tables dbt materialized.

#### Connection and model


| Item              | Practice in this project                                                                                           |
| ----------------- | ------------------------------------------------------------------------------------------------------------------ |
| **Source tables** | `gold.fact_product_nutrition`, `gold.pipeline_audit`, `gold.dim_`* (product, brand, category, country, nutriscore) |
| **Grain**         | Fact = product × `snapshot_date`; audit = one row per `ingest_run_id`                                              |
| **Relationships** | Star schema: fact keys → dimension `*_key` columns; hide technical keys from report view where possible            |
| **Global filter** | `ingest_run_id` slicer (default **All**) on every page so ops can compare runs or isolate one batch                |
| **Refresh**       | Manual or scheduled refresh after a successful DAG run (Gold append/MERGE + dbt run)                               |


#### Report pages (four dashboards)


| Page                    | Primary tables                               | What it monitors                                                                                                                                                                                                                                                                                                                                     |
| ----------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Pipeline Health**     | `pipeline_audit`                             | KPI cards: unique products (total), products per latest run, nutriscore mismatch %, missing kcal %, silver completion time, hours since last silver finish. Table of all runs with volume and quality columns. Line charts: products processed per run (drop flags fetch/API issues), missing calories % over time, nutriscore mismatch % over time. |
| **Column Completeness** | `fact_product_nutrition` (+ dims for labels) | KPI % missing for kcal, protein, sugar, fat, NOVA, nutriscore grade. Stacked bar: missing vs present across selected nutrients. Detail table: sample products with null `energy_kcal_per_100g` for investigation.                                                                                                                                    |
| **Product Mix**         | `fact_product_nutrition` + dims              | Nutri-Score grade mix (donut), NOVA processing level (bar), sugar traffic-light tiers (donut), top 10 categories and countries — validates conformed dimensions and tier logic after Silver/Gold.                                                                                                                                                    |
| **Sanity Check**        | `fact_product_nutrition`                     | Histogram of `energy_kcal_per_100g` (0–900 band documented on-page: values outside band nulled in Silver; kJ mislabeled as kcal divided by 4.184). KPI count of products with kcal > 700. Scatter **Calories vs Fats** to spot unit or labeling outliers.                                                                                            |


**What Power BI does not do:** It does not replace dbt tests or pytest; it **visualizes** the outputs of those gates. A green DAG + passing `dbt test` should correlate with improving trends on Pipeline Health, not the other way around.

---

### Step 6 — Data quality implementation

Quality is a **gate**, not an afterthought.


| Layer                    | Mechanism                                 | Examples                                                                                      |
| ------------------------ | ----------------------------------------- | --------------------------------------------------------------------------------------------- |
| **Python (CI + local)**  | `pytest` on `src/openfood`                | Fetch retries, pagination advance, upload paths                                               |
| **Silver (dbt sources)** | `sources.yml` tests                       | `not_null` barcode, `unique` barcode, accepted values on tiers                                |
| **Silver (custom SQL)**  | `dbt/tests/`                              | Energy kcal in range; sugar tier valid; sugars non-negative; EAN % threshold; null kcal % cap |
| **Gold (schema.yml)**    | Generic + relationship tests              | PK uniqueness on dims; FK relationships on fact; `not_null` on keys and `snapshot_date`       |
| **Gold (custom SQL)**    | `fact_energy_kcal_in_range.sql`           | Fact-level energy sanity                                                                      |
| **Pipeline**             | DAG fails if `test_dbt_gold_models` fails | No “successful” run with broken Gold                                                          |


**Categories mirrored from enterprise practice:**

1. Primary key uniqueness (dimensions and barcode in Silver)
2. Referential integrity (fact → dims)
3. Measure sanity (energy, sugars, tiers)
4. Distribution / completeness thresholds (null kcal %, EAN %)
5. Regulatory consistency (Nutri-Score mismatch rate tracked in `pipeline_audit`)
6. Run-level summary stats (`pipeline_audit` row per ingest)

---

### Step 7 — CI/CD and deployment

**Workflow:** `.github/workflows/ci_cd.yml`

1. **test** — Python 3.12, `pytest tests/` with coverage on `src/`
2. **databricks_delivery** (on `main`) — `PATCH /api/2.0/repos/{DATABRICKS_REPO_ID}` to pull Git-backed repo

Databricks Bronze/Silver jobs must point at **Repos** paths (e.g. `/Repos/<user>/nutrichain-food-lakehouse/databricks/...`), not legacy `/Shared` imports.

---

## Success metrics


| Metric                    | Target / signal                                                                                           |
| ------------------------- | --------------------------------------------------------------------------------------------------------- |
| DAG success rate          | ≥ 95% over a 2-week window (scheduled + manual)                                                           |
| Products ingested per run | > 0 pages; bounded by `OPENFOOD_MAX_PAGES × OPENFOOD_RECORDS_PER_PAGE` per run                            |
| Pagination advance        | `pagination_state.json` shows increasing `next_page_start` across successful runs                         |
| Silver dedup              | Logged `duplicates_removed` per run; stable unique `barcode` count                                        |
| Nutri-Score mismatch rate | Tracked in `pipeline_audit.nutriscore_mismatch_pct`; alert if sustained > 10%                             |
| Gold test suite           | `dbt test` passes on `source:silver` and `path:models/gold`                                               |
| Fact grain integrity      | Gold row count aligns with Silver unique products for the modeled snapshot                                |
| Bronze lineage            | Rows present for `ingest_run_id` matching Airflow batch                                                   |
| CI                        | `pytest` green on every push/PR; Repos sync on `main` after tests                                         |
| Power BI refresh          | After a successful DAG run, Gold KPIs and run table match `pipeline_audit` row for latest `ingest_run_id` |
| Sanity visuals            | Histogram mass within 0–900 kcal/100g; scatter shows expected fat–calorie correlation for oils/fats       |


---

## Project specification (technical)


| Area               | Specification                                                                           |
| ------------------ | --------------------------------------------------------------------------------------- |
| **Source**         | Open Food Facts public REST API; no API key                                             |
| **Ingest cadence** | Every 4 hours UTC (`0 */4 * * `*)                                                       |
| **Batch identity** | `run_date` = `YYYYMMDD`, `run_id` / `ingest_run_id` = `YYYYMMDD_HH00`                   |
| **Catalog**        | `nutrichain_lakehouse` (default); schemas `bronze`, `silver`, `gold`                    |
| **Bronze table**   | `bronze.bronze_openfood_products_raw`                                                   |
| **Silver table**   | `silver.silver_openfood_products` (MERGE on `barcode`)                                  |
| **Gold tables**    | dbt-managed dims, `fact_product_nutrition`, `pipeline_audit`                            |
| **Orchestration**  | Airflow 2.9, Postgres metadata, Docker Compose locally                                  |
| **Transform**      | PySpark 3.x on Databricks (Silver); dbt-databricks 1.8 (Gold)                           |
| **BI**             | Power BI Desktop; Databricks SQL → Gold fact/dims + `pipeline_audit`; four report pages |
| **Secrets**        | `.env` and `dbt/profiles.yml` gitignored; GitHub Actions secrets for Databricks         |


---

## Results (expected outcomes)

After a successful implementation cycle:

- **Category nutrition benchmarking** is possible in one SQL/BI query — peers, averages, and rank in category without manual JSON wrangling.  
- **Regulatory-oriented flags** (tiers, Nutri-Score mismatch) are materialized columns, not ad hoc spreadsheet rules.  
- **Operations visibility** — `pipeline_audit` drives the **Pipeline Health** page (run table, mismatch %, missing kcal trends, latest silver timestamp).  
- **Analyst self-service** — **Product Mix**, **Column Completeness**, and **Sanity Check** pages validate tiers, dimensions, and Silver energy rules without opening notebooks.  
- **Any SQL-capable BI tool** can connect to the same Gold tables; Power BI is the reference implementation in `powerbi/`.

---

## What I learned

**Design first, code second.** Layer contracts (Bronze string JSON, Silver cleanse-only, Gold model-only) were decided before notebook and dbt code — that prevented mixing concerns and made failures easier to localize.

**Separation of concerns at the data layer is a discipline.** Bronze never transforms nutrition values. Silver never builds the star schema. Gold never re-implements unit conversion. That split matches how maintainable teams run lakehouses.

**Data quality rules are business requirements.** Each Silver rule maps to a real OFF quirk (kJ/kcal, salt/sodium, EU tiers, Nutri-Score bands) — not arbitrary engineer preferences.

**Fail fast beats fail silently.** Zero-row Bronze raises; Silver raises on empty explode; DAG chains dbt test after run; pytest guards ingest behavior in CI.

**Platform constraints drive architecture.** Moving API fetch to Airflow because of Databricks egress limits is a practical pattern for portfolio and production alike.

**Documentation is the long tail of value.** This plan, architecture diagrams, `schema.yml` tests, and inline comments in PySpark/dbt are written for the next engineer — client, colleague, or future me.

This project was built as part of intensive training with **Baraa Khatib Salkini** — senior data architect and instructor ([Data With Baraa](https://www.datawithbaraa.com/)). Medallion discipline, documentation standards, and production-oriented habits throughout this repo reflect that mentorship.

---

