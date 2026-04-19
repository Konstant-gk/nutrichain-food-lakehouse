# openFDA Drug Label Data Lakehouse — Project Plan

**Stack:** Git · Databricks · openFDA **API** (`/drug/label.json`, JSON over HTTPS) · Apache Spark · Python · Spark SQL  
**Pattern:** Medallion (Bronze → Silver → Gold) on **Delta Lake** (Parquet files + transaction log)  
**Consumption:** Power BI (and/or Databricks SQL) on **Gold**  
**Orchestration:** Databricks **Workflows** (Jobs)

---

## Project requirements

### Objective

Build a **maintainable, testable** batch pipeline that **lands** openFDA drug product labeling JSON in a **Databricks lakehouse**, **refines** it through **Silver** and **Gold**, and **serves** curated tables for **analytics and dashboards**—with **clear lineage**, **job observability**, and **portfolio-grade documentation**.

### Specifications

- **Data sources:** Public **openFDA** REST API for **drug product labels** (`drug/label.json`). Payloads are **nested JSON** documents; access is **paginated** (`limit` / `skip`); higher rate limits with an **API key** stored in **Databricks Secrets** (never in Git).
- **Ingestion and compute:** **Batch** ingestion using **Python** on Databricks (HTTP client + Spark), **Spark SQL** for transformations, and **Delta** tables as the **system of record** per layer.
- **Data quality:** Validate **schema expectations**, **pagination completeness** within a run, and **deduplication** logic in **Silver**; record **run-level** metrics and optional **audit** rows for traceability.
- **Integration:** Unify API results into **conformed** tables suitable for **Star schema** or **flat** analytical tables in **Gold**, optimized for **Power BI** connections.
- **Scope:** **Bounded** pages per run (`max_pages`) on **Databricks** is acceptable **if documented**; optional **chunked backfill** is a later epic—not a hidden limitation.
- **Documentation:** **Architecture** diagram (A→Z), **data dictionary** for key columns, **runbook** (how to run, what “success” means), and **naming conventions** (this document + code comments).

---

## The problem with the data and the “as-is” workflow today

Today, **openFDA is an API, not a warehouse.** That is the core problem this project solves.


| Issue                        | Why it hurts                                                                                                                                             |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **No analyst-ready tables**  | Consumers cannot attach Power BI or run stable SQL joins against the API as if it were a database.                                                       |
| **Nested JSON**              | Label sections are **nested** and **variable**; raw JSON is hard to query, filter, and aggregate without a **designed** relational or star-shaped model. |
| **Pagination and limits**    | Data arrives in **pages**; you must **engineer** completeness checks, retries, and **audit** of what was requested versus what returned.                 |
| **Rate limits and failures** | Production-style ingestion needs **backoff**, **idempotent** writes, and **clear failure** behavior—none of which the API provides “for free.”           |
| **No built-in lineage**      | Without a **Bronze** layer, you cannot prove **what you pulled when**, or **replay** a run for debugging or compliance-style questions.                  |
| **Duplicates and drift**     | Re-runs and overlapping windows can **duplicate** payloads; **Silver** must define **keys** and **dedupe** rules.                                        |


**After this project:** the API remains the **source**, but the **lakehouse** becomes the **trusted analytics home**: **Bronze** preserves evidence, **Silver** cleans and conforms, **Gold** serves **BI-ready** grains.

---

## Goals

### Goals

- **G1 — Batch ingestion** from openFDA into **Bronze Delta** with **minimal** transform and **rich** metadata (`ingest_run_id`, pagination fields, hashes, timestamps).
- **G2 — Silver** deduplication, typing, and **conformed** columns for analysis.
- **G3 — Gold** tables or views for **specific questions** and **Power BI** consumption.
- **G4 — Orchestration** via **Databricks Jobs** (task DAG, retries, schedule).
- **G5 — Reproducibility:** Git as source of truth; Databricks Repos or equivalent sync.

---

## Stakeholders (portfolio framing)


| Stakeholder          | Success looks like                                                                           |
| -------------------- | -------------------------------------------------------------------------------------------- |
| **Analytics / BI**   | Stable **Gold** grain, documented refresh, **Power BI** can connect without JSON gymnastics. |
| **Data engineering** | Idempotent jobs, logs, validation, **secrets hygiene**.                                      |
| **Governance**       | **Bronze** raw payload + **lineage** columns; optional **audit** table.                      |


---

## Medallion architecture (Layer definitions)


| Dimension                 | Bronze                                                                                    | Silver                                                                                 | Gold                                                                               |
| ------------------------- | ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| **Objective**             | **Raw zone:** preserve API truth + traceability for debugging and audit.                  | **Curated zone:** clean, standardized, **analysis-ready** rows.                        | **Consumption zone:** **business-ready** models for reporting.                     |
| **Object type**           | **Delta table** (append-first; raw JSON column + metadata).                               | **Delta table** (deduped, typed).                                                      | **Delta tables** (and/or **views** on Delta) for **facts/dims** or **flat** marts. |
| **Load method (typical)** | **Append** (incremental batches) + **bounded** runs; optional **merge** strategies later. | **Overwrite** or **merge** / incremental per **business key** (choose explicitly).     | **Overwrite** or **merge** / incremental for **mart** refresh (choose explicitly). |
| **Data transformation**   | **Minimal:** land `raw_json`, hashes, request metadata; **no** heavy business rules.      | **Cleaning, standardization, normalization, derived columns, enrichment** (as needed). | **Integration, aggregation, business rules**; **star schema** or **wide** tables.  |
| **Data modeling**         | **None** (beyond keys for lineage).                                                       | **Conformed** keys; still **not** final star schema.                                   | **Star schema**, **aggregates**, **flat** tables for BI.                           |


---

## Naming conventions (professional, Baraa-style adapted for this project)

**General rules**

- **Language:** English only for **catalogs, schemas, tables, columns** in docs and code.
- **Case:** **snake_case** for SQL/table/column names unless your org enforces otherwise.
- **Avoid reserved words** as bare identifiers (`table`, `select`, `group`, …); use suffixes (`_tbl`, `_dt`) if needed.
- **Prefix layer** in the **table name** so objects are **self-describing** in logs and Unity Catalog: `bronze_`*, `silver_`*, `gold_*` (or schema-separated pattern; **pick one** and stay consistent).

**Bronze — raw / evidence**

- **Rule:** Identify **source** + **dataset** + **raw** intent.
- **Pattern:** `bronze_<source>_<entity>_raw`  
- **Example:** `bronze_openfda_drug_label_raw`  
- **Source system name:** `openfda` (short, stable). **Entity:** `drug_label` (logical dataset, not a renamed FDA internal table name—because the source is an API, not a DB table).

**Silver — cleaned / conformed**

- **Rule:** Same **source** + **entity**, new **layer**; **do not** reuse the same full name as Bronze without `_raw` vs `_curated` distinction.
- **Pattern:** `silver_<source>_<entity>`  
- **Example:** `silver_openfda_drug_label`

**Gold — business / analytics**

- **Rule:** Use **business-meaningful** names; prefer **star** naming where applicable.
- **Patterns:**  
  - `gold_<domain>_<grain>` for curated marts, e.g. `gold_label_section_summary`  
  - **Kimball-style:** `dim_<entity>`, `fact_<event>` (e.g. `dim_drug_product`, `fact_label_revision`)
- **Choose one primary style** for the portfolio slice and document it.

**Jobs, notebooks, and Git**

- **Python module:** `bronze_openfda_product_label_ingestion.py` (kebab/snake aligned with repo).  
- **Databricks Job name:** `openfda_drug_label_bronze_daily` (source + entity + layer + cadence).  
- **Secrets scope:** e.g. `openfda-api` (hyphenated scope names are common).

---

## Epics and tasks (execution checklist)

Use this as task list. Mark status in your tracker:

### Epic 1 — Requirements analysis


| Status | Task                                                                                                   |
| ------ | ------------------------------------------------------------------------------------------------------ |
| [ ]    | **Analyse:** Problem with **API-as-source** vs **lakehouse** needs (this document § “The problem…”).   |
| [ ]    | **Analyse:** **Business questions** and **success metrics** (freshness, job success, validation pass). |
| [ ]    | **Analyse:** **Operating constraints** (Databricks Free, batch scope, API limits).                     |


### Epic 2 — Design data architecture


| Status | Task                                                                                                                         |
| ------ | ---------------------------------------------------------------------------------------------------------------------------- |
| [ ]    | **Analyse:** Choose **batch** ingestion and **medallion** boundaries (Bronze minimal, Silver dedupe, Gold grain).            |
| [ ]    | **Design:** **Bronze / Silver / Gold** table names and **Unity Catalog** schema layout (`catalog.schema.table`).             |
| [ ]    | **Document:** **Data flow** diagram (A→Z: openFDA → Bronze → Silver → Gold → Power BI) and file in `docs/projects/openfda/`. |
| [ ]    | **Design:** **Orchestration** (Databricks Workflow tasks: Bronze job → Silver job → Gold job), retries, parameters.          |


### Epic 3 — Project initialization


| Status | Task                                                                                                                                        |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| [ ]    | **Document:** **Naming conventions** (this document) agreed and copied to README or CONTRIBUTING if needed.                                 |
| [ ]    | **Coding:** **Git** repo structure aligned with `git/docs/guidelines/repo_structure_guide.md`.                                              |
| [ ]    | **Coding:** **Databricks** workspace: cluster or serverless policy, **Secrets** scope for API key, **Repos** (or equivalent) linked to Git. |
| [ ]    | **Coding:** **Catalog / schema** (and **volumes** if used) created for **dev**; document names.                                             |


### Epic 4 — Build Bronze layer


| Status | Task                                                                                                                           |
| ------ | ------------------------------------------------------------------------------------------------------------------------------ |
| [ ]    | **Analyse:** **Source** assessment — endpoint, pagination, keys, failure modes (429/5xx).                                      |
| [ ]    | **Coding:** **Ingestion** in **Python** + **Spark** (`requests` or equivalent), write **append** to **Bronze Delta**.          |
| [ ]    | **Validating:** **Completeness** checks for a run (pages, empty results, early stop) + **schema** checks for required columns. |
| [ ]    | **Document:** **Bronze** data dictionary (metadata + `raw_json` policy).                                                       |
| [ ]    | **Commit:** Code in Git with **conventional** messages.                                                                        |


### Epic 5 — Build Silver layer


| Status | Task                                                                                                              |
| ------ | ----------------------------------------------------------------------------------------------------------------- |
| [ ]    | **Analyse:** Profile **Bronze** (nested JSON paths, cardinality, nulls).                                          |
| [ ]    | **Coding:** **Spark SQL** / PySpark transforms — **clean**, **normalize**, **dedupe** on **business key** + hash. |
| [ ]    | **Validating:** **Correctness** checks post-transform (counts, uniqueness, key nulls).                            |
| [ ]    | **Document:** **Silver** transformation rules (what changed from Bronze and why).                                 |
| [ ]    | **Commit:** Silver logic versioned in Git.                                                                        |


### Epic 6 — Build Gold layer


| Status | Task                                                                                                                |
| ------ | ------------------------------------------------------------------------------------------------------------------- |
| [ ]    | **Analyse:** **Business objects** and **dimensions/facts** (or **flat** wide tables) for Power BI.                  |
| [ ]    | **Coding:** **Integrate** Silver into **Gold** tables (joins, aggregates, **SCD** only if you explicitly scope it). |
| [ ]    | **Validating:** **Integration** checks (join integrity, **fan-out** risks, **grain** tests).                        |
| [ ]    | **Document:** **Star schema** or **mart** diagram (draw.io or Mermaid) + **data catalog** notes for consumers.      |
| [ ]    | **Commit:** Gold SQL/notebooks in Git.                                                                              |


### Epic 7 — Orchestration and operations


| Status | Task                                                                                                         |
| ------ | ------------------------------------------------------------------------------------------------------------ |
| [ ]    | **Coding:** **Databricks Job** with task dependencies, **parameters** (`catalog`, `schema`, `max_pages`).    |
| [ ]    | **Coding:** **Logging** and **run IDs**; optional **audit** Delta table for run summaries.                   |
| [ ]    | **Validating:** **End-to-end** dry run on schedule; capture **failure** alerts (email or job notifications). |


### Epic 8 — Visualization and handoff


| Status | Task                                                                                                                    |
| ------ | ----------------------------------------------------------------------------------------------------------------------- |
| [ ]    | **Analyse:** **Power BI** connection mode (**Import** vs **DirectQuery**) vs data size and freshness.                   |
| [ ]    | **Coding:** **Power BI** connection to **Databricks SQL** / **Unity Catalog** (or export path if you use intermediate). |
| [ ]    | **Document:** **Consumer** README — how to refresh, what **Gold** tables mean, known limitations.                       |


---

## Success metrics (KPIs)


| Metric                   | Definition                                                                 | Target                               |
| ------------------------ | -------------------------------------------------------------------------- | ------------------------------------ |
| **Job success rate**     | Runs ending in success / total scheduled                                   | ≥ 95% over a 2-week window           |
| **Freshness**            | Time between scheduled run and latest `ingested_at` in Bronze              | Within agreed batch SLA (e.g. T+24h) |
| **Completeness (proxy)** | Pagination and page counts match expectations; no silent zero-page success | Documented; assert in logs           |
| **Validation**           | Automated validation gate passes where implemented                         | Pass unless documented API outage    |
| **Observability**        | Every run has `ingest_run_id` and traceable logs                           | Always                               |


---

## Operating constraints (summary)

- **Databricks Free:** fair-use compute; **bound** `max_pages` per run; document **chunked backfill** as future work.
- **Secrets:** API key in **Databricks Secrets** only; **never** commit secrets to Git.
- **Reproducibility:** Git is source of truth; notebooks/scripts call **library** code, not duplicated logic.

---

