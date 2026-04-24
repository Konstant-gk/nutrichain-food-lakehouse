# NutriChain Food Lakehouse — Project Plan

**Stack:** Git · Airflow (Docker) · Open Food Facts API · Databricks (Spark + Delta) · dbt · Power BI  
**Pattern:** Medallion (Bronze → Silver → Gold) on Delta Lake  
**Consumption:** Power BI via Databricks SQL on Gold  
**Orchestration:** Apache Airflow (Dockerized) → Databricks Jobs

---

## Company story

**NutriChain Retail Intelligence** is a data analytics company that serves supermarket chains and food brands. Our clients need to:
- Understand the nutritional profile of their product catalog
- Benchmark their private-label products against category competitors  
- Comply with EU regulatory health labeling requirements (Nutri-Score)
- Identify which product categories have the worst nutritional quality

**The data problem:** Open Food Facts is a crowd-sourced database of 3M+ food products. It is free and real, but it is extremely messy: calories in kJ for some products, kcal for others; salt and sodium filled inconsistently; Nutri-Score grades assigned incorrectly; duplicate barcodes; free-text categories in 30+ languages.

**Our role as data engineers:** We build the pipeline that takes this raw, messy, crowd-sourced data and turns it into clean, trusted, business-ready tables that our analysts can use in Power BI without ever touching a JSON file.

---

## The problem we solve

| Raw Data Problem | Business Impact Without Fix |
|--|--|
| Energy in kJ vs kcal | Analysts compare apples to oranges in energy reports |
| Salt ≠ sodium (factor 2.5) | Regulatory compliance reports flag wrong products |
| Duplicate barcodes (same product, different names) | Counts and rankings are inflated |
| Nutri-Score grade wrong for ~8% of products | Regulatory flagging gives false positives |
| Free-text categories inconsistent | Cannot group by category for benchmarking |
| Null proteins on 40% of products | Protein density scoring impossible without fix |

---

## Medallion architecture

| Layer | Purpose | What we do |
|--|--|--|
| **Bronze** | Raw evidence | Land JSON exactly as received. Add run_id, timestamp. Never modify values. |
| **Silver** | Trusted clean | Fix units, fill nulls, deduplicate, add derived columns, flag issues. |
| **Gold** | Business ready | Build Star Schema: dims + fact with business metrics, rankings, averages. |

---

## Star schema (Gold)

```
dim_nutriscore ────────────────────────────────────────────┐
dim_brand ─────────────────────────────────────────────────┤
dim_category ──────────────────────────────────────────────┤
dim_country ───────────────────────────────────────────────┤
dim_product ───────────────────────────────────────────────┤
↓
fact_product_nutrition
(product_id, brand_id, category_id,
country_id, nutriscore_grade_key,
snapshot_date,
energy_kcal_per_100g, proteins_100g,
fat_100g, sugars_100g, salt_100g,
protein_density_score, nutriscore_score_raw,
sugar_tier, fat_tier, salt_tier,
nova_group_label, protein_density_tier,
category_avg_sugar, category_avg_fat,
is_above_avg_sugar, is_above_avg_fat,
healthiness_rank_in_category,
nutriscore_grade_mismatch)
```



---

## Silver transformations applied

| Column | Rule | Why |
|--|--|--|
| `energy_kcal_per_100g` | Use kcal field if present; else kJ ÷ 4.184 | OFF stores both formats inconsistently |
| `sodium_corrected_100g` | If sodium null: sodium = salt ÷ 2.5 | Many contributors fill only salt |
| `sugar_tier` | EU thresholds: <5g=low, 5–12.5g=medium, >12.5g=high | Regulatory compliance logic |
| `fat_tier` | EU thresholds: <3g=low, 3–17.5g=medium, >17.5g=high | Regulatory compliance logic |
| `salt_tier` | EU thresholds: <0.3g=low, 0.3–1.5g=medium, >1.5g=high | Regulatory compliance logic |
| `protein_density_score` | proteins_g / kcal × 100 | Measures protein per calorie |
| `nova_group_label` | 1=unprocessed, 2=culinary, 3=processed, 4=ultra_processed | Readable NOVA classification |
| `ingredient_count` | count(split(ingredients_text, ",")) | Proxy for processing complexity |
| `primary_category` | first item from categories comma list | Enables category-level joins |
| `primary_brand` | first item from brands comma list | Enables brand-level joins |
| `nutriscore_grade_recalculated` | Apply official score bands | Detect mislabeled products |
| `nutriscore_grade_mismatch` | reported ≠ recalculated → True | Flag for regulatory team |
| `row_hash` | SHA256(barcode + last_modified_unix) | Deduplication key |

---

## Gold business logic

| Metric | Formula | Used by |
|--|--|--|
| `healthiness_rank_in_category` | RANK() by nutriscore_score within category | Retail buyer ranking report |
| `category_avg_sugar/fat/salt` | AVG() over category window | Benchmarking dashboard |
| `is_above_avg_sugar/fat/salt` | product value > category avg | "Worse than peers" flag in PBI |
| `protein_density_tier` | score ≥ 10 = high, ≥ 5 = medium, else low | Nutrition team targeting |

---

## Success metrics

| Metric | Target |
|--|--|
| Daily pipeline success rate | ≥ 95% over 2-week window |
| Products ingested per run | > 5,000 (bounded by max_pages) |
| Silver dedup rate logged | Always — logged as duplicates_removed |
| Nutri-Score mismatch rate | < 10% (flag if higher — data quality alert) |
| Gold fact row count | Matches silver dedup count |