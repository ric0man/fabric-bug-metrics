# Bug Metrics Pipeline — Microsoft Fabric

A small, end-to-end Microsoft Fabric solution that ingests an Azure DevOps work-item
export and turns it into a reporting-ready model answering one business question:

> **How many bugs are created per product per month?**

The solution follows a medallion architecture (Bronze → Silver → Gold) inside a single
Fabric Lakehouse, implemented in PySpark notebooks, and is designed to be clear,
validated, and easy to explain rather than maximally complex.

---

## Approach and architecture

```
raw CSV (Files/raw)
      │   01_bronze_ingestion   (PySpark)
      ▼
bronze_workitems      ─ lossless, all-string copy of the source + load metadata
      │   02_silver_cleaning    (PySpark)
      ▼
silver_workitems      ─ typed, cleaned, de-duplicated, all work-item types kept
      │   03_gold_star_schema   (PySpark)
      ▼
fact_bug + dim_product + dim_date   ─ star schema, consumed by Power BI
```

**Why a single Lakehouse (not Lakehouse + Warehouse):** one Lakehouse holds all three
medallion layers as Delta tables, and its SQL analytics endpoint is enough for Power BI.
This keeps the solution small and reviewable, matching the case's "clear, defensible
solution" instruction.

**Why PySpark throughout:** all three layers use the PySpark DataFrame API so the
pipeline reads uniformly and is easy to maintain. The transformations are simple enough
that Spark SQL would also work, but consistency aids readability.

**Layers:**

- **Bronze** — lands the semicolon-delimited CSV with schema inference *off* (every
  column read as string) so Bronze is a faithful, lossless copy. Adds `_ingested_at`
  and `_source_file` for lineage. Validates row/column counts before writing. Overwrite
  mode keeps the load idempotent.
- **Silver** — converts the literal `"NULL"` and empty strings to real nulls, trims
  values, drops empty/unusable columns, casts types explicitly, and de-duplicates on the
  business key. Keeps **all** work-item types so the layer is reusable for other metrics.
  Quality gates fail the run if critical fields are missing or keys are not unique.
- **Gold** — filters to bugs and builds a star schema: `fact_bug` (one row per bug) plus
  `dim_product` and `dim_date`. Includes referential-integrity and conservation checks,
  and a final query that produces bugs per product per month.

---

## Key definitions

- **What is a bug?** A work item with `work_item_type = 'Bug'`. Filtering happens in
  Gold; Silver keeps all types so other metrics remain possible without reprocessing.
- **Which date determines the reporting month?** `created_date`. The business question is
  "bugs *created* per month", and `created_date` is the only lifecycle date that is always
  populated — roughly half the bugs are still open, so `resolved_date` / `closed_date`
  would silently drop them.

---

## Assumptions made

- **Bug classification** uses `work_item_type = 'Bug'` exactly; no sub-types or
  severity-based inclusion rules are applied.
- **Reporting month** is based on `created_date` (see above).
- **Product dimension is derived from the data** — no separate product master file exists
  in this delivery. Project and product are 1:1 in this dataset, so the project name is
  carried into `dim_product` safely.
- **The input is a one-off snapshot** of 500 rows and 38 source columns; counts are
  asserted against these expected values.
- **No true duplicates exist in this snapshot.** De-duplication is defensive: if the same
  item ever appears twice, the most recent version wins (ordered by `changed_date`, then
  `ingestion_datetime`).
- **The final model includes only bugs** (`fact_bug`), while the Silver layer retains all
  work-item types for future reuse.

---

## Data quality issues identified

- Missing values are encoded inconsistently — the literal string `"NULL"` in most columns,
  and empty strings in `severity`. Both are standardized to real nulls in Silver.
- `area_level_4` and `area_level_5` are 100% empty in this delivery and are dropped.
- The precomputed `*_days` metrics (`bug_aging_days`, `cycle_time_days`, `lead_time_days`,
  etc.) are 95–100% empty and not verifiable; they are dropped. Any aging/cycle-time metric
  can be re-derived from the raw lifecycle dates if needed.
- `work_item_id` is **not globally unique** — the same id can exist in two different
  organizations as two distinct items. The business key is therefore
  `(organization_id, project_id, work_item_id)`.
- Timestamps arrive with seven fractional-second digits; a plain cast parses them and
  truncates to microseconds, which is sufficient for monthly reporting.
- Many lifecycle dates (`resolved_date`, `closed_date`, `activated_date`) are null for
  open items — a key reason `created_date` is used for the reporting month.
- The source file has a doubled `.csv.csv` extension and is semicolon-delimited.

---

## Modeling decisions

- **Star schema:** one fact (`fact_bug`) and two dimensions (`dim_product`, `dim_date`),
  matching the case's suggested model and the natural shape for Power BI.
- **Fact grain is one row per bug.** Counts are *not* pre-aggregated — Power BI runs
  `COUNT(*)` over `fact_bug`, which keeps the model flexible across date grains and filters
  and makes validation against Silver trivial.
- **Degenerate dimensions:** `state`, `priority`, and `severity` are kept on the fact
  rather than split into their own tables. At this volume, separate dimensions would add
  complexity without analytical benefit.
- **Date dimension spans all work items**, not only bugs, so future bug months require no
  model change. An integer `date_key` (e.g. `20240517`) links the fact to `dim_date`.
- **Product dimension** derived from distinct product attributes in the data.

---

## How the solution should be reviewed

1. Start with `00_data_exploration` (read-only) to see the profiling that
   justifies the cleaning rules, the composite key, and the choice of
   `created_date`. Then run the pipeline in order: `01_bronze_ingestion` →
   `02_silver_cleaning` → `03_gold_star_schema`. Each `# CELL ----` block maps to
   one Fabric notebook cell.
2. Each notebook contains **assertions / quality gates that fail loudly** — a green run is
   itself evidence of correctness (row counts, unique keys, non-null critical fields).
3. The decisive check is **conservation**: the bug count in Gold must equal the bug count
   in Silver, and both must equal 30. Gold also enforces referential integrity so every
   fact row resolves to both dimensions.
4. The final cell of the Gold notebook displays *bugs per product per month* — the direct
   answer to the business question and the query Power BI will run over the star schema.

Expected checkpoints: Bronze = 500 rows / 38 source columns; Silver = 500 rows; Gold =
5 products, 30 bugs.

---

## Productionizing / extending the solution

This is a deliberately focused snapshot solution. To run it in production I would:

- **Switch from snapshot to incremental loads** — replace overwrite with append plus a
  load identifier, and use a watermark (`changed_date` / `ingestion_datetime`) to process
  only new or changed items.
- **Orchestrate with a Fabric Data Pipeline** — land the file automatically, run the three
  notebooks in sequence on a schedule, and stop on any quality-gate failure.
- **Add a data-quality framework** — formalize the inline assertions into column-level
  tests (e.g. expectations on nullability, ranges, and uniqueness) with logged results.
- **Handle dimension change** — if product attributes evolve, apply slowly changing
  dimension logic instead of a full rebuild.
- **CI/CD** — use Fabric deployment pipelines with the GitHub integration to promote across
  dev/test/prod, with automated checks on pull requests.
- **Monitoring and alerting** — surface quality-gate failures and row-count anomalies to a
  monitoring channel.

---

## Repository structure

```
fabric-bug-metrics/
├─ README.md                  this file
├─ .gitignore
├─ data/
│  └─ brnz_azdo_workitems_current_anonymized.csv.csv   anonymized case-study input
└─ notebooks/
   ├─ 00_data_exploration.py    read-only profiling of the raw data (run first)
   ├─ 01_bronze_ingestion.py
   ├─ 02_silver_cleaning.py
   └─ 03_gold_star_schema.py
```

The hand-written `notebooks/*.py` are the reviewable source of truth. When connected to
Fabric Git integration, Fabric also syncs its own workspace items under a `/fabric`
folder; those are the deployed copies of the same notebooks.
