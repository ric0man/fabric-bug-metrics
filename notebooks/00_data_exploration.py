# Notebook: 00_data_exploration
# Layer: Exploration (not part of the production ETL)
# Purpose: Profile the raw Azure DevOps export BEFORE building the pipeline,
# to surface data-quality issues and justify the modeling decisions taken in
# silver and gold. This notebook is read-only — it writes no tables.
#
# Findings from this analysis are summarized at the bottom and carried into
# the README (assumptions + data quality sections).

# CELL ---------------------------------------------------------------
# Read the raw file exactly as the source delivers it: semicolon-delimited,
# header row present, every column as string (no schema inference) so nothing
# is silently re-typed during profiling.
RAW_FILE = "Files/raw/brnz_azdo_workitems_current_anonymized.csv.csv"

from pyspark.sql import functions as F

df = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", False)
    .csv(RAW_FILE)
)

# CELL ---------------------------------------------------------------
# 1. Shape of the dataset: how many rows and columns are we dealing with?
print("Rows:", df.count())
print("Columns:", len(df.columns))
print(df.columns)

# CELL ---------------------------------------------------------------
# 2. Preview a few rows to eyeball the content and spot obvious issues
#    (e.g. the literal string "NULL", empty severity, mixed date formats).
display(df.limit(10))

# CELL ---------------------------------------------------------------
# 3. Missingness profile. The source encodes missing data inconsistently:
#    the literal string "NULL" in most columns and empty strings in others.
#    Count both per column so we know what silver has to clean.
missing = df.select([
    F.count(F.when(F.trim(F.col(c)).isin("NULL", ""), c)).alias(c)
    for c in df.columns
])
display(missing)

# CELL ---------------------------------------------------------------
# 4. Fully-empty columns — candidates to drop in silver.
#    Expectation: area_level_4 / area_level_5 are 100% empty, and the
#    precomputed *_days metric columns are 95-100% empty.
total = df.count()
empty_cols = []
for c in df.columns:
    non_missing = df.filter(~F.trim(F.col(c)).isin("NULL", "") & F.col(c).isNotNull()).count()
    if non_missing == 0:
        empty_cols.append(c)
print("Columns that are 100% empty:", empty_cols)

# CELL ---------------------------------------------------------------
# 5. What work item types exist, and how many of each?
#    This drives the bug definition: a bug = work_item_type 'Bug'.
display(
    df.groupBy("work_item_type").count().orderBy(F.desc("count"))
)

# CELL ---------------------------------------------------------------
# 6. Products in the data. There is no separate product master file, so the
#    product dimension will have to be derived from these values.
display(
    df.groupBy("product_id", "product_name").count().orderBy("product_id")
)

# CELL ---------------------------------------------------------------
# 7. The business question, straight on the raw data as a sanity baseline:
#    bugs created per product per month (created_date drives the month).
bugs = df.filter(F.col("work_item_type") == "Bug")
print("Total bugs:", bugs.count())

display(
    bugs
    .withColumn("year_month", F.date_format(F.col("created_date").cast("timestamp"), "yyyy-MM"))
    .groupBy("product_name", "year_month")
    .count()
    .orderBy("product_name", "year_month")
)

# CELL ---------------------------------------------------------------
# 8. Is work_item_id unique on its own? If not, we need a composite key.
#    Expectation: it is NOT globally unique (same id can exist in two orgs),
#    so silver keys on (organization_id, project_id, work_item_id).
total_rows = df.count()
distinct_ids = df.select("work_item_id").distinct().count()
distinct_keys = df.select("organization_id", "project_id", "work_item_id").distinct().count()
print(f"rows={total_rows} | distinct work_item_id={distinct_ids} | distinct composite key={distinct_keys}")

# Show any work_item_id that appears more than once.
display(
    df.groupBy("work_item_id").count().filter("count > 1").orderBy(F.desc("count"))
)

# CELL ---------------------------------------------------------------
# 9. Date coverage and completeness of the lifecycle dates. created_date is
#    chosen for the reporting month because it is the only one always present.
date_cols = ["created_date", "activated_date", "resolved_date", "closed_date", "changed_date"]
display(
    df.select([
        F.count(F.when(~F.trim(F.col(c)).isin("NULL", "") & F.col(c).isNotNull(), c)).alias(c + "_populated")
        for c in date_cols
    ])
)
display(
    df.select(
        F.min(F.col("created_date").cast("timestamp")).alias("earliest_created"),
        F.max(F.col("created_date").cast("timestamp")).alias("latest_created"),
    )
)

# CELL ---------------------------------------------------------------
# SUMMARY OF FINDINGS (carried into README)
# - 500 rows, 38 columns; one-off snapshot, semicolon-delimited, double .csv ext.
# - Missing values encoded as literal "NULL" and as empty strings -> standardize.
# - area_level_4/area_level_5 are 100% empty; *_days metrics 95-100% empty -> drop.
# - work_item_type defines a bug; ~30 bugs in this snapshot.
# - No product master -> derive dim_product from the data; project:product is 1:1.
# - work_item_id NOT globally unique -> key on (organization_id, project_id, work_item_id).
# - created_date is always populated; resolved/closed often null (open items)
#   -> use created_date for the reporting month.
