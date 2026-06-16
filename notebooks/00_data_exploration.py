# Notebook: 00_data_exploration
# Layer: Exploration (not part of the production ETL)
# Purpose: Quick profile of the raw Azure DevOps export BEFORE building the
# pipeline, to surface data-quality issues and justify the silver/gold design.
# Read-only: writes no tables.

# CELL 1 -------------------------------------------------------------
# Read the raw file as delivered: semicolon-delimited, header present, all
# columns as string (no schema inference) so nothing is silently re-typed.
RAW_FILE = "Files/raw/brnz_azdo_workitems_current_anonymized.csv.csv"

from pyspark.sql import functions as F

df = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", False)
    .csv(RAW_FILE)
)
print("Rows:", df.count(), "| Columns:", len(df.columns))
display(df.limit(10))

# CELL 2 -------------------------------------------------------------
# Work item types and products. These drive two key decisions:
# a bug = work_item_type 'Bug', and dim_product is derived from the data
# (no separate product master exists).
display(df.groupBy("work_item_type").count().orderBy(F.desc("count")))
display(df.groupBy("product_id", "product_name").count().orderBy("product_id"))

# CELL 3 -------------------------------------------------------------
# The business question on raw data as a baseline: bugs created per product
# per month (created_date drives the reporting month).
bugs = df.filter(F.col("work_item_type") == "Bug")
print("Total bugs:", bugs.count())
display(
    bugs
    .withColumn("year_month", F.date_format(F.col("created_date").cast("timestamp"), "yyyy-MM"))
    .groupBy("product_name", "year_month").count()
    .orderBy("product_name", "year_month")
)

# CELL 4 -------------------------------------------------------------
# Data quality checks: missing values (source uses literal "NULL" and empty
# strings) and whether work_item_id is unique on its own.
display(
    df.select([
        F.count(F.when(F.trim(F.col(c)).isin("NULL", ""), c)).alias(c)
        for c in df.columns
    ])
)
print("distinct work_item_id:", df.select("work_item_id").distinct().count())
print("distinct (org, project, work_item_id):",
      df.select("organization_id", "project_id", "work_item_id").distinct().count())

# CELL 5 -------------------------------------------------------------
# SUMMARY OF FINDINGS (carried into README)
# - 500 rows, 38 columns; semicolon-delimited one-off snapshot.
# - Missing data encoded as literal "NULL" and empty strings -> standardize in silver.
# - area_level_4/5 are 100% empty; *_days metrics 95-100% empty -> drop in silver.
# - work_item_type defines a bug; ~30 bugs in this snapshot.
# - No product master -> derive dim_product from data; project:product is 1:1.
# - work_item_id NOT globally unique -> key on (organization_id, project_id, work_item_id).
# - created_date always populated; resolved/closed often null -> use created_date for the month.
print("See comments above for the summary of findings.")
