# Notebook: 03_gold_star_schema
# Layer: Gold
# Purpose: Build the reporting-ready star schema from silver.
# Language: PySpark DataFrame API — kept consistent with bronze and silver,
# which both use PySpark, so the whole pipeline reads uniformly.
#
# Business definitions (documented in README as well):
# - A bug is a work item with work_item_type = 'Bug'.
# - The reporting month is derived from created_date. The business question
#   is "bugs created per month", and created_date is the only lifecycle date
#   that is always populated (about half the bugs are still open, so
#   closed_date or resolved_date would drop them).
#
# Model: fact_bug (grain: one row per bug) + dim_product + dim_date.
# Counts are not pre-aggregated; Power BI performs COUNT(*) over fact_bug,
# which keeps the model flexible across date grains and filters, and makes
# validation against silver trivial.

# CELL ---------------------------------------------------------------
# Parameters
SILVER_TABLE      = "silver_workitems"
DIM_PRODUCT       = "dim_product"
DIM_DATE          = "dim_date"
FACT_BUG          = "fact_bug"
EXPECTED_PRODUCTS = 5
EXPECTED_BUGS     = 30

from pyspark.sql import functions as F

# CELL ---------------------------------------------------------------
# Read silver.
silver = spark.table(SILVER_TABLE)
print("Silver rows in:", silver.count())

# CELL ---------------------------------------------------------------
# dim_product: one row per product. No separate product master exists in
# this delivery, so the dimension is derived from the data itself.
# Project and product are 1:1 in this dataset.
dim_product = (
    silver
    .filter(F.col("product_id").isNotNull())
    .select("product_id", "product_name", "organization_name", "project_name")
    .distinct()
)
dim_product.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true").saveAsTable(DIM_PRODUCT)
display(spark.table(DIM_PRODUCT).orderBy("product_id"))
# Expected: 5 rows, one per product

# CELL ---------------------------------------------------------------
# dim_date: daily calendar spanning the created_date range of ALL work
# items, not only bugs, so future bug months require no model change.
bounds = silver.agg(
    F.min(F.to_date("created_date")).alias("min_d"),
    F.max(F.to_date("created_date")).alias("max_d"),
).first()

dim_date = (
    spark.createDataFrame([(bounds["min_d"], bounds["max_d"])], ["min_d", "max_d"])
    .select(
        F.explode(
            F.sequence("min_d", "max_d", F.expr("interval 1 day"))
        ).alias("date")
    )
    .withColumn("date_key",       F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",           F.year("date"))
    .withColumn("month_number",   F.month("date"))
    .withColumn("month_short",    F.date_format("date", "MMM"))
    .withColumn("year_month",     F.date_format("date", "yyyy-MM"))
    .withColumn("year_month_key", F.date_format("date", "yyyyMM").cast("int"))
    .withColumn("quarter",        F.quarter("date"))
    .select("date_key", "date", "year", "month_number",
            "month_short", "year_month", "year_month_key", "quarter")
)
dim_date.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true").saveAsTable(DIM_DATE)
display(spark.sql(
    "SELECT MIN(date) AS from_date, MAX(date) AS to_date, COUNT(*) AS days FROM dim_date"
))

# CELL ---------------------------------------------------------------
# fact_bug: one row per bug. state, priority and severity are kept on the
# fact as degenerate attributes — at this volume, separate dimensions for
# them would add complexity without analytical benefit.
fact_bug = (
    silver
    .filter(F.col("work_item_type") == "Bug")
    .withColumn("created_date_key", F.date_format("created_date", "yyyyMMdd").cast("int"))
    .select(
        "organization_id",
        "project_id",
        "work_item_id",
        "product_id",
        "created_date_key",
        "created_date",
        "state",
        "priority",
        "severity",
        "resolved_date",
        "closed_date",
    )
)
fact_bug.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true").saveAsTable(FACT_BUG)
print("fact_bug rows:", spark.table(FACT_BUG).count())  # Expected: 30

# CELL ---------------------------------------------------------------
# Validation 1: referential integrity — every fact row must resolve to both
# dimensions. left_anti returns fact rows with no matching dimension key.
fact  = spark.table(FACT_BUG)
dim_p = spark.table(DIM_PRODUCT)
dim_d = spark.table(DIM_DATE)

orphan_products = fact.join(dim_p.select("product_id"), "product_id", "left_anti").count()
orphan_dates = (
    fact.select(F.col("created_date_key").alias("date_key"))
        .join(dim_d.select("date_key"), "date_key", "left_anti").count()
)
orphans = orphan_products + orphan_dates
assert orphans == 0, f"{orphans} fact rows do not join to a dimension"

# Validation 2: conservation — the bug count must match silver exactly.
silver_bugs = silver.filter(F.col("work_item_type") == "Bug").count()
gold_bugs = fact.count()
assert gold_bugs == silver_bugs == EXPECTED_BUGS, \
    f"Mismatch: silver={silver_bugs}, gold={gold_bugs}"
print("Gold validations passed")

# CELL ---------------------------------------------------------------
# The business answer: bugs created per product per month.
# This is effectively the query the Power BI model will execute over the
# star schema (fact_bug joined to dim_product and dim_date).
bugs_per_product_month = (
    fact
    .join(dim_p, "product_id")
    .join(dim_d, fact["created_date_key"] == dim_d["date_key"])
    .groupBy("product_name", "year_month")
    .agg(F.count("*").alias("bug_count"))
    .orderBy("product_name", "year_month")
)
display(bugs_per_product_month)
