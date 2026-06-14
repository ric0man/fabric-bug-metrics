# Notebook: 02_silver_cleaning
# Layer: Silver
# Purpose: Transform bronze into a typed, cleaned, deduplicated table.
# All work item types are kept at this layer; bug filtering happens in gold,
# so silver stays reusable for other metrics later.

# CELL ---------------------------------------------------------------
# Parameters
BRONZE_TABLE = "bronze_workitems"
SILVER_TABLE = "silver_workitems"

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CELL ---------------------------------------------------------------
# Read bronze.
df = spark.table(BRONZE_TABLE)
source_count = df.count()
print(f"Bronze rows in: {source_count}")

# CELL ---------------------------------------------------------------
# Standardize missing values. The export encodes missing data as the literal
# string "NULL", and severity uses empty strings. Both are converted to real
# nulls, and all values are trimmed. The ingestion metadata columns are
# already typed and pass through unchanged.
META_COLS = {"_ingested_at", "_source_file"}

df = df.select([
    F.col(c) if c in META_COLS
    else F.when(F.trim(F.col(c)).isin("NULL", ""), None)
          .otherwise(F.trim(F.col(c))).alias(c)
    for c in df.columns
])

# CELL ---------------------------------------------------------------
# Drop columns that carry no usable information:
# - area_level_4 / area_level_5: 100% empty in this delivery
# - *_days columns: metrics precomputed by the source system's own pipeline,
#   95-100% empty and not verifiable. Any aging or cycle time we need can be
#   derived from the raw lifecycle dates instead.
DROP_COLS = [
    "area_level_4", "area_level_5",
    "p1_bug_aging_days", "p2_bug_aging_days", "bug_aging_days",
    "cycle_time_days", "lead_time_days",
]
df = df.drop(*DROP_COLS)

# CELL ---------------------------------------------------------------
# Cast columns to proper types. Timestamps arrive as
# 'yyyy-MM-dd HH:mm:ss.SSSSSSS' (seven fractional digits); a plain cast
# parses this and truncates to microseconds, which is sufficient for
# monthly reporting.
TS_COLS = ["created_date", "changed_date", "activated_date",
           "resolved_date", "closed_date", "ingestion_datetime"]
INT_COLS = ["work_item_id", "priority", "product_id"]
DOUBLE_COLS = ["story_points", "completed_work"]

for c in TS_COLS:
    df = df.withColumn(c, F.col(c).cast("timestamp"))
for c in INT_COLS:
    df = df.withColumn(c, F.col(c).cast("int"))
for c in DOUBLE_COLS:
    df = df.withColumn(c, F.col(c).cast("double"))

# CELL ---------------------------------------------------------------
# Deduplicate on the business key. work_item_id alone is not unique across
# the dataset (id 6775 exists in two different organizations as two distinct
# items), so the key is (organization_id, project_id, work_item_id).
# If the same item is ever delivered twice, the most recent version wins.
key = ["organization_id", "project_id", "work_item_id"]
w = Window.partitionBy(*key).orderBy(
    F.col("changed_date").desc_nulls_last(),
    F.col("ingestion_datetime").desc_nulls_last(),
)
df = (
    df.withColumn("_rn", F.row_number().over(w))
      .filter(F.col("_rn") == 1)
      .drop("_rn")
)

# CELL ---------------------------------------------------------------
# Data quality gates. The bug metric depends on work_item_type, created_date
# and product_name, so these must be complete and the key must be unique.
# Any violation stops the run before writing.
clean_count = df.count()

dup_keys = df.groupBy(*key).count().filter("count > 1").count()
assert dup_keys == 0, f"{dup_keys} duplicate business keys remain after deduplication"

crit = df.filter(
    F.col("work_item_type").isNull()
    | F.col("created_date").isNull()
    | F.col("product_name").isNull()
).count()
assert crit == 0, f"{crit} rows missing work_item_type, created_date or product_name"

# This snapshot contains no true duplicates, so no rows should be lost.
assert clean_count == source_count, f"Unexpected row loss: {source_count} -> {clean_count}"
print(f"Quality gates passed | Silver rows out: {clean_count}")

# CELL ---------------------------------------------------------------
# Write silver.
df.write.mode("overwrite").format("delta") \
  .option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)
print(f"Written: {SILVER_TABLE}")

# CELL -------------------------------