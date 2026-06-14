# Notebook: 01_bronze_ingestion
# Layer: Bronze
# Purpose: Land the raw Azure DevOps work item export into the Lakehouse
# without any transformation. Cleaning and typing are handled in silver.

# CELL ---------------------------------------------------------------
# Parameters
RAW_FILE = "Files/raw/brnz_azdo_workitems_current_anonymized.csv.csv"  # double .csv extension is from the source system
BRONZE_TABLE = "bronze_workitems"
EXPECTED_ROWS = 500  # known size of this one-off snapshot delivery

# CELL ---------------------------------------------------------------
# Read the source file. The export is semicolon-delimited.
# Schema inference is disabled intentionally: all columns are read as string
# so the bronze layer remains a faithful, lossless copy of the source.
# Type casting is deferred to silver, where it is explicit and reviewable.
df_raw = (
    spark.read
    .option("header", True)
    .option("delimiter", ";")
    .option("inferSchema", False)
    .csv(RAW_FILE)
)

print(f"Rows read: {df_raw.count()} | Columns: {len(df_raw.columns)}")

# CELL ---------------------------------------------------------------
# Add ingestion metadata for lineage: load timestamp and source file name.
from pyspark.sql import functions as F

df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(RAW_FILE))
)

# CELL ---------------------------------------------------------------
# Validate the load before writing. Failing fast here prevents a broken
# or partial file from reaching downstream layers.
actual_rows = df_bronze.count()
assert actual_rows == EXPECTED_ROWS, f"Expected {EXPECTED_ROWS} rows, got {actual_rows}"
assert len(df_raw.columns) == 38, f"Expected 38 source columns, got {len(df_raw.columns)}"
print("Load validation passed")

# CELL ---------------------------------------------------------------
# Write to a Delta table. Overwrite mode keeps the load idempotent — the
# notebook can be rerun safely. A recurring load would use append with a
# load identifier instead (see README, productionization).
df_bronze.write.mode("overwrite").format("delta").saveAsTable(BRONZE_TABLE)
print(f"Written: {BRONZE_TABLE}")

# CEL