# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: ratings (Auto Loader stream)
# MAGIC
# MAGIC * **Source type**: file stream - Auto Loader (`format("cloudFiles")`) watching
# MAGIC   `/Volumes/.../ratings/landing/`.
# MAGIC * **Trigger**: `availableNow=True` - the stream picks up every file currently
# MAGIC   in the landing directory, processes them in micro-batches, and then **stops**.
# MAGIC   Perfect for a notebook you re-run by hand on Free Edition.
# MAGIC * **Checkpoint**: kept inside the Volume so it survives between runs and
# MAGIC   guarantees each file is read **exactly once**.
# MAGIC * **Write**: append to managed Delta table `bronze_ratings`.
# MAGIC * **Bronze rule**: keep the data as-is. We only add ingestion metadata
# MAGIC   (`_ingest_ts`, `_source_file`). Cleaning/typing happens in silver.
# MAGIC
# MAGIC ## How the "add new data → dashboard changes" test works
# MAGIC 1. Drop `ratings_inc_01.csv` into the landing folder.
# MAGIC 2. Re-run this notebook. Auto Loader sees one new file, appends ~N rows,
# MAGIC    and stops. Re-run again → 0 new rows (idempotency from the checkpoint).
# MAGIC 3. Re-run `03_silver` and `04_gold`, refresh the dashboard.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema",  "movielens", "Schema")
dbutils.widgets.text("volume",  "raw",       "Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")

SCHEMA_FQN          = f"`{CATALOG}`.`{SCHEMA}`"
BRONZE_TABLE        = f"{SCHEMA_FQN}.bronze_ratings"
LANDING_DIR         = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/ratings/landing"
CHECKPOINT_DIR      = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/ratings/_checkpoints/auto_loader_ratings"
SCHEMA_LOCATION_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/ratings/_checkpoints/auto_loader_ratings_schema"

print("Landing       :", LANDING_DIR)
print("Checkpoint    :", CHECKPOINT_DIR)
print("Schema store  :", SCHEMA_LOCATION_DIR)
print("Target table  :", BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show what is currently in the landing folder
# MAGIC Run this before the stream to know how many files Auto Loader is about to see.

# COMMAND ----------

try:
    display(dbutils.fs.ls(LANDING_DIR))
except Exception as e:
    # If the folder doesn't exist yet, the user forgot to run 00_setup.py
    raise RuntimeError(
        f"{LANDING_DIR} does not exist. Run 00_setup.py first, then upload "
        f"ratings_initial.csv into the landing folder."
    ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the read stream (Auto Loader)
# MAGIC
# MAGIC We pin the schema explicitly. Auto Loader still records it in
# MAGIC `cloudFiles.schemaLocation` so the stream can evolve safely on later runs.

# COMMAND ----------

ratings_schema = StructType([
    StructField("userId",    IntegerType(), nullable=False),
    StructField("movieId",   IntegerType(), nullable=False),
    StructField("rating",    DoubleType(),  nullable=True),
    StructField("timestamp", LongType(),    nullable=True),   # Unix epoch seconds (UTC)
])

stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",          "csv")
        .option("cloudFiles.schemaLocation",  SCHEMA_LOCATION_DIR)
        .option("header",                     "true")
        .schema(ratings_schema)
        .load(LANDING_DIR)
        .withColumn("_ingest_ts",   F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the stream - `availableNow` trigger
# MAGIC
# MAGIC `availableNow=True` is the modern replacement for `Trigger.Once`. It
# MAGIC processes everything currently visible, in multiple micro-batches if
# MAGIC needed, then stops the stream. Perfect for a "run when I tell you" notebook.

# COMMAND ----------

query = (
    stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .option("mergeSchema",        "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)

# Block until this triggered run finishes (then the stream terminates).
query.awaitTermination()
print("Auto Loader run finished.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many rows did this run actually append?
# MAGIC Reads the last progress entry from the query history, useful for the
# MAGIC "before / after the increment" comparison.

# COMMAND ----------

try:
    last_progress = query.recentProgress[-1] if query.recentProgress else {}
    sources = last_progress.get("sources", [{}])
    print("numInputRows (this run) :", last_progress.get("numInputRows"))
    print("batchId                 :", last_progress.get("batchId"))
    print("source description      :", sources[0].get("description"))
except Exception as e:
    print("No recent progress available:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the bronze table

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS row_count FROM {BRONZE_TABLE}"))
display(spark.sql(f"SELECT * FROM {BRONZE_TABLE} ORDER BY _ingest_ts DESC LIMIT 10"))
