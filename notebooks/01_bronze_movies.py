# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze: movies (batch, object source)
# MAGIC
# MAGIC * **Source type**: object / batch (one CSV uploaded to a UC Volume).
# MAGIC * **Read**: single batch `spark.read.csv(...)`.
# MAGIC * **Write**: managed Delta table `bronze_movies`, **overwrite** with schema overwrite (idempotent — re-running just rewrites the same data).
# MAGIC * **Schema**: kept as-is from the file (`movieId INT`, `title STRING`, `genres STRING` — pipe-separated).
# MAGIC
# MAGIC No transformations here. Cleaning happens in the silver layer.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema",  "movielens", "Schema")
dbutils.widgets.text("volume",  "raw",       "Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")

SCHEMA_FQN     = f"`{CATALOG}`.`{SCHEMA}`"
BRONZE_TABLE   = f"{SCHEMA_FQN}.bronze_movies"
MOVIES_PATH    = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/movies/movies.csv"

print("Reading from :", MOVIES_PATH)
print("Writing to   :", BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read movies.csv
# MAGIC Explicit schema avoids the cost of inference and locks in the expected types.

# COMMAND ----------

movies_schema = StructType([
    StructField("movieId", IntegerType(), nullable=False),
    StructField("title",   StringType(),  nullable=True),
    StructField("genres",  StringType(),  nullable=True),
])

df_movies_raw = (
    spark.read
        .option("header", "true")
        .option("multiLine", "true")        # some titles contain commas in quotes
        .option("escape", '"')
        .schema(movies_schema)
        .csv(MOVIES_PATH)
)

print(f"Rows read: {df_movies_raw.count():,}")
display(df_movies_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add ingestion metadata
# MAGIC Useful for lineage and for spotting which run wrote each row.

# COMMAND ----------

df_bronze_movies = (
    df_movies_raw
        .withColumn("_ingest_ts",      F.current_timestamp())
        .withColumn("_source_file",    F.lit(MOVIES_PATH))
        .withColumn("_ingest_run_id",  F.lit(spark.sparkContext.applicationId))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write bronze_movies (overwrite)
# MAGIC Bronze for the dimension is a deterministic copy of the file, so `overwrite`
# MAGIC is the right semantics — re-running gives the same table.

# COMMAND ----------

(
    df_bronze_movies.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(BRONZE_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {BRONZE_TABLE}"))

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS row_count FROM {BRONZE_TABLE}"))
display(spark.sql(f"SELECT * FROM {BRONZE_TABLE} LIMIT 10"))
