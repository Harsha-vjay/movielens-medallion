# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC
# MAGIC Creates the Unity Catalog schema, the managed Volume, and the sub-folders
# MAGIC inside the Volume that the rest of the pipeline expects:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/<volume>/
# MAGIC   ├─ movies/                                ← drop movies.csv here
# MAGIC   └─ ratings/
# MAGIC       ├─ landing/                           ← drop ratings_*.csv files here (Auto Loader watches this)
# MAGIC       └─ _checkpoints/auto_loader_ratings/  ← streaming checkpoint
# MAGIC ```
# MAGIC
# MAGIC Idempotent: safe to re-run; only creates what is missing.
# MAGIC
# MAGIC **Run on**: Databricks Free Edition, serverless.

# COMMAND ----------

# Parameters — change once via the widget bar at the top of the notebook,
# everything downstream is built from these three values.
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema",  "movielens", "Schema")
dbutils.widgets.text("volume",  "raw",       "Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")

# Fully-qualified names / paths derived from the widgets
SCHEMA_FQN = f"`{CATALOG}`.`{SCHEMA}`"
VOLUME_FQN = f"`{CATALOG}`.`{SCHEMA}`.`{VOLUME}`"
VOLUME_ROOT          = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
MOVIES_DIR           = f"{VOLUME_ROOT}/movies"
RATINGS_LANDING_DIR  = f"{VOLUME_ROOT}/ratings/landing"
RATINGS_CHECKPOINT   = f"{VOLUME_ROOT}/ratings/_checkpoints/auto_loader_ratings"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create catalog / schema / volume (idempotent)

# COMMAND ----------

# Free Edition gives you the `workspace` catalog out of the box — we only
# create the schema and the volume. If you change the catalog widget to a
# catalog you own, this still works.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_FQN}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create the folder layout inside the Volume
# MAGIC
# MAGIC `dbutils.fs.mkdirs` is a no-op if the folder already exists, so this is
# MAGIC idempotent too.

# COMMAND ----------

for path in (MOVIES_DIR, RATINGS_LANDING_DIR, RATINGS_CHECKPOINT):
    dbutils.fs.mkdirs(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Print resolved paths
# MAGIC
# MAGIC Copy these out of the cell output and use them when uploading files via
# MAGIC *Catalog Explorer → Upload to volume*.

# COMMAND ----------

print("Catalog                : ", CATALOG)
print("Schema (FQN)           : ", f"{CATALOG}.{SCHEMA}")
print("Volume (FQN)           : ", f"{CATALOG}.{SCHEMA}.{VOLUME}")
print("Volume root            : ", VOLUME_ROOT)
print("Movies upload dir      : ", MOVIES_DIR)
print("Ratings landing dir    : ", RATINGS_LANDING_DIR)
print("Ratings checkpoint dir : ", RATINGS_CHECKPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity check — list what is currently in the volume

# COMMAND ----------

display(dbutils.fs.ls(VOLUME_ROOT))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next steps
# MAGIC
# MAGIC 1. *Catalog Explorer → workspace → movielens → raw → movies* → **Upload** `movies.csv`.
# MAGIC 2. *Catalog Explorer → workspace → movielens → raw → ratings → landing* → **Upload** `ratings_initial.csv`
# MAGIC    (produced locally by `scripts/split_ratings.py`).
# MAGIC 3. Run `01_bronze_movies.py`, then `02_bronze_ratings_stream.py`.
