# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver
# MAGIC
# MAGIC Cleans both bronze tables into analytics-ready silver tables.
# MAGIC
# MAGIC | Silver table     | Built from        | Transformations                                                                 |
# MAGIC |------------------|-------------------|--------------------------------------------------------------------------------|
# MAGIC | `silver_movies`  | `bronze_movies`   | dedupe on `movieId`; **explode pipe-separated `genres` → one row per `(movieId, genre)`**; drop the `(no genres listed)` sentinel. |
# MAGIC | `silver_ratings` | `bronze_ratings`  | cast Unix `timestamp` → `event_time` / `event_date` / `event_month`; filter rating to `[0.5, 5.0]`; drop nulls; dedupe on `(userId, movieId)` keeping the latest `event_time`. |
# MAGIC
# MAGIC Both writes use `overwrite` because silver is a deterministic re-derivation
# MAGIC of bronze — idempotent and safe to re-run after each bronze update.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema",  "movielens", "Schema")
dbutils.widgets.text("volume",  "raw",       "Volume")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
VOLUME  = dbutils.widgets.get("volume")

SCHEMA_FQN              = f"`{CATALOG}`.`{SCHEMA}`"
BRONZE_MOVIES_TABLE     = f"{SCHEMA_FQN}.bronze_movies"
BRONZE_RATINGS_TABLE    = f"{SCHEMA_FQN}.bronze_ratings"
SILVER_MOVIES_TABLE     = f"{SCHEMA_FQN}.silver_movies"
SILVER_RATINGS_TABLE    = f"{SCHEMA_FQN}.silver_ratings"

print("Reading:", BRONZE_MOVIES_TABLE, "+", BRONZE_RATINGS_TABLE)
print("Writing:", SILVER_MOVIES_TABLE, "+", SILVER_RATINGS_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. silver_movies — dedupe, explode genres, drop sentinel
# MAGIC
# MAGIC MovieLens uses the literal string `(no genres listed)` when a film has no
# MAGIC genres assigned. We drop those rows after the explode and log the count.

# COMMAND ----------

bronze_movies = spark.table(BRONZE_MOVIES_TABLE)

# Dedupe on movieId — bronze is overwritten on each run, but be defensive.
deduped_movies = (
    bronze_movies
        .dropDuplicates(["movieId"])
        .select("movieId", "title", "genres")
)

# Explode pipe-separated genres into one row per (movieId, genre).
movies_exploded = (
    deduped_movies
        .withColumn("genre", F.explode(F.split(F.col("genres"), r"\|")))
        .withColumn("genre", F.trim(F.col("genre")))
)

dropped_no_genres = movies_exploded.filter(F.col("genre") == "(no genres listed)").count()
print(f"Dropping {dropped_no_genres:,} '(no genres listed)' rows.")

silver_movies = (
    movies_exploded
        .filter(F.col("genre").isNotNull())
        .filter(F.col("genre") != "")
        .filter(F.col("genre") != "(no genres listed)")
        .select("movieId", "title", "genre")
)

(
    silver_movies.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_MOVIES_TABLE)
)

display(spark.sql(f"""
    SELECT
        COUNT(*)                              AS rows,
        COUNT(DISTINCT movieId)               AS distinct_movies,
        COUNT(DISTINCT genre)                 AS distinct_genres
    FROM {SILVER_MOVIES_TABLE}
"""))
display(spark.sql(f"SELECT * FROM {SILVER_MOVIES_TABLE} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. silver_ratings — type, filter, dedupe
# MAGIC
# MAGIC Steps:
# MAGIC 1. Drop bronze metadata columns.
# MAGIC 2. Cast Unix-epoch-seconds `timestamp` → `event_time` (timestamp), and
# MAGIC    derive `event_date` (date) and `event_month` (`YYYY-MM-01` date).
# MAGIC 3. Filter `rating` to the valid MovieLens range `[0.5, 5.0]`.
# MAGIC 4. Drop nulls on key columns.
# MAGIC 5. Dedupe on `(userId, movieId)` — if a user rated the same movie twice,
# MAGIC    keep the latest `event_time`.

# COMMAND ----------

bronze_ratings = spark.table(BRONZE_RATINGS_TABLE)

typed_ratings = (
    bronze_ratings
        .select("userId", "movieId", "rating", "timestamp")
        .withColumn("event_time",  F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
        .withColumn("event_date",  F.to_date(F.col("event_time")))
        .withColumn("event_month", F.trunc(F.col("event_date"), "month"))
)

filtered_ratings = (
    typed_ratings
        .filter(F.col("userId").isNotNull())
        .filter(F.col("movieId").isNotNull())
        .filter(F.col("rating").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter((F.col("rating") >= F.lit(0.5)) & (F.col("rating") <= F.lit(5.0)))
)

# Dedupe — latest rating per (userId, movieId)
w = Window.partitionBy("userId", "movieId").orderBy(F.col("event_time").desc())
silver_ratings = (
    filtered_ratings
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select("userId", "movieId", "rating", "event_time", "event_date", "event_month")
)

(
    silver_ratings.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_RATINGS_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify silver_ratings

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*)                                       AS rows,
        COUNT(DISTINCT userId)                         AS distinct_users,
        COUNT(DISTINCT movieId)                        AS distinct_movies,
        MIN(event_date)                                AS first_date,
        MAX(event_date)                                AS last_date,
        ROUND(AVG(rating), 3)                          AS avg_rating
    FROM {SILVER_RATINGS_TABLE}
"""))
display(spark.sql(f"SELECT * FROM {SILVER_RATINGS_TABLE} ORDER BY event_time DESC LIMIT 10"))
