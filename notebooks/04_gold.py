# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold (business objectives)
# MAGIC
# MAGIC Both objectives **join the two diverse sources** via the silver layer:
# MAGIC `silver_ratings` (streamed) × `silver_movies` (batch dimension, genre exploded).
# MAGIC
# MAGIC | # | Gold table              | Grain                              | Measures                                          |
# MAGIC |---|-------------------------|------------------------------------|---------------------------------------------------|
# MAGIC | 1 | `gold_genre_trends`     | `(genre, event_month)`             | `avg_rating`, `rating_count`                      |
# MAGIC | 2 | `gold_trending_movies`  | `(event_month, movieId)` top-20    | `rating_count`, `avg_rating`, `rank`              |
# MAGIC
# MAGIC Both tables are recomputed and overwritten on every run, so they always
# MAGIC reflect the latest state of silver.

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
SILVER_MOVIES_TABLE     = f"{SCHEMA_FQN}.silver_movies"
SILVER_RATINGS_TABLE    = f"{SCHEMA_FQN}.silver_ratings"
GOLD_GENRE_TRENDS       = f"{SCHEMA_FQN}.gold_genre_trends"
GOLD_TRENDING_MOVIES    = f"{SCHEMA_FQN}.gold_trending_movies"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective 1 — gold_genre_trends
# MAGIC
# MAGIC Per `(genre, event_month)`:
# MAGIC - `rating_count` — total number of ratings.
# MAGIC - `avg_rating`   — average rating (rounded to 3 dp).
# MAGIC
# MAGIC Joins `silver_ratings` with the **exploded** `silver_movies`, so a film
# MAGIC tagged with 3 genres contributes a rating to all 3 genre-buckets.

# COMMAND ----------

silver_ratings = spark.table(SILVER_RATINGS_TABLE)
silver_movies  = spark.table(SILVER_MOVIES_TABLE)

genre_trends = (
    silver_ratings.alias("r")
        .join(silver_movies.alias("m"), on="movieId", how="inner")
        .groupBy("m.genre", "r.event_month")
        .agg(
            F.count(F.lit(1)).alias("rating_count"),
            F.round(F.avg("r.rating"), 3).alias("avg_rating"),
        )
        .select(
            F.col("m.genre").alias("genre"),
            F.col("r.event_month").alias("event_month"),
            "rating_count",
            "avg_rating",
        )
)

(
    genre_trends.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_GENRE_TRENDS)
)

display(spark.sql(f"""
    SELECT *
    FROM {GOLD_GENRE_TRENDS}
    ORDER BY event_month DESC, rating_count DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objective 2 — gold_trending_movies
# MAGIC
# MAGIC Per `event_month`, rank movies by `rating_count` (desc), then `avg_rating`
# MAGIC (desc) as a tiebreaker, and keep the top 20 per month with their `rank`.

# COMMAND ----------

# Dedupe movie titles (silver_movies has one row per genre).
movie_titles = silver_movies.select("movieId", "title").dropDuplicates(["movieId"])

monthly_movie_stats = (
    silver_ratings
        .groupBy("movieId", "event_month")
        .agg(
            F.count(F.lit(1)).alias("rating_count"),
            F.round(F.avg("rating"), 3).alias("avg_rating"),
        )
        .join(movie_titles, on="movieId", how="left")
)

w = (
    Window
        .partitionBy("event_month")
        .orderBy(F.col("rating_count").desc(), F.col("avg_rating").desc(), F.col("movieId").asc())
)

trending = (
    monthly_movie_stats
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 20)
        .select("event_month", "rank", "movieId", "title", "rating_count", "avg_rating")
)

(
    trending.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TRENDING_MOVIES)
)

display(spark.sql(f"""
    SELECT *
    FROM {GOLD_TRENDING_MOVIES}
    WHERE event_month = (SELECT MAX(event_month) FROM {GOLD_TRENDING_MOVIES})
    ORDER BY rank
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick row-count summary

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'gold_genre_trends'    AS table, COUNT(*) AS rows FROM {GOLD_GENRE_TRENDS}
    UNION ALL
    SELECT 'gold_trending_movies' AS table, COUNT(*) AS rows FROM {GOLD_TRENDING_MOVIES}
"""))
