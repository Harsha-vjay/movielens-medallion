# AI Prompt Log

The course rubric requires that every prompt you give to an AI assistant during this project be submitted as **plain text**. Paste each prompt below in chronological order. Keep the original wording — do not paraphrase. If a prompt is long, keep it long; the grader needs the real prompt, not a summary.

Format:

```
### YYYY-MM-DD — short label
<the exact prompt you sent>
```

Optional: add a one-line *Outcome:* note after each block if the response materially changed your plan.

---

### 2026-06-19 — Scaffold the entire repo (initial prompt to Claude)

> @"C:\Users\harsh\Downloads\DM2-FinalProject.pdf"
> You are setting up a Databricks Medallion-architecture project for a university Data Management course. Build a clean, git-ready repository of Databricks-importable notebooks and supporting scripts/docs.
>
> CRITICAL CONSTRAINTS:
> - Target platform is Databricks FREE EDITION (serverless, Unity Catalog). It has NO personal access token / CLI / REST API access. So: produce FILES ONLY. Do NOT attempt to authenticate to Databricks, call the Databricks CLI/SDK/API, or run Spark locally. Everything is imported and run manually in the Databricks UI.
> - Write notebooks as Databricks source `.py` files (start each with `# Databricks notebook source`, separate cells with `# COMMAND ----------`). PySpark.
> - Parameterize catalog/schema/volume at the top of every notebook via dbutils.widgets, defaulting to CATALOG="workspace", SCHEMA="movielens", VOLUME="raw". Read widgets into variables; build all paths from them.
> - Make every notebook idempotent and re-runnable (CREATE ... IF NOT EXISTS, overwrite/merge where appropriate). Comment generously.
>
> DATASET (the only data source — cite it, do not invent data):
> - MovieLens ml-latest-small (GroupLens). Download: https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
> - Citation: F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM TiiS 5,4. DOI: 10.1145/2827872.
> - Files used: movies.csv (movieId, title, genres [pipe-separated]) and ratings.csv (userId, movieId, rating, timestamp [Unix epoch seconds]).
>
> TWO DIVERSE SOURCE TYPES (rubric requires diversity):
> - Source 1 — OBJECT/BATCH: movies.csv, uploaded to a UC Volume, batch-read into Bronze. This is the static dimension.
> - Source 2 — STREAM: ratings, ingested with Auto Loader (format("cloudFiles")) watching a landing folder in the Volume, using .trigger(availableNow=True), checkpoint inside the Volume. This is the streaming fact AND the mechanism for the grader's "add new data -> dashboard changes" test.
> - The two objectives MUST come from JOINING these two sources, not two unrelated charts.
>
> MEDALLION LAYERS:
> - Bronze: bronze_movies (batch, as-is), bronze_ratings (Auto Loader append, as-is + ingestion metadata).
> - Silver: silver_movies — dedupe; explode pipe-separated genres into one row per (movieId, genre); handle "(no genres listed)". silver_ratings — cast timestamp via from_unixtime/to_timestamp to event_time + event_date + event_month; filter rating to [0.5,5.0]; dedupe on (userId, movieId) keeping latest event_time; drop nulls.
> - Gold (the two business objectives, both joining ratings x movies):
>   * Objective 1 -> gold_genre_trends: per (genre, event_month): avg_rating, rating_count.
>   * Objective 2 -> gold_trending_movies: per event_month, rank movies by rating_count then avg_rating; keep top 20 with a rank column.
>
> REPO STRUCTURE:
> movielens-medallion/
>   README.md, CITATION.md, PROBLEMS.md, PROMPTS.md, .gitignore
>   notebooks/00_setup.py, 01_bronze_movies.py, 02_bronze_ratings_stream.py, 03_silver.py, 04_gold.py, 05_dashboard_queries.sql
>   scripts/split_ratings.py
>   screenshots/ (empty, with .gitkeep and a README listing screenshots to capture)
>
> DELIVERABLE DOCS — also write into README.md a numbered RUNBOOK:
> 1. Databricks one-time setup.
> 2. Run order 00 -> 01 -> 02 -> 03 -> 04 + dashboard build.
> 3. THE TEST (drop increment -> re-run 02/03/04 -> dashboard changes).
> 4. SCREENSHOT CHECKLIST.
>
> Start by creating the repo and all files. After scaffolding, walk me through the one-time Databricks setup step by step.

*Outcome:* Created the full repo scaffold under `movielens-medallion/`, then proceeded with the Databricks one-time setup walkthrough in chat.

---

<!-- Add subsequent prompts below as you keep working. Examples of things worth logging:
- Asking the AI to debug a specific PySpark error.
- Asking it to refactor a notebook cell.
- Asking it to draft dashboard query variants.
Each entry should be the exact prompt you sent. -->
