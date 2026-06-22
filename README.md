# MovieLens Medallion - A Databricks Project

An End-to-end Bronze / Silver / Gold (Medallion Architecture) pipeline on **Databricks** (serverless, Unity Catalog) using the **MovieLens ml-latest-small** dataset.

Two diverse sources are deliberately combined:

| # | Source        | Type            | Ingestion             | Role                                  |
|---|---------------|-----------------|-----------------------|---------------------------------------|
| 1 | `movies.csv`  | Object / batch  | One-shot batch read   | Static dimension                      |
| 2 | `ratings.csv` | File stream     | Auto Loader (`cloudFiles`, `availableNow=True`) | Streaming fact + the "add new data" test |

Both business objectives below are computed by **joining the two sources**.

---

## 1. Architecture

```
                  ┌──────────────────────────┐
                  │ /Volumes/<cat>/<sch>/raw │
                  │  ├─ movies/movies.csv    │  ← Source 1 (object/batch)
                  │  └─ ratings/landing/     │  ← Source 2 (stream landing dir)
                  │     │  ratings_initial.csv
                  │     │  ratings_inc_01.csv …
                  │     └─ _checkpoints/auto_loader_ratings/
                  └─────────────┬────────────┘
                                │
        ┌───────────────────────┼──────────────────────────┐
        │                       │                          │
        ▼ batch read            ▼ Auto Loader              │
  bronze_movies          bronze_ratings (append, +meta)    │
        │                       │                          │
        └──────────┬────────────┘                          │
                   ▼                                       │
              SILVER                                       │
   silver_movies (genre exploded)                          │
   silver_ratings (typed, deduped, filtered)               │
                   │                                       │
                   ▼                                       │
              GOLD (ratings & movies)                      │
   gold_genre_trends      ← Objective 1                    │
   gold_trending_movies   ← Objective 2                    │
                   │                                       │
                   ▼                                       │
          AI/BI Dashboard  ◀───────────────────────────────┘
                refreshed after each increment is dropped
```

---

## 2. Two business objectives → two sources

| Objective                                                         | Gold table            | Sources joined                              |
|-------------------------------------------------------------------|-----------------------|---------------------------------------------|
| **Genre engagement over time** (avg rating + rating count per genre per month) | `gold_genre_trends`   | `silver_ratings` & `silver_movies` (exploded genres) |
| **Trending titles per month** (top-20 movies by rating count, then avg rating) | `gold_trending_movies`| `silver_ratings` & `silver_movies`          |

Neither objective can be answered from a single source - that is the point.

---

## 3. Dataset & citation

- **MovieLens ml-latest-small** (GroupLens) - <https://files.grouplens.org/datasets/movielens/ml-latest-small.zip>
- F. Maxwell Harper and Joseph A. Konstan. 2015. *The MovieLens Datasets: History and Context.* ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4: 19:1–19:19. DOI: <https://doi.org/10.1145/2827872>

See [`CITATION.md`](CITATION.md) for the full reference and license note. The raw data files are **not** committed (see [`.gitignore`](.gitignore)).

---

## 4. Repo layout

```
movielens-medallion/
├── README.md                 ← you are here
├── CITATION.md
├── PROBLEMS.md               ← running problem log (rubric item)
├── PROMPTS.md                ← running AI-prompt log (rubric item)
├── .gitignore
├── data/
│   ├── increments/
│   ├── initial/
├── dashboard/
│   ├── MovieLens - Genre & Trending Dashboard.lvdash.json
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_bronze_movies.py
│   ├── 02_bronze_ratings_stream.py
│   ├── 03_silver.py
│   ├── 04_gold.py
├── scripts/
│   └── split_ratings.py      ← LOCAL pandas script - builds initial + monthly increments
└── screenshots/              ← submission screenshots (screenshots/README.md)
```

All notebooks are saved as Databricks **source** `.py` files (header `# Databricks notebook source`, cells separated by `# COMMAND ----------`), so they can be imported directly via *Workspace → Import → File*.

---

## 5. Parameters (every notebook)

Each notebook starts with three `dbutils.widgets.text(...)` calls defaulting to:

| Widget    | Default     |
|-----------|-------------|
| `catalog` | `workspace` |
| `schema`  | `movielens` |
| `volume`  | `raw`       |

All Unity Catalog objects and Volume paths are built from those widgets, so the project can be relocated by changing the widget values - no code edits.

---

## 6. RUNBOOK

### 6.1 One-time setup

1. **Locally**: download `ml-latest-small.zip`, unzip, and run:
   ```bash
   cd movielens-medallion/scripts
   python split_ratings.py
   ```
   This writes `data/initial/ratings_initial.csv` (earliest ~80 % of ratings) and `data/increments/ratings_inc_01.csv`, `_02.csv`, … (the remainder, split by month).

2. In Databricks (Free Edition, serverless):
   - *Workspace → Repos → Add → Git Folder* and point it at this repo (or *Import* the `notebooks/` folder).
   - Open `notebooks/00_setup.py`, attach to **Serverless**, **Run all**. This creates:
     - schema `workspace.movielens`
     - volume `workspace.movielens.raw`
     - sub-paths `/Volumes/workspace/movielens/raw/movies/`,
       `/Volumes/workspace/movielens/raw/ratings/landing/`,
       `/Volumes/workspace/movielens/raw/ratings/_checkpoints/auto_loader_ratings/`
   - *Catalog Explorer → workspace → movielens → raw → Upload*:
     - `movies.csv` → `movies/`
     - `ratings_initial.csv` → `ratings/landing/`

### 6.2 First full run

| Step | Notebook                          | What it does                                                                 |
|------|-----------------------------------|------------------------------------------------------------------------------|
| 1    | `00_setup.py`                     | Creates schema, volume, sub-folders. Prints resolved paths.                  |
| 2    | `01_bronze_movies.py`             | Batch read of `movies.csv` → `bronze_movies` (overwrite).                    |
| 3    | `02_bronze_ratings_stream.py`     | Auto Loader (`availableNow=True`) reads `ratings/landing/` → `bronze_ratings` (append). |
| 4    | `03_silver.py`                    | Builds `silver_movies` (genre exploded) and `silver_ratings` (typed, deduped, filtered). |
| 5    | `04_gold.py`                      | Builds `gold_genre_trends` and `gold_trending_movies` by joining the two silvers. |
| 6    | Dashboard                         | Open *SQL Editor*, paste queries from `notebooks/05_dashboard_queries.sql`, save each as a visual on a new AI/BI dashboard. |

### 6.3 Dashboard visuals (from `05_dashboard_queries.sql`)

| Query name                 | Chart type             | Source table            |
|----------------------------|------------------------|-------------------------|
| `kpi_overview`             | 4 KPI counters         | `silver_ratings`, `silver_movies` |
| `genre_avg_rating_trend`   | Line (x: month, y: avg, series: genre) | `gold_genre_trends` |
| `rating_count_by_genre`    | Bar (genre × count, all months) | `gold_genre_trends` |
| `top_trending_movies`      | Table (or horizontal bar) | `gold_trending_movies` |
| `monthly_ratings_volume`   | Bar (month × total ratings) | `silver_ratings`   |

### 6.4 THE TEST - "add new data → dashboard changes"

1. *Catalog Explorer*: upload `data/increments/ratings_inc_01.csv` into
   `/Volumes/workspace/movielens/raw/ratings/landing/`.
2. Re-run `02_bronze_ratings_stream.py`. Auto Loader's checkpoint guarantees **only the new file** is consumed - the cell output will show the new row count.
3. Re-run `03_silver.py` and `04_gold.py`.
4. Refresh the AI/BI dashboard.
5. Expected: `genre_avg_rating_trend` extends with a new month, `top_trending_movies` shifts, `kpi_overview` counts grow.

---

## 7. Screenshot list

See [`screenshots/README.md`](screenshots/README.md).

---

## 8. Idempotency & re-runnability

- All `CREATE`s use `IF NOT EXISTS`.
- Bronze movies, silver, and gold writes use `mode("overwrite")` with `overwriteSchema=true`.
- Bronze ratings uses Auto Loader **append** with a persistent checkpoint, so re-running `02` is a no-op unless new files have landed.
- Every notebook can be re-run end-to-end at any time.

---

- The dashboard cannot be shared directly because the project runs on Databricks Free Edition (single-user workspace, no external sharing). Its content is delivered as MovieLens_Dashboard_before.pdf + MovieLens_Dashboard_after.pdf.
