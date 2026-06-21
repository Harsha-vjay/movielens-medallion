-- =============================================================================
--  05 — Dashboard queries (AI/BI Dashboard, Databricks SQL)
-- =============================================================================
--  How to use:
--    1. Open *SQL Editor* in Databricks.
--    2. Make sure the catalog/schema in the upper-left matches the widgets
--       you used for the notebooks (default: workspace.movielens).
--    3. Copy each query block below into its own query tab, run it, then
--       *Add to dashboard* with the chart type listed in the comment header.
--
--  All queries read only from the **gold** layer (plus silver for the KPI
--  counters), so the dashboard reflects the latest pipeline run automatically.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Q1. kpi_overview              — visualization: 4 × KPI counter
--     One row, four columns. Drop on the dashboard as four counter widgets.
-- -----------------------------------------------------------------------------
SELECT
    (SELECT COUNT(*)                FROM workspace.movielens.silver_ratings) AS total_ratings,
    (SELECT COUNT(DISTINCT movieId) FROM workspace.movielens.silver_ratings) AS distinct_movies_rated,
    (SELECT COUNT(DISTINCT userId)  FROM workspace.movielens.silver_ratings) AS distinct_users,
    (SELECT ROUND(AVG(rating), 3)   FROM workspace.movielens.silver_ratings) AS overall_avg_rating;

-- -----------------------------------------------------------------------------
-- Q2. genre_avg_rating_trend    — visualization: LINE chart
--     X-axis: event_month   Y-axis: avg_rating   Series (color): genre
--     Filter out very small buckets so the line isn't dominated by noise.
-- -----------------------------------------------------------------------------
SELECT
    event_month,
    genre,
    avg_rating,
    rating_count
FROM workspace.movielens.gold_genre_trends
WHERE rating_count >= 20
ORDER BY event_month, genre;

-- -----------------------------------------------------------------------------
-- Q3. rating_count_by_genre     — visualization: BAR chart (horizontal)
--     X-axis: total_ratings    Y-axis: genre    Sort: desc
-- -----------------------------------------------------------------------------
SELECT
    genre,
    SUM(rating_count) AS total_ratings,
    ROUND(SUM(rating_count * avg_rating) / SUM(rating_count), 3) AS weighted_avg_rating
FROM workspace.movielens.gold_genre_trends
GROUP BY genre
ORDER BY total_ratings DESC;

-- -----------------------------------------------------------------------------
-- Q4. top_trending_movies       — visualization: TABLE (or horizontal BAR)
--     Shows the top 20 movies for the latest month in the gold table.
--     The dashboard will shift automatically after a new increment is loaded.
-- -----------------------------------------------------------------------------
SELECT
    event_month,
    rank,
    title,
    rating_count,
    avg_rating
FROM workspace.movielens.gold_trending_movies
WHERE event_month = (SELECT MAX(event_month) FROM workspace.movielens.gold_trending_movies)
ORDER BY rank;

-- -----------------------------------------------------------------------------
-- Q5. monthly_ratings_volume    — visualization: BAR chart
--     X-axis: event_month    Y-axis: ratings_in_month
--     Use this as the "before / after the increment" reference chart —
--     a new bar appears for the newly added month.
-- -----------------------------------------------------------------------------
SELECT
    event_month,
    COUNT(*) AS ratings_in_month
FROM workspace.movielens.silver_ratings
GROUP BY event_month
ORDER BY event_month;
