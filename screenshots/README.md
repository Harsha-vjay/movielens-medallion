# Screenshots list

| #  | File name                          | What it captures                                                                                  |
|----|------------------------------------|--------------------------------------------------------------------------------------------------|
| 1  | `01_catalog_explorer.jpg`          | *Catalog Explorer* showing the `workspace.movielens` schema with the four tables and the `raw` volume. |
| 2  | `02_volume_layout.jpg`             | The `raw` Volume expanded, showing `movies/`, `ratings/landing/`, `ratings/_checkpoints/`.       |
| 3  | `03_bronze_movies_count.jpg`       | Cell output from `01_bronze_movies.py` showing the row count and sample rows.                    |
| 4  | `04_autoloader_stream_output.jpg`  | The cell in `02_bronze_ratings_stream.py` that prints `numInputRows` and `batchId` after a run. |
| 5  | `05_silver_ratings_summary.jpg`    | The summary cell in `03_silver.py` (`distinct_users`, `distinct_movies`, date range, avg).      |
| 6  | `06_gold_genre_trends.jpg`         | `display(...)` of `gold_genre_trends` from `04_gold.py`.                                         |
| 7  | `07_gold_trending_movies.jpg`      | `display(...)` of `gold_trending_movies` for the latest month.                                   |
| 8  | `08_dashboard_before_increment.pdf`          | Full AI/BI dashboard *before* loading any increment.                                             |
| 9  | `09_landing_folder_with_increment.jpg` | The landing folder in *Catalog Explorer* showing `ratings_inc_01.csv` newly uploaded.        |
| 10 | `10_autoloader_only_new_file.jpg`  | `02_bronze_ratings_stream.py` rerun: cell output showing only the new file was consumed.        |
| 11 | `11_dashboard_after_increment.pdf`           | The same dashboard *after* the increment - line chart extended, trending list shifted, KPIs grew. |
| 12 | `12_git_folder_connected.jpg`      | *Workspace → Repos* showing this repo connected as a Git Folder.                                 |


