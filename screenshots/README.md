# Screenshots checklist

Save every screenshot listed below into this folder using the suggested file
name. Use PNG. The grader maps each screenshot directly to a rubric line item.

| #  | File name                          | What to capture                                                                                  |
|----|------------------------------------|--------------------------------------------------------------------------------------------------|
| 1  | `01_catalog_explorer.png`          | *Catalog Explorer* showing the `workspace.movielens` schema with the four tables and the `raw` volume. |
| 2  | `02_volume_layout.png`             | The `raw` Volume expanded, showing `movies/`, `ratings/landing/`, `ratings/_checkpoints/`.       |
| 3  | `03_bronze_movies_count.png`       | Cell output from `01_bronze_movies.py` showing the row count and sample rows.                    |
| 4  | `04_autoloader_stream_output.png`  | The cell in `02_bronze_ratings_stream.py` that prints `numInputRows` and `batchId` after a run. |
| 5  | `05_silver_ratings_summary.png`    | The summary cell in `03_silver.py` (`distinct_users`, `distinct_movies`, date range, avg).      |
| 6  | `06_gold_genre_trends.png`         | `display(...)` of `gold_genre_trends` from `04_gold.py`.                                         |
| 7  | `07_gold_trending_movies.png`      | `display(...)` of `gold_trending_movies` for the latest month.                                   |
| 8  | `08_dashboard_before.png`          | Full AI/BI dashboard *before* loading any increment.                                             |
| 9  | `09_landing_folder_with_increment.png` | The landing folder in *Catalog Explorer* showing `ratings_inc_01.csv` newly uploaded.        |
| 10 | `10_autoloader_only_new_file.png`  | `02_bronze_ratings_stream.py` rerun: cell output showing only the new file was consumed.        |
| 11 | `11_dashboard_after.png`           | The same dashboard *after* the increment — line chart extended, trending list shifted, KPIs grew. |
| 12 | `12_git_folder_connected.png`      | *Workspace → Repos* showing this repo connected as a Git Folder.                                 |

Tips:

- For "before" / "after" pairs, keep the same dashboard filters/zoom so the
  comparison is unambiguous.
- Crop to the relevant region; full-screen captures lose detail.
- Put the screenshots in this folder so they are tracked alongside the code
  (only the screenshots themselves are tracked — `data/` and the raw CSVs are
  in `.gitignore`).
