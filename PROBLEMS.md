# Problem Log

A running log of issues hit while building / running this project. 

| Date       | Layer / Step                          | Problem                                                                                  | Root cause                                                                                              | Fix                                                                                                                                | Status   |
|------------|---------------------------------------|------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|----------|
| 2026-06-19 | `02_bronze_ratings_stream` / Auto Loader | First run failed with `Path does not exist: /Volumes/workspace/movielens/raw/ratings/landing`. | Forgot to run `00_setup.py` before `02`, so the landing folder and checkpoint directory had not been created. | Ran `00_setup.py` first (it does `dbutils.fs.mkdirs` on the landing + checkpoint paths), then re-ran `02`. Added a precondition note in the README runbook. | Resolved |
| 2026-06-19 | `03_silver` / `silver_movies`         | `genre` column contained the literal string `(no genres listed)` after the explode.       | MovieLens uses that sentinel when a film has no genres; splitting on `|` returns it as a single token.  | Added a `WHERE genre <> '(no genres listed)'` filter after the explode and recorded the count of dropped rows in the cell output.   | Resolved |
| 2026-06-19 | `04_gold` / `gold_trending_movies`    | Re-running the notebook duplicated rows in the gold table.                              | Used `mode("append")` instead of `mode("overwrite")` - gold is a deterministic recomputation, so append is wrong. | Switched gold writes to `mode("overwrite").option("overwriteSchema","true")` and documented idempotency in the README.              | Resolved |


