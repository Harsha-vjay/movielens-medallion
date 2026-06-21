# Citation & License

## Dataset

**MovieLens ml-latest-small** — produced by the GroupLens research group at the University of Minnesota.

Download URL: <https://files.grouplens.org/datasets/movielens/ml-latest-small.zip>

The package used in this project contains, among other files:

- `movies.csv` — columns: `movieId`, `title`, `genres` (pipe-separated)
- `ratings.csv` — columns: `userId`, `movieId`, `rating` (0.5–5.0 in 0.5 steps), `timestamp` (Unix epoch seconds, UTC)

## Required citation

> F. Maxwell Harper and Joseph A. Konstan. 2015. **The MovieLens Datasets: History and Context.** *ACM Transactions on Interactive Intelligent Systems (TiiS)* 5, 4: Article 19 (December 2015), 19 pages. DOI: <https://doi.org/10.1145/2827872>

BibTeX:

```bibtex
@article{harper2015movielens,
  author    = {Harper, F. Maxwell and Konstan, Joseph A.},
  title     = {The {MovieLens} Datasets: History and Context},
  journal   = {ACM Transactions on Interactive Intelligent Systems (TiiS)},
  volume    = {5},
  number    = {4},
  pages     = {19:1--19:19},
  year      = {2015},
  publisher = {ACM},
  doi       = {10.1145/2827872}
}
```

## License / usage note

The MovieLens datasets are released by GroupLens under their own usage terms. From the dataset README:

- The data may be used for **research, education, and personal projects**.
- The data **may not be redistributed without permission** from GroupLens, and downstream users must **preserve the attribution** above.

For this reason the raw CSV files (and the zip) are **not** committed to this repository — see [`.gitignore`](.gitignore). Every notebook in this project loads the data from a Unity Catalog Volume that the grader populates manually following the [README runbook](README.md#6-runbook).

Always refer to the official GroupLens page for the latest terms: <https://grouplens.org/datasets/movielens/>.
