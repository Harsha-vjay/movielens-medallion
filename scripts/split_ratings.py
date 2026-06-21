"""
split_ratings.py
================

LOCAL helper script — runs on your laptop with **pandas only** (no Spark, no
Databricks). It splits MovieLens `ratings.csv` into:

* `data/initial/ratings_initial.csv`   — earliest ~80 % of ratings (by timestamp)
* `data/increments/ratings_inc_01.csv`, `_02.csv`, ...
                                       — the remaining ~20 %, one CSV per month

The initial file is what you upload to the Auto Loader landing folder during
the one-time setup. The increment files are what you drop in, one at a time,
to demonstrate "add new data → dashboard changes".

USAGE
-----
1. Download `ml-latest-small.zip` from
       https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
   and unzip it. You should now have a `ml-latest-small/ratings.csv`.

2. From this folder:
       python split_ratings.py --ratings /path/to/ml-latest-small/ratings.csv
   (or, if `ratings.csv` lives next to this script, just `python split_ratings.py`)

3. Files appear under `./data/initial/` and `./data/increments/` relative
   to where you ran the script. `data/` is git-ignored (see .gitignore).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

INITIAL_FRACTION = 0.80  # earliest 80% goes to the initial load


def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    default_input  = here / "ratings.csv"
    default_outdir = here.parent / "data"   # movielens-medallion/data/
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ratings", type=Path, default=default_input,
                   help=f"Path to MovieLens ratings.csv (default: {default_input})")
    p.add_argument("--outdir",  type=Path, default=default_outdir,
                   help=f"Output base directory (default: {default_outdir})")
    p.add_argument("--initial-fraction", type=float, default=INITIAL_FRACTION,
                   help=f"Fraction of earliest ratings to put in the initial load (default: {INITIAL_FRACTION})")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if not args.ratings.is_file():
        print(f"ERROR: ratings.csv not found at {args.ratings}", file=sys.stderr)
        print("Pass --ratings /path/to/ratings.csv, or unzip ml-latest-small "
              "next to this script.", file=sys.stderr)
        return 2

    initial_dir = args.outdir / "initial"
    inc_dir     = args.outdir / "increments"
    initial_dir.mkdir(parents=True, exist_ok=True)
    inc_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading {args.ratings} ...")
    df = pd.read_csv(args.ratings)
    expected = {"userId", "movieId", "rating", "timestamp"}
    missing = expected - set(df.columns)
    if missing:
        print(f"ERROR: missing expected columns: {sorted(missing)}", file=sys.stderr)
        return 3
    print(f"  rows: {len(df):,}")

    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    # ---- initial load = earliest N% of rows by timestamp ---------------------
    cutoff = int(len(df) * args.initial_fraction)
    initial_df = df.iloc[:cutoff].copy()
    tail_df    = df.iloc[cutoff:].copy()
    print(f"  initial rows : {len(initial_df):,}  (earliest {args.initial_fraction:.0%})")
    print(f"  tail rows    : {len(tail_df):,}     (split into monthly increments)")

    initial_path = initial_dir / "ratings_initial.csv"
    initial_df.to_csv(initial_path, index=False)
    print(f"Wrote {initial_path}  ({len(initial_df):,} rows)")

    # ---- monthly increments --------------------------------------------------
    tail_df["__month"] = pd.to_datetime(tail_df["timestamp"], unit="s", utc=True).dt.to_period("M")
    months = sorted(tail_df["__month"].unique())
    if not months:
        print("No tail rows — increment files not produced. "
              "(Lower --initial-fraction if you want some.)")
        return 0

    print(f"  tail spans {len(months)} month(s): {months[0]} ... {months[-1]}")
    for i, m in enumerate(months, start=1):
        chunk = tail_df.loc[tail_df["__month"] == m].drop(columns="__month")
        out = inc_dir / f"ratings_inc_{i:02d}.csv"
        chunk.to_csv(out, index=False)
        print(f"Wrote {out}  ({len(chunk):,} rows)  — month {m}")

    print()
    print("DONE. Next steps:")
    print(f"  1. Upload {initial_path}")
    print( "     to /Volumes/<catalog>/<schema>/<volume>/ratings/landing/")
    print( "     via Catalog Explorer in Databricks.")
    print( "  2. Run notebooks 00 → 01 → 02 → 03 → 04 and build the dashboard.")
    print( "  3. To run THE TEST, drop one increment file (e.g. ratings_inc_01.csv)")
    print( "     into the same landing folder, then re-run 02 → 03 → 04 and")
    print( "     refresh the dashboard.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
