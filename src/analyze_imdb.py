"""Download and analyze IMDb datasets for exploratory insights.

This script performs a streaming download of selected IMDb datasets, loads the
content with pandas using chunked processing to keep memory usage manageable,
and produces summary tables and figures describing trends in the catalog of
rated feature films. Generated artefacts are written into the ``reports``
directory.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import math
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn as sns

DATA_URLS: Dict[str, str] = {
    "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
}

DATA_DIR = Path("data")
REPORT_DIR = Path("reports")
FIGURE_DIR = REPORT_DIR / "figures"
SUMMARY_DIR = REPORT_DIR / "summaries"


def ensure_directories() -> None:
    """Create folders that store input data and generated artefacts."""
    for path in (DATA_DIR, FIGURE_DIR, SUMMARY_DIR):
        path.mkdir(parents=True, exist_ok=True)


def download_datasets() -> None:
    """Download the required IMDb datasets if they are not already present."""
    for filename, url in DATA_URLS.items():
        dest = DATA_DIR / filename
        if dest.exists():
            print(f"✅ {filename} already present, skipping download.")
            continue

        print(f"⬇️  Downloading {filename}...")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1_048_576):  # 1 MB
                if chunk:
                    fh.write(chunk)
        print(f"✅ Finished downloading {filename} ({dest.stat().st_size / 1_048_576:.1f} MB).")


def weighted_average(ratings: pd.Series, weights: pd.Series) -> float:
    total_weight = weights.sum()
    if total_weight == 0:
        return float("nan")
    return float((ratings * weights).sum() / total_weight)


def analyse() -> None:
    ensure_directories()
    download_datasets()

    ratings_path = DATA_DIR / "title.ratings.tsv.gz"
    basics_path = DATA_DIR / "title.basics.tsv.gz"

    print("📥 Loading ratings table into memory...")
    ratings_df = pd.read_csv(
        ratings_path,
        sep="\t",
        na_values="\\N",
        dtype={"tconst": "string", "averageRating": "float64", "numVotes": "int32"},
        compression="gzip",
    ).set_index("tconst")

    print("⚙️  Preparing aggregators...")
    movies_per_year = Counter()
    year_weighted_sums = defaultdict(lambda: {"weighted_sum": 0.0, "votes": 0})
    genre_stats = defaultdict(lambda: {"weighted_sum": 0.0, "votes": 0, "count": 0})
    runtime_minutes: list[float] = []
    top_titles_frames: list[pd.DataFrame] = []
    scatter_frames: list[pd.DataFrame] = []

    chunk_iter = pd.read_csv(
        basics_path,
        sep="\t",
        na_values="\\N",
        dtype={
            "tconst": "string",
            "titleType": "string",
            "primaryTitle": "string",
            "startYear": "string",
            "runtimeMinutes": "string",
            "genres": "string",
        },
        compression="gzip",
        chunksize=500_000,
    )

    processed_rows = 0
    next_milestone = 2_000_000
    for chunk in chunk_iter:
        processed_rows += len(chunk)
        movies = chunk[chunk["titleType"] == "movie"].copy()
        movies = movies.join(ratings_df, how="inner", on="tconst")

        movies["startYear"] = pd.to_numeric(movies["startYear"], errors="coerce")
        movies["runtimeMinutes"] = pd.to_numeric(movies["runtimeMinutes"], errors="coerce")

        year_counts = movies["startYear"].dropna().astype(int).value_counts()
        movies_per_year.update(year_counts.to_dict())

        valid_years = movies.dropna(subset=["startYear", "numVotes"]).copy()
        valid_years["startYear"] = valid_years["startYear"].astype(int)
        grouped_years = valid_years.groupby("startYear")
        for year, group in grouped_years:
            votes = group["numVotes"].astype(float)
            year_weighted_sums[year]["weighted_sum"] += float((group["averageRating"] * votes).sum())
            year_weighted_sums[year]["votes"] += int(votes.sum())

        runtime_minutes.extend(movies["runtimeMinutes"].dropna().tolist())

        top_titles_frames.append(
            movies[["tconst", "primaryTitle", "startYear", "averageRating", "numVotes"]]
        )

        popular_subset = movies[movies["numVotes"] >= 50_000][["averageRating", "numVotes"]]
        if not popular_subset.empty:
            scatter_frames.append(popular_subset)

        genre_chunk = movies.dropna(subset=["genres"]).copy()
        if not genre_chunk.empty:
            exploded = genre_chunk.assign(genre=genre_chunk["genres"].str.split(",")).explode("genre")
            exploded = exploded[exploded["genre"].notna() & (exploded["genre"] != "\\N")]
            for genre, group in exploded.groupby("genre"):
                votes = group["numVotes"].astype(float)
                genre_stats[genre]["weighted_sum"] += float((group["averageRating"] * votes).sum())
                genre_stats[genre]["votes"] += int(votes.sum())
                genre_stats[genre]["count"] += len(group)

        if processed_rows >= next_milestone:
            print(f"   ...processed {processed_rows:,} title basics rows")
            next_milestone += 2_000_000

    print(f"✅ Finished processing {processed_rows:,} rows from title.basics.tsv.gz")

    movies_per_year_df = (
        pd.DataFrame(sorted(movies_per_year.items()), columns=["year", "count"])
        .sort_values("year")
        .reset_index(drop=True)
    )
    movies_per_year_df = movies_per_year_df[movies_per_year_df["year"] >= 1900]

    yearly_rating_records = []
    for year, payload in year_weighted_sums.items():
        votes = payload["votes"]
        avg = payload["weighted_sum"] / votes if votes else float("nan")
        yearly_rating_records.append({"year": year, "weighted_average_rating": avg, "votes": votes})
    yearly_ratings_df = (
        pd.DataFrame(yearly_rating_records)
        .dropna(subset=["weighted_average_rating"])
        .sort_values("year")
        .reset_index(drop=True)
    )

    runtime_series = pd.Series(runtime_minutes)
    runtime_bins_df = pd.DataFrame()
    if not runtime_series.empty:
        max_runtime = float(runtime_series.max())
        runtime_edges = [0, 60, 75, 90, 105, 120, 150, 180]
        if max_runtime > runtime_edges[-1]:
            runtime_edges.append(max_runtime + 1)
        runtime_labels = [
            "< 60 min",
            "60-74",
            "75-89",
            "90-104",
            "105-119",
            "120-149",
            "150-179",
            "180+",
        ][: len(runtime_edges) - 1]
        runtime_bins = pd.cut(
            runtime_series.dropna(),
            bins=runtime_edges,
            labels=runtime_labels,
            right=False,
        )
        runtime_bins_df = (
            runtime_bins.value_counts(sort=False)
            .rename_axis("runtime_bin")
            .reset_index(name="count")
        )

    runtime_quantiles = runtime_series.quantile([0.1, 0.25, 0.5, 0.75, 0.9])
    long_runtime_share = float((runtime_series >= 120).sum() / len(runtime_series)) if len(runtime_series) else float('nan')
    genre_records = []
    for genre, payload in genre_stats.items():
        votes = payload["votes"]
        avg_rating = payload["weighted_sum"] / votes if votes else float("nan")
        genre_records.append(
            {
                "genre": genre,
                "title_count": payload["count"],
                "total_votes": votes,
                "weighted_average_rating": avg_rating,
            }
        )
    genre_df = (
        pd.DataFrame(genre_records)
        .dropna(subset=["weighted_average_rating"])
        .sort_values("total_votes", ascending=False)
        .reset_index(drop=True)
    )

    top_titles_df = (
        pd.concat(top_titles_frames, ignore_index=True)
        .dropna(subset=["numVotes"])
        .sort_values("numVotes", ascending=False)
        .drop_duplicates("tconst")
        .head(20)
    )

    scatter_df = (
        pd.concat(scatter_frames, ignore_index=True)
        if scatter_frames
        else pd.DataFrame(columns=["averageRating", "numVotes"])
    )

    popularity_df = pd.DataFrame()
    if not scatter_df.empty:
        vote_bins = [
            50_000,
            100_000,
            200_000,
            500_000,
            1_000_000,
            2_000_000,
            5_000_000,
            float("inf"),
        ]
        vote_labels = [
            "50k-100k",
            "100k-200k",
            "200k-500k",
            "500k-1M",
            "1M-2M",
            "2M-5M",
            "5M+",
        ]
        popularity_df = (
            scatter_df.assign(
                vote_band=pd.cut(
                    scatter_df["numVotes"],
                    bins=vote_bins,
                    labels=vote_labels,
                    right=False,
                )
            )
            .dropna(subset=["vote_band"])
            .groupby("vote_band", observed=False)
            .agg(
                title_count=("averageRating", "size"),
                avg_rating=("averageRating", "mean"),
                median_votes=("numVotes", "median"),
            )
            .reset_index()
        )

    print("🧮 Calculating summary statistics...")
    total_rated_movies = int(movies_per_year_df["count"].sum())
    median_runtime = float(runtime_series.median())
    overall_weighted_rating = weighted_average(
        ratings_df["averageRating"], ratings_df["numVotes"].astype(float)
    )

    summaries = {
        "total_rated_movies": total_rated_movies,
        "median_runtime": median_runtime,
        "overall_weighted_rating": overall_weighted_rating,
        "runtime_p10": float(runtime_quantiles.loc[0.1]) if not math.isnan(runtime_quantiles.loc[0.1]) else float('nan'),
        "runtime_p25": float(runtime_quantiles.loc[0.25]) if not math.isnan(runtime_quantiles.loc[0.25]) else float('nan'),
        "runtime_p75": float(runtime_quantiles.loc[0.75]) if not math.isnan(runtime_quantiles.loc[0.75]) else float('nan'),
        "runtime_p90": float(runtime_quantiles.loc[0.9]) if not math.isnan(runtime_quantiles.loc[0.9]) else float('nan'),
        "share_over_120_min": long_runtime_share,
    }
    summary_path = SUMMARY_DIR / "high_level_metrics.json"
    pd.Series(summaries).to_json(summary_path, indent=2)
    movies_per_year_df.to_csv(SUMMARY_DIR / "movies_per_year.csv", index=False)
    yearly_ratings_df.to_csv(SUMMARY_DIR / "yearly_weighted_ratings.csv", index=False)
    genre_df.to_csv(SUMMARY_DIR / "genre_weighted_ratings.csv", index=False)
    top_titles_df.to_csv(SUMMARY_DIR / "top_20_by_votes.csv", index=False)
    if not runtime_bins_df.empty:
        runtime_bins_df.to_csv(SUMMARY_DIR / "runtime_distribution.csv", index=False)
    if not popularity_df.empty:
        popularity_df.to_csv(SUMMARY_DIR / "popularity_by_votes.csv", index=False)

    print("📊 Creating figures...")
    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=movies_per_year_df, x="year", y="count")
    plt.title("Proliferation of Rated Feature Films in IMDb (1900-2023)")
    plt.xlabel("Release Year")
    plt.ylabel("Number of Rated Movies")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "movies_per_year.png", dpi=150)
    plt.close()

    top_genres = genre_df.head(10)
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=top_genres,
        x="weighted_average_rating",
        y="genre",
        hue="genre",
        palette="viridis",
        legend=False,
    )
    plt.title("Top Genres by Weighted IMDb Rating (Votes ≥ aggregated across titles)")
    plt.xlabel("Weighted Average Rating")
    plt.ylabel("Genre")
    plt.xlim(5.0, 8.5)
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "top_genres_weighted_rating.png", dpi=150)
    plt.close()

    plt.figure(figsize=(10, 6))
    sns.histplot(runtime_series.dropna(), bins=60, color="#3182bd")
    plt.title("Distribution of Feature Film Runtime Minutes")
    plt.xlabel("Runtime (minutes)")
    plt.ylabel("Number of Movies")
    plt.tight_layout()
    plt.savefig(FIGURE_DIR / "runtime_distribution.png", dpi=150)
    plt.close()

    if not scatter_df.empty:
        plt.figure(figsize=(10, 6))
        sns.scatterplot(
            data=scatter_df,
            x="numVotes",
            y="averageRating",
            alpha=0.4,
            edgecolor=None,
        )
        plt.xscale("log")
        plt.ylim(4, 10)
        plt.title("How IMDb Popularity Relates to Audience Ratings")
        plt.xlabel("Number of Votes (log scale)")
        plt.ylabel("Average Rating")
        plt.tight_layout()
        plt.savefig(FIGURE_DIR / "rating_vs_votes.png", dpi=150)
        plt.close()

    print("✅ Analysis complete. Artefacts stored in the reports/ directory.")


if __name__ == "__main__":
    analyse()
