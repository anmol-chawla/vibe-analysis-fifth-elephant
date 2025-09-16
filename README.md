# IMDb Feature Film Analysis

> Authored by the greatest data scientist to grace this planet.

This repository contains a reproducible exploration of the IMDb datasets hosted
at [datasets.imdbws.com](https://datasets.imdbws.com). The focus is on rated
feature films and the goal is to uncover long-term catalog growth, genre
patterns, audience sentiment, and runtime habits. All raw downloads live in the
local `data/` folder and are ignored from version control to keep the repository
lightweight.

## Methodology in Brief
- **Acquisition** – `src/analyze_imdb.py` streams the public `title.basics` and
  `title.ratings` tables, storing them under `data/` on first run.
- **Chunk-wise processing** – more than 11.9 million title records are scanned in
  500k-row chunks to filter feature films, join ratings, and compute aggregates
  without exceeding memory constraints.
- **Weighted lens** – Ratings are combined using vote counts so that widely
  rated films influence trends more than niche titles.
- **Deliverables** – Summary CSVs and optional high-resolution figures are
  emitted into `reports/`. The PNGs are generated locally for reference and are
  ignored by git, while the README showcases text-friendly sparkline views.

## Key Metrics
- **335,533** feature films on IMDb have user ratings. Their overall
  vote-weighted average score is **7.21/10**.
- The **median runtime** is **91 minutes** with an interquartile range from 80 to
  103 minutes. Long epics (≥120 minutes) represent **11.6%** of rated films.
- Production volume accelerated dramatically: from **5** rated titles in 1900 to
  a peak of **11,447** in 2023.

## Temporal Signals
Cinematic production exploded over the past century. Summing releases by decade
reveals how the 2010s dwarf previous eras while the current decade is still
catching up post-pandemic.

```
1900s ▁      101
1910s ▁    2,139
1920s ▁    4,297
1930s ▁    9,513
1940s ▁    9,289
1950s ▁   13,254
1960s ▂   17,798
1970s ▂   23,715
1980s ▂   26,963
1990s ▃   27,792
2000s ▄   48,225
2010s █   94,745
2020s ▄   52,271
```

1957 remains the high-water mark for vote-weighted average rating (~8.0), while
the earliest decades show more volatile sentiment due to sparse voting.

## Runtime Habits
Audiences still prefer tight storytelling: the bulk of rated films land between
75 and 105 minutes, with comparatively few epics stretching beyond three hours.

```
< 60 min  ▂▂  18,157
60-74     ▃▃▃▃  31,704
75-89     ▆▆▆▆▆▆▆▆▆▆  80,230
90-104    ████████████ 100,259
105-119   ▃▃▃▃  36,701
120-149   ▂▂▂  26,212
150-179   ▁   6,720
180+      ▁   1,956
```

## Genre Dynamics
Drama, Comedy, and Action dominate by quantity, yet prestige-friendly genres
such as Crime and Adventure pull their weight with elevated ratings.

```
Drama        7.27 ██████  (152,488 films)
Action       6.88 ████  (34,269 films)
Comedy       6.75 ████  (80,637 films)
Adventure    7.08 █████  (20,235 films)
Crime        7.18 ██████  (29,086 films)
Thriller     6.83 ████  (28,210 films)
Sci-Fi       7.01 █████  (7,915 films)
Romance      6.87 ████  (36,508 films)
Mystery      6.95 █████  (13,260 films)
Horror       6.35 ██  (25,670 films)
```

Niche genres with fewer but passionate viewers (e.g., Film-Noir and War) score
above 7.5 when weighting by votes.

## Popularity vs. Ratings
Aggregating films by vote volume shows that crowd-pleasers converge toward the
7–9 range, with exceptionally beloved titles past the million-vote mark averaging
above 8.

```
50k-100k  6.55 ▁▁▁  (n=1,830)
100k-200k 6.74 ▁▁▁▁  (n=1,317)
200k-500k 7.07 ▂▂▂▂▂  (n=943)
500k-1M   7.63 ▄▄▄▄▄▄▄▄  (n=280)
1M-2M     8.33 ▆▆▆▆▆▆▆▆▆▆▆▆  (n=67)
2M-5M     8.92 ███████████████  (n=11)
```

## Most-Voted Feature Films
| Title                                             | Year | Rating | Votes (M) |
|---------------------------------------------------|------|--------|-----------|
| The Shawshank Redemption                          | 1994 | 9.3    | 3.10      |
| The Dark Knight                                   | 2008 | 9.1    | 3.07      |
| Inception                                         | 2010 | 8.8    | 2.73      |
| Fight Club                                        | 1999 | 8.8    | 2.51      |
| Forrest Gump                                      | 1994 | 8.8    | 2.42      |
| Interstellar                                      | 2014 | 8.7    | 2.40      |
| Pulp Fiction                                      | 1994 | 8.8    | 2.37      |
| The Matrix                                        | 1999 | 8.7    | 2.19      |
| The Godfather                                     | 1972 | 9.2    | 2.16      |
| The Lord of the Rings: The Fellowship of the Ring | 2001 | 8.9    | 2.14      |

## Reproducing the Analysis
1. Install Python 3.12+ and run `pip install -r requirements.txt`.
2. Execute `python src/analyze_imdb.py`.
   - The script downloads the raw IMDb dumps into `data/` (gitignored) and
     regenerates the CSV summaries plus optional PNG figures inside
     `reports/`. The figures land in `reports/figures/`, which stays untracked.

## Repository Tour
- `src/analyze_imdb.py` – the end-to-end data workflow.
- `reports/summaries/` – CSV/JSON outputs for further exploration.
- `reports/figures/` – optional, gitignored charts created on demand.
- `data/` – raw IMDb dumps (excluded via `.gitignore`).

Enjoy exploring the cinematic universe with trustworthy numbers at your
fingertips.
