# How to use this

> UI inspired by [keilerkonzept/sliding-topk-tui-demo](https://github.com/keilerkonzept/sliding-topk-tui-demo)
>
> Sample data from [Web Server Access Logs](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs) on Kaggle

## Data

Place the access log file from Kaggle under `data/access.log`:

```
log-speed/
├── data/
│   └── access.log
├── program/
└── ...
```

## Run

Three demo UIs (one per method) and one benchmark:

```sh
go run base_1.go      # Naive UI
go run base_2.go      # Sliding Naive UI
go run proposed.go    # Proposed UI (HeavyKeeper + IncrementalRanker)
go run bench.go       # Benchmark all 3 → results/
```

## Methods

- **Naive** — hash map + full sort per query. No sliding window.
- **Sliding Naive** — ring buffer of per-tick maps, exact sliding window, full sort per query.
- **Proposed** — HeavyKeeper sliding sketch + IncrementalRanker.

## Layout

```
log-speed/
├── data/access.log
├── results/                 # bench output: chart.html, summary.txt, latency.csv
├── base_1.go                # entry: Naive UI
├── base_2.go                # entry: Sliding Naive UI
├── proposed.go              # entry: Proposed UI
├── bench.go                 # entry: bench all 3
└── program/
    ├── methods.go           # Method interface + Naive + SlidingNaive + Proposed
    ├── ui.go                # shared TUI
    └── bench.go             # bench harness
```

## UI metrics

- `records`: total ingested records.
- `throughput`: processing speed (records/sec).
- `replay position`: current timestamp in the replayed data.
- `top-1`: current #1 item and count.
- `track`: current tracked item when `t` is enabled (`off` if disabled).

## UI keys

- `p`: pause/resume.
- `t` or `Space`: track selected item.
- `s`: toggle linear/log scale.
- `q` or `Ctrl+C`: quit.

## Bench

Default reads the entire `data/access.log`. Limit with `-n`:

```sh
go run bench.go               # full file
go run bench.go -n 1000000    # limit to 1M records
```

Output lands in `results/`.
