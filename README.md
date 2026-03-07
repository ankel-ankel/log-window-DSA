# How to use this mess

> UI inspired by [keilerkonzept/sliding-topk-tui-demo](https://github.com/keilerkonzept/sliding-topk-tui-demo)
>
> Sample data from [Web Server Access Logs](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs) on Kaggle

## Data

Create a `data` folder in the project root and place the access log file downloaded from Kaggle inside it:

```
project/
├── data/
│   └── access.log
├── program/
│   └── ...
└── ...
```

## Run

```sh
go run run.go            # recommended (replay mode)
go run run.go fast       # fast (no replay, lower FPS)
```

## Metrics

- `records`: total ingested records.
- `throughput`: processing speed (records/sec).
- `refresh p95`: p95 duration of each Top-K ranking refresh.
- `replay position`: current timestamp in the replayed data (only shown in replay mode).
- `top-1`: current #1 item and count.
- `track`: current tracked item when `t` is enabled (`off` if tracking is disabled).

## Keys

- `p`: pause/resume.
- `t` or `Space`: track selected item.
- `s`: toggle linear/log scale.
- `q` or `Ctrl+C`: quit.
