from __future__ import annotations

import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client, LocalCluster

from benchmark.memory import peak_memory_bytes

CELL_SIZE_DEGREES = 0.01
INPUT_COLUMNS = ["TIMESTAMP", "MISSING_DATA", "POLYLINE"]
OUTPUT_COLUMNS = ["grid_lon", "grid_lat", "hour_utc"]


def _aggregate_partition(partition: pd.DataFrame) -> pd.DataFrame:
    rows: list[tuple[int, int, int]] = []
    for timestamp_value, missing_data, polyline in partition.itertuples(
        index=False, name=None
    ):
        if str(missing_data).strip().lower() != "false":
            continue
        try:
            timestamp = int(str(timestamp_value).strip())
            coordinates = json.loads(polyline)
            if not isinstance(coordinates, list) or not coordinates:
                continue
            point = coordinates[0]
            if not isinstance(point, list) or len(point) != 2:
                continue
            longitude, latitude = point
            if (
                isinstance(longitude, bool)
                or isinstance(latitude, bool)
                or not isinstance(longitude, (int, float))
                or not isinstance(latitude, (int, float))
                or not math.isfinite(longitude)
                or not math.isfinite(latitude)
                or not -180 <= longitude <= 180
                or not -90 <= latitude <= 90
            ):
                continue
        except (TypeError, ValueError, OverflowError, json.JSONDecodeError):
            continue

        try:
            hour_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc).hour
        except (OverflowError, OSError, ValueError):
            continue
        rows.append(
            (
                math.floor(longitude / CELL_SIZE_DEGREES),
                math.floor(latitude / CELL_SIZE_DEGREES),
                hour_utc,
            )
        )

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS, dtype="int64")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark a Dask spatial-temporal aggregation over train.csv."
    )
    parser.add_argument("--csv", type=Path, default=Path("/data/train.csv"))
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--blocksize", default="16 MiB")
    parser.add_argument("--split-out", type=int, default=2)
    parser.add_argument("--output", type=Path, default=Path("/results/dask.json"))
    args = parser.parse_args()
    if args.workers < 1 or args.split_out < 1:
        parser.error("--workers and --split-out must be positive")
    return args


def main() -> None:
    args = parse_args()
    if not args.csv.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {args.csv}")

    meta = pd.DataFrame(
        {
            "grid_lon": pd.Series(dtype="int64"),
            "grid_lat": pd.Series(dtype="int64"),
            "hour_utc": pd.Series(dtype="int64"),
        }
    )
    with LocalCluster(
        n_workers=args.workers,
        threads_per_worker=1,
        processes=True,
    ) as cluster, Client(cluster) as client:
        started = time.perf_counter()
        source = dd.read_csv(
            args.csv,
            usecols=INPUT_COLUMNS,
            dtype={column: "object" for column in INPUT_COLUMNS},
            keep_default_na=False,
            blocksize=args.blocksize,
        )
        trips = source.map_partitions(_aggregate_partition, meta=meta)
        counts = trips.groupby(OUTPUT_COLUMNS).size(split_out=args.split_out).compute()
        elapsed = time.perf_counter() - started
        memory_peak = peak_memory_bytes()

        result: dict[str, Any] = {
            "engine": "dask",
            "parallelism": args.workers,
            "elapsed_seconds": round(elapsed, 6),
            "peak_memory_bytes": memory_peak,
            "peak_memory_mib": round(memory_peak / (1024**2), 2),
            "memory_measurement": "container cgroup peak since container start",
            "valid_trip_count": int(counts.sum()),
            "aggregate_group_count": int(len(counts)),
            "input_file": args.csv.name,
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
