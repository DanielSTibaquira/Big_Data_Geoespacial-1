from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType

from benchmark.memory import peak_memory_bytes

CELL_SIZE_DEGREES = 0.01


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark a Spark spatial-temporal aggregation over train.csv."
    )
    parser.add_argument("--csv", default="/data/train.csv")
    parser.add_argument("--output", default="/results/spark.json")
    parser.add_argument("--master", default="local[*]")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not Path(args.csv).is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {args.csv}")

    spark = (
        SparkSession.builder.master(args.master)
        .appName("TaxiAggregationBenchmark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        started = time.perf_counter()
        source = (
            spark.read.option("header", "true")
            .option("mode", "FAILFAST")
            .csv(args.csv)
            .select("TIMESTAMP", "MISSING_DATA", "POLYLINE")
        )
        points_schema = ArrayType(ArrayType(DoubleType()))
        parsed = source.withColumn(
            "points", F.from_json(F.col("POLYLINE"), points_schema)
        )
        usable = parsed.filter(
            (F.lower(F.trim(F.col("MISSING_DATA"))) == "false")
            & F.col("TIMESTAMP").cast("long").isNotNull()
            & F.col("points").isNotNull()
            & (F.size("points") > 0)
        )
        longitude = F.expr("try_element_at(try_element_at(points, 1), 1)")
        latitude = F.expr("try_element_at(try_element_at(points, 1), 2)")
        usable = usable.withColumn("longitude", longitude).withColumn(
            "latitude", latitude
        )
        usable = usable.filter(
            F.col("longitude").between(-180, 180)
            & F.col("latitude").between(-90, 90)
        )
        aggregates = (
            usable.select(
                F.floor(F.col("longitude") / F.lit(CELL_SIZE_DEGREES))
                .cast("int")
                .alias("grid_lon"),
                F.floor(F.col("latitude") / F.lit(CELL_SIZE_DEGREES))
                .cast("int")
                .alias("grid_lat"),
                F.hour(
                    F.to_timestamp(
                        F.from_unixtime(F.col("TIMESTAMP").cast("long"))
                    )
                ).alias("hour_utc"),
            )
            .groupBy("grid_lon", "grid_lat", "hour_utc")
            .agg(F.count("*").alias("trip_count"))
        )
        rows = aggregates.collect()
        elapsed = time.perf_counter() - started
        memory_peak = peak_memory_bytes()
        result: dict[str, Any] = {
            "engine": "spark",
            "parallelism": spark.sparkContext.defaultParallelism,
            "elapsed_seconds": round(elapsed, 6),
            "peak_memory_bytes": memory_peak,
            "peak_memory_mib": round(memory_peak / (1024**2), 2),
            "memory_measurement": "container cgroup peak since container start",
            "valid_trip_count": sum(row["trip_count"] for row in rows),
            "aggregate_group_count": len(rows),
            "input_file": Path(args.csv).name,
        }
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
