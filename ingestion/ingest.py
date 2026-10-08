from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection

LOGGER = logging.getLogger("taxi_ingestion")
CSV_COLUMNS = [
    "TRIP_ID",
    "CALL_TYPE",
    "ORIGIN_CALL",
    "ORIGIN_STAND",
    "TAXI_ID",
    "TIMESTAMP",
    "DAY_TYPE",
    "MISSING_DATA",
    "POLYLINE",
]
OUTPUT_COLUMNS = [
    "_id",
    "trip_id",
    "call_type",
    "origin_call",
    "origin_stand",
    "taxi_id",
    "timestamp",
    "started_at",
    "day_type",
    "location",
    "destination",
    "point_count",
    "trip_duration_seconds",
]


def _parse_integer(value: Any, *, optional: bool = False) -> int | None:
    text = str(value).strip()
    if optional and not text:
        return None
    if not text:
        raise ValueError("expected an integer")
    return int(text)


def _parse_polyline(value: Any) -> tuple[list[list[float]] | None, str | None]:
    if not isinstance(value, str) or not value.strip():
        return None, "empty_polyline"

    try:
        points = json.loads(value)
    except json.JSONDecodeError:
        return None, "invalid_polyline"

    if not isinstance(points, list) or not points:
        return None, "empty_polyline"

    coordinates: list[list[float]] = []
    for point in points:
        if not isinstance(point, list) or len(point) != 2:
            return None, "invalid_coordinates"

        lon, lat = point
        if (
            isinstance(lon, bool)
            or isinstance(lat, bool)
            or not isinstance(lon, (int, float))
            or not isinstance(lat, (int, float))
            or not math.isfinite(lon)
            or not math.isfinite(lat)
            or not -180 <= lon <= 180
            or not -90 <= lat <= 90
        ):
            return None, "invalid_coordinates"

        coordinates.append([float(lon), float(lat)])

    return coordinates, None


def build_document(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    missing_data = str(row.get("MISSING_DATA", "")).strip().lower()
    if missing_data == "true":
        return None, "missing_data"
    if missing_data != "false":
        return None, "invalid_record"

    coordinates, polyline_error = _parse_polyline(row.get("POLYLINE"))
    if polyline_error:
        return None, polyline_error
    if coordinates is None:
        return None, "invalid_polyline"

    try:
        trip_id = str(row["TRIP_ID"]).strip()
        if not trip_id:
            raise ValueError("empty trip id")
        taxi_id = _parse_integer(row["TAXI_ID"])
        timestamp = _parse_integer(row["TIMESTAMP"])
        if taxi_id is None or timestamp is None:
            raise ValueError("required integer field is empty")
        started_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        origin_call = _parse_integer(row.get("ORIGIN_CALL", ""), optional=True)
        origin_stand = _parse_integer(row.get("ORIGIN_STAND", ""), optional=True)
    except (KeyError, TypeError, ValueError, OverflowError, OSError):
        return None, "invalid_record"

    origin = coordinates[0]
    destination = coordinates[-1]
    source_identity = json.dumps(
        [str(row.get(column, "")).strip() for column in CSV_COLUMNS],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    document_id = hashlib.sha256(source_identity.encode("utf-8")).hexdigest()
    return (
        {
            "_id": document_id,
            "trip_id": trip_id,
            "call_type": str(row.get("CALL_TYPE", "")).strip() or None,
            "origin_call": origin_call,
            "origin_stand": origin_stand,
            "taxi_id": taxi_id,
            "timestamp": timestamp,
            "started_at": started_at,
            "day_type": str(row.get("DAY_TYPE", "")).strip() or None,
            "location": {"type": "Point", "coordinates": origin},
            "destination": {"type": "Point", "coordinates": destination},
            "point_count": len(coordinates),
            "trip_duration_seconds": (len(coordinates) - 1) * 15,
        },
        None,
    )


def transform_partition(
    partition: pd.DataFrame,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    counters: Counter[str] = Counter(rows_read=len(partition))
    documents: list[dict[str, Any]] = []

    for row in partition.to_dict(orient="records"):
        document, reason = build_document(row)
        if reason:
            counters[reason] += 1
        elif document is not None:
            documents.append(document)

    counters["valid"] = len(documents)
    return documents, dict(counters)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean Porto taxi trips with Dask and upsert them into MongoDB."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(os.getenv("DATA_PATH", "data/train.csv")),
        help="Path to train.csv (default: DATA_PATH or data/train.csv).",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    )
    parser.add_argument(
        "--database",
        default=os.getenv("MONGO_DATABASE", "taxi_geospatial"),
    )
    parser.add_argument(
        "--collection",
        default=os.getenv("MONGO_COLLECTION", "trips"),
    )
    parser.add_argument(
        "--scheduler-address",
        default=os.getenv("DASK_SCHEDULER_ADDRESS"),
        help="Dask scheduler address; omit to start a local two-worker cluster.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process only the first N CSV rows, for a small test run.",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--partitions-per-batch", type=int, default=2)
    parser.add_argument("--blocksize", default="16 MiB")
    parser.add_argument(
        "--expected-workers",
        type=int,
        default=int(os.getenv("DASK_EXPECTED_WORKERS", "2")),
    )
    args = parser.parse_args()

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be greater than zero")
    if args.batch_size <= 0 or args.partitions_per_batch <= 0:
        parser.error("--batch-size and --partitions-per-batch must be greater than zero")
    if args.expected_workers <= 0:
        parser.error("--expected-workers must be greater than zero")
    return args


def _make_client(scheduler_address: str | None) -> Client:
    if scheduler_address:
        return Client(scheduler_address)
    return Client(n_workers=2, threads_per_worker=1, processes=True)


def _upsert_documents(
    collection: Collection[dict[str, Any]],
    documents: list[dict[str, Any]],
    batch_size: int,
) -> tuple[int, int]:
    upserted = 0
    modified = 0
    for start in range(0, len(documents), batch_size):
        batch = documents[start : start + batch_size]
        operations = [
            ReplaceOne({"_id": document["_id"]}, document, upsert=True)
            for document in batch
        ]
        result = collection.bulk_write(operations, ordered=False)
        upserted += result.upserted_count
        modified += result.modified_count
    return upserted, modified


def _read_limited_sample(
    dataframe: dd.DataFrame, limit: int, client: Client
) -> pd.DataFrame:
    sample_partitions: list[pd.DataFrame] = []
    rows_read = 0

    for partition in dataframe.to_delayed():
        future = client.compute(partition)
        partition_frame = client.gather(future)
        remaining = limit - rows_read
        sample_partitions.append(partition_frame.head(remaining))
        rows_read += min(len(partition_frame), remaining)
        if rows_read >= limit:
            break

    if not sample_partitions:
        return pd.DataFrame(columns=CSV_COLUMNS)
    return pd.concat(sample_partitions, ignore_index=True)


def ingest(args: argparse.Namespace) -> dict[str, int]:
    if not args.csv.is_file():
        raise FileNotFoundError(f"Dataset not found: {args.csv}")

    client = _make_client(args.scheduler_address)
    mongo_client: MongoClient[dict[str, Any]] | None = None
    try:
        if args.scheduler_address:
            client.wait_for_workers(args.expected_workers, timeout=120)

        mongo_client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        collection: Collection[dict[str, Any]] = mongo_client[args.database][
            args.collection
        ]
        collection.create_index([("location", "2dsphere")], name="location_2dsphere")

        ddf = dd.read_csv(
            args.csv,
            usecols=CSV_COLUMNS,
            dtype={column: "string" for column in CSV_COLUMNS},
            keep_default_na=False,
            blocksize=args.blocksize,
        )
        if args.limit is not None:
            sample = _read_limited_sample(ddf, args.limit, client)
            number_of_partitions = max(1, min(2, len(sample)))
            ddf = dd.from_pandas(sample, npartitions=number_of_partitions)

        delayed_partitions = [
            dask.delayed(transform_partition)(partition)
            for partition in ddf.to_delayed()
        ]
        totals: Counter[str] = Counter()

        for start in range(0, len(delayed_partitions), args.partitions_per_batch):
            task_batch = delayed_partitions[
                start : start + args.partitions_per_batch
            ]
            futures = client.compute(task_batch)
            partition_results = client.gather(futures)

            for documents, counters in partition_results:
                totals.update(counters)
                upserted, modified = _upsert_documents(
                    collection, documents, args.batch_size
                )
                totals["upserted"] += upserted
                totals["modified"] += modified

            LOGGER.info(
                "Partitions %d-%d/%d; read=%d valid=%d rejected=%d upserted=%d",
                start + 1,
                min(start + len(task_batch), len(delayed_partitions)),
                len(delayed_partitions),
                totals["rows_read"],
                totals["valid"],
                totals["rows_read"] - totals["valid"],
                totals["upserted"],
            )

        return dict(totals)
    finally:
        client.close()
        if mongo_client is not None:
            mongo_client.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    totals = ingest(args)
    rejected = totals.get("rows_read", 0) - totals.get("valid", 0)
    LOGGER.info(
        "Ingestion complete: read=%d valid=%d rejected=%d upserted=%d modified=%d",
        totals.get("rows_read", 0),
        totals.get("valid", 0),
        rejected,
        totals.get("upserted", 0),
        totals.get("modified", 0),
    )


if __name__ == "__main__":
    main()
