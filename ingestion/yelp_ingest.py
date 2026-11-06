from __future__ import annotations

import argparse
import json
import logging
import math
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dask.base import compute
import dask.dataframe as dd
from dask.delayed import delayed
import pandas as pd
from dask.distributed import Client
from pymongo import MongoClient, ReplaceOne
from pymongo.collection import Collection

from ingestion.yelp_dataset import (
    DATASET_HANDLE,
    REGION_CENTER,
    REGION_RADIUS_KM,
    REGION_STATES,
    business_filter_counts,
    download_yelp_dataset,
    read_yelp_data,
    select_target_businesses,
    validate_dataset_dir,
)

LOGGER = logging.getLogger("yelp_ingestion")
BUSINESS_FIELDS = (
    "business_id",
    "name",
    "city",
    "state",
    "latitude",
    "longitude",
    "stars",
    "review_count",
    "is_open",
    "categories",
)
REVIEW_FIELDS = ("review_id", "business_id", "stars", "text", "date")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _parse_review_date(value: Any) -> datetime | None:
    if _is_missing(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _parse_stars(value: Any) -> int | None:
    if isinstance(value, bool) or _is_missing(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or not numeric.is_integer():
        return None
    stars = int(numeric)
    return stars if 1 <= stars <= 5 else None


def build_review_documents(
    partition: pd.DataFrame, target_business_ids: set[str]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    counters: Counter[str] = Counter(rows_read=len(partition))
    documents: list[dict[str, Any]] = []
    seen_review_ids: set[str] = set()

    for row in partition.to_dict(orient="records"):
        business_id = row.get("business_id")
        review_id = row.get("review_id")
        if _is_missing(business_id) or _is_missing(review_id):
            counters["missing_ids"] += 1
            continue

        business_id = str(business_id).strip()
        review_id = str(review_id).strip()
        if not business_id or not review_id:
            counters["missing_ids"] += 1
            continue
        if review_id in seen_review_ids:
            counters["duplicate_reviews"] += 1
            continue
        seen_review_ids.add(review_id)
        if business_id not in target_business_ids:
            counters["outside_target_businesses"] += 1
            continue

        text = row.get("text")
        if not isinstance(text, str) or not text.strip():
            counters["missing_text"] += 1
            continue

        stars = _parse_stars(row.get("stars"))
        if stars is None:
            counters["invalid_stars"] += 1
            continue

        review_date = _parse_review_date(row.get("date"))
        if review_date is None:
            counters["invalid_date"] += 1
            continue

        sentiment = "negative" if stars <= 2 else "neutral" if stars == 3 else "positive"
        documents.append(
            {
                "_id": review_id,
                "review_id": review_id,
                "business_id": business_id,
                "stars": stars,
                "sentiment": sentiment,
                "text": text.strip(),
                "date": review_date,
            }
        )
        counters["valid"] += 1

    return documents, dict(counters)


def build_business_document(row: dict[str, Any]) -> dict[str, Any]:
    business_id = str(row.get("business_id", "")).strip()
    if not business_id:
        raise ValueError("Business row is missing business_id")

    try:
        longitude = float(row["longitude"])
        latitude = float(row["latitude"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Business {business_id} has invalid coordinates") from error
    if (
        not math.isfinite(longitude)
        or not math.isfinite(latitude)
        or not -180 <= longitude <= 180
        or not -90 <= latitude <= 90
    ):
        raise ValueError(f"Business {business_id} has invalid coordinates")
    raw_is_open = row.get("is_open")
    if _is_missing(raw_is_open) or raw_is_open not in (0, 1, 0.0, 1.0):
        raise ValueError(f"Business {business_id} has invalid is_open status")

    raw_categories = row.get("categories")
    categories = (
        [category.strip() for category in raw_categories.split(",") if category.strip()]
        if isinstance(raw_categories, str)
        else []
    )
    return {
        "_id": business_id,
        "business_id": business_id,
        "name": row.get("name") if isinstance(row.get("name"), str) else "",
        "city": row.get("city") if isinstance(row.get("city"), str) else "",
        "state": row.get("state") if isinstance(row.get("state"), str) else "",
        "stars": float(row["stars"]) if not _is_missing(row.get("stars")) else None,
        "review_count_source": (
            int(row["review_count"])
            if not _is_missing(row.get("review_count"))
            else None
        ),
        "is_open": int(raw_is_open),
        "categories": categories,
        "primary_category": categories[0] if categories else "Restaurants",
        "restaurant_family": "Restaurants",
        "location": {
            "type": "Point",
            "coordinates": [longitude, latitude],
        },
    }


def _make_client(scheduler_address: str | None) -> Client:
    if scheduler_address:
        return Client(scheduler_address)
    return Client(n_workers=2, threads_per_worker=1, processes=True)


def _upsert_documents(
    collection: Collection[dict[str, Any]],
    documents: list[dict[str, Any]],
    batch_size: int,
) -> int:
    upserted = 0
    for start in range(0, len(documents), batch_size):
        batch = documents[start : start + batch_size]
        result = collection.bulk_write(
            [
                ReplaceOne({"_id": document["_id"]}, document, upsert=True)
                for document in batch
            ],
            ordered=False,
        )
        upserted += result.upserted_count
    return upserted


def _read_limited_reviews(
    reviews: dd.DataFrame, limit: int, client: Client
) -> dd.DataFrame:
    sample_partitions: list[pd.DataFrame] = []
    rows_read = 0
    for partition in reviews.to_delayed():
        partition_frame = compute(partition, scheduler=client.get)[0]
        remaining = limit - rows_read
        sample_partitions.append(partition_frame.head(remaining))
        rows_read += min(len(partition_frame), remaining)
        if rows_read >= limit:
            break
    sample = (
        pd.concat(sample_partitions, ignore_index=True)
        if sample_partitions
        else pd.DataFrame(columns=REVIEW_FIELDS)
    )
    return dd.from_pandas(sample, npartitions=max(1, min(2, len(sample))))


def _business_lifetimes(
    reviews: Collection[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    sentiment_score = {
        "$switch": {
            "branches": [
                {"case": {"$lte": ["$stars", 2]}, "then": -1.0},
                {"case": {"$eq": ["$stars", 3]}, "then": 0.0},
            ],
            "default": 1.0,
        }
    }
    pipeline = [
        {
            "$group": {
                "_id": "$business_id",
                "first_review_date": {"$min": "$date"},
                "last_review_date": {"$max": "$date"},
                "review_count": {"$sum": 1},
                "average_stars": {"$avg": "$stars"},
                "mean_score": {"$avg": sentiment_score},
                "negative_reviews": {
                    "$sum": {"$cond": [{"$lte": ["$stars", 2]}, 1, 0]}
                },
            }
        }
    ]
    lifetimes: dict[str, dict[str, Any]] = {}
    for item in reviews.aggregate(pipeline, allowDiskUse=True):
        first_review = item["first_review_date"]
        last_review = item["last_review_date"]
        if first_review.tzinfo is None:
            first_review = first_review.replace(tzinfo=timezone.utc)
        if last_review.tzinfo is None:
            last_review = last_review.replace(tzinfo=timezone.utc)
        days_business = max(0.0, (last_review - first_review).total_seconds() / 86400)
        lifetimes[item["_id"]] = {
            "first_review_date": first_review,
            "last_review_date": last_review,
            "review_count": item["review_count"],
            "days_business": days_business,
            "average_stars": item.get("average_stars"),
            "mean_score": item.get("mean_score"),
            "negative_reviews": item.get("negative_reviews", 0),
        }
    return lifetimes


def _upsert_businesses(
    collection: Collection[dict[str, Any]],
    business_rows: pd.DataFrame,
    lifetimes: dict[str, dict[str, Any]],
    batch_size: int,
) -> int:
    documents: list[dict[str, Any]] = []
    upserted = 0
    for row in business_rows.to_dict(orient="records"):
        document = build_business_document(
            {str(key): value for key, value in row.items()}
        )
        lifetime = lifetimes.get(document["business_id"])
        is_open = document["is_open"]
        document["life"] = {
            "first_review_date": lifetime["first_review_date"] if lifetime else None,
            "last_review_date": lifetime["last_review_date"] if lifetime else None,
            "review_count": lifetime["review_count"] if lifetime else 0,
            "days_business": lifetime["days_business"] if lifetime else 0.0,
            "event": is_open == 0,
            "censored": is_open == 1,
        }
        document["sentiment"] = {
            "mean_score": lifetime["mean_score"] if lifetime else None,
            "average_stars": lifetime["average_stars"] if lifetime else None,
            "negative_reviews": lifetime["negative_reviews"] if lifetime else 0,
        }
        documents.append(document)
        if len(documents) >= batch_size:
            upserted += _upsert_documents(collection, documents, batch_size)
            documents.clear()
    upserted += _upsert_documents(collection, documents, batch_size)
    return upserted


def ingest_yelp(args: argparse.Namespace) -> dict[str, Any]:
    dataset_dir = (
        validate_dataset_dir(args.dataset_dir)
        if args.dataset_dir
        else download_yelp_dataset()
    )
    dataframes = read_yelp_data(dataset_dir, blocksize=args.blocksize)
    client = _make_client(args.scheduler_address)
    mongo_client: MongoClient[dict[str, Any]] | None = None
    try:
        if args.scheduler_address:
            client.wait_for_workers(args.expected_workers, timeout=120)

        all_businesses = dataframes["business.json"]
        business_selection = select_target_businesses(all_businesses)
        business_rows = compute(
            business_selection[list(BUSINESS_FIELDS)],
            scheduler=client.get,
        )[0]
        if business_rows.empty:
            raise ValueError("No Yelp businesses match the configured study area")
        target_business_ids = set(business_rows["business_id"].astype(str))
        selection_counts = business_filter_counts(
            all_businesses,
            scheduler=client.get,
        )

        source_reviews = dataframes["review.json"]
        if args.limit is not None:
            source_reviews = _read_limited_reviews(source_reviews, args.limit, client)

        mongo_client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
        mongo_client.admin.command("ping")
        database = mongo_client[args.database]
        businesses: Collection[dict[str, Any]] = database[args.businesses_collection]
        reviews: Collection[dict[str, Any]] = database[args.reviews_collection]
        if args.replace_collections:
            businesses.drop()
            reviews.drop()
        businesses.create_index([("location", "2dsphere")], name="location_2dsphere")
        reviews.create_index([("business_id", 1)], name="business_id_1")

        totals: Counter[str] = Counter()
        partitions = source_reviews.to_delayed()
        for start in range(0, len(partitions), args.partitions_per_batch):
            task_batch = [
                delayed(build_review_documents)(partition, target_business_ids)
                for partition in partitions[
                    start : start + args.partitions_per_batch
                ]
            ]
            results = compute(*task_batch, scheduler=client.get)
            for documents, counters in results:
                totals.update(counters)
                totals["accepted"] += len(documents)
                totals["upserted_reviews"] += _upsert_documents(
                    reviews, documents, args.batch_size
                )
            LOGGER.info(
                "Review partitions %d-%d/%d; read=%d accepted=%d rejected=%d",
                start + 1,
                min(start + len(task_batch), len(partitions)),
                len(partitions),
                totals["rows_read"],
                totals["accepted"],
                totals["rows_read"] - totals["accepted"],
            )

        lifetimes = _business_lifetimes(reviews)
        business_upserts = _upsert_businesses(
            businesses,
            business_rows,
            lifetimes,
            args.batch_size,
        )
        return {
            "dataset": DATASET_HANDLE,
            "study_area": {
                "name": "Philadelphia metropolitan study area",
                "center_latitude": REGION_CENTER[0],
                "center_longitude": REGION_CENTER[1],
                "radius_km": REGION_RADIUS_KM,
                "states": list(REGION_STATES),
                "family": "Restaurants",
            },
            "businesses": {
                **selection_counts,
                "selected": len(business_rows),
                "upserted": business_upserts,
            },
            "reviews": {
                **dict(totals),
                "businesses_with_reviews": len(lifetimes),
            },
            "collections": {
                "database": args.database,
                "businesses": args.businesses_collection,
                "reviews": args.reviews_collection,
            },
        }
    finally:
        client.close()
        if mongo_client is not None:
            mongo_client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Philadelphia-area Yelp restaurants and reviews into MongoDB."
    )
    parser.add_argument("--dataset-dir", type=Path)
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    )
    parser.add_argument(
        "--database",
        default=os.getenv("YELP_MONGO_DATABASE", "lifemap"),
    )
    parser.add_argument(
        "--businesses-collection",
        default=os.getenv("YELP_BUSINESSES_COLLECTION", "businesses"),
    )
    parser.add_argument(
        "--reviews-collection",
        default=os.getenv("YELP_REVIEWS_COLLECTION", "reviews"),
    )
    parser.add_argument(
        "--scheduler-address",
        default=os.getenv("DASK_SCHEDULER_ADDRESS"),
    )
    parser.add_argument(
        "--expected-workers",
        type=int,
        default=int(os.getenv("DASK_EXPECTED_WORKERS", "2")),
    )
    parser.add_argument("--limit", type=int, help="Limit source review rows for a test run.")
    parser.add_argument("--blocksize", default="16 MiB")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--partitions-per-batch", type=int, default=2)
    parser.add_argument(
        "--replace-collections",
        action="store_true",
        help="Drop the target business and review collections before ingestion.",
    )
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be greater than zero")
    if args.batch_size <= 0 or args.partitions_per_batch <= 0:
        parser.error("--batch-size and --partitions-per-batch must be greater than zero")
    if args.expected_workers <= 0:
        parser.error("--expected-workers must be greater than zero")
    if args.businesses_collection == args.reviews_collection:
        parser.error("Businesses and reviews must use different collections")
    return args


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    report = ingest_yelp(args)
    serialized = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    print(serialized)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(serialized + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
