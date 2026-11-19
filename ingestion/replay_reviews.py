from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Protocol

from pymongo import ASCENDING, MongoClient

class Producer(Protocol):
    def send(self, topic: str, *, key: bytes, value: dict[str, Any]) -> Any: ...

    def flush(self) -> Any: ...


def parse_utc_cutoff(value: str) -> datetime:
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    try:
        cutoff = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "cutoff must be an ISO-8601 datetime with a timezone"
        ) from error
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "cutoff must include a timezone, for example 2021-01-01T00:00:00Z"
        )
    return cutoff.astimezone(timezone.utc)


def make_review_message(
    review: dict[str, Any], business: dict[str, Any]
) -> dict[str, Any]:
    location = business.get("location")
    if not isinstance(location, dict) or location.get("type") != "Point":
        raise ValueError(f"Business {review['business_id']} has no GeoJSON location")
    coordinates = location.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) != 2:
        raise ValueError(f"Business {review['business_id']} has invalid coordinates")
    longitude, latitude = coordinates
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
        raise ValueError(f"Business {review['business_id']} has invalid coordinates")
    review_date = review.get("date")
    if not isinstance(review_date, datetime):
        raise ValueError(f"Review {review['_id']} has no valid date")
    if review_date.tzinfo is None:
        review_date = review_date.replace(tzinfo=timezone.utc)
    return {
        "review_id": str(review.get("review_id", review["_id"])),
        "business_id": str(review["business_id"]),
        "text": str(review.get("text", "")),
        "stars": int(review["stars"]),
        "sentiment": review.get("sentiment"),
        "date": review_date.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        ),
        "location": location,
        "primary_category": business.get("primary_category", "Restaurants"),
    }


def replay_reviews(
    reviews: Any,
    businesses: Any,
    producer: Producer,
    *,
    cutoff: datetime,
    rate: float,
    limit: int | None = None,
    topic: str = "reviews",
    sleep: Any = time.sleep,
    clock: Any = time.monotonic,
) -> int:
    if rate <= 0:
        raise ValueError("rate must be greater than zero")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be greater than zero")

    businesses.create_index([("location", "2dsphere")], name="location_2dsphere")
    business_by_id = {
        business["_id"]: business
        for business in businesses.find(
            {},
            {
                "_id": 1,
                "location": 1,
                "primary_category": 1,
            },
        )
    }
    reviews.create_index(
        [("date", ASCENDING), ("_id", ASCENDING)],
        name="date_replay_order",
    )
    cursor = reviews.find(
        {"date": {"$gt": cutoff}},
        {
            "_id": 1,
            "review_id": 1,
            "business_id": 1,
            "text": 1,
            "stars": 1,
            "sentiment": 1,
            "date": 1,
        },
    ).sort([("date", ASCENDING), ("_id", ASCENDING)])
    if limit is not None:
        cursor = cursor.limit(limit)

    sent = 0
    next_send = clock()
    previous_date: datetime | None = None
    try:
        for review in cursor:
            review_date = review["date"]
            if review_date.tzinfo is None:
                review_date = review_date.replace(tzinfo=timezone.utc)
            if previous_date is not None and review_date < previous_date:
                raise RuntimeError("MongoDB returned reviews out of date order")
            previous_date = review_date
            business_id = review["business_id"]
            business = business_by_id.get(business_id)
            if business is None:
                raise ValueError(f"Review references unknown business {business_id}")
            if sent:
                next_send += 1.0 / rate
                sleep(max(0.0, next_send - clock()))
            payload = make_review_message(review, business)
            delivery = producer.send(
                topic,
                key=payload["business_id"].encode("utf-8"),
                value=payload,
            )
            if hasattr(delivery, "get"):
                delivery.get(timeout=30)
            sent += 1
    finally:
        producer.flush()
    return sent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay Yelp reviews after the training cutoff to Kafka."
    )
    parser.add_argument("--cutoff", type=parse_utc_cutoff, default=parse_utc_cutoff(
        os.getenv("LIFEMAP_TRAINING_CUTOFF", "2021-01-01T00:00:00Z")
    ))
    parser.add_argument("--rate", type=float, default=5.0, help="Messages per second.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--topic", default="reviews")
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://mongodb:27017"),
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    args = parser.parse_args()
    if args.rate <= 0:
        parser.error("--rate must be greater than zero")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be greater than zero")
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    return args


def main() -> None:
    from kafka import KafkaProducer

    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap_servers.split(","),
        value_serializer=lambda value: json.dumps(
            value, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
    )
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        database = client[os.getenv("LIFEMAP_DATABASE", "lifemap")]
        sent = replay_reviews(
            database[os.getenv("LIFEMAP_REVIEWS_COLLECTION", "reviews")],
            database[os.getenv("LIFEMAP_BUSINESSES_COLLECTION", "businesses")],
            producer,
            cutoff=args.cutoff,
            rate=args.rate,
            limit=args.limit,
            topic=args.topic,
        )
        print(json.dumps({"topic": args.topic, "published": sent, "cutoff": args.cutoff.isoformat()}))
    finally:
        producer.close()
        client.close()


if __name__ == "__main__":
    main()
