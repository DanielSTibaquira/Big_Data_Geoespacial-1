from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify Yelp reviews from Kafka and maintain geospatial windows."
    )
    parser.add_argument(
        "--risk-model",
        type=Path,
        default=Path(
            os.getenv(
                "LIFEMAP_RISK_MODEL_PATH",
                "/lifemap/risk_model_as_of_2021-01-01.json",
            )
        ),
    )
    parser.add_argument("--kafka-bootstrap-servers", default=os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    ))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "reviews"))
    parser.add_argument(
        "--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    )
    parser.add_argument("--database", default=os.getenv("LIFEMAP_DATABASE", "lifemap"))
    parser.add_argument(
        "--model",
        type=Path,
        default=Path(
            os.getenv(
                "LIFEMAP_MODEL_PATH",
                "/lifemap/model_serving_as_of_2021-01-01.json",
            )
        ),
    )
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path(os.getenv("LIFEMAP_CHECKPOINT_ROOT", "/lifemap/checkpoints")),
    )
    parser.add_argument("--minimum-hotspot-reviews", type=int, default=10)
    parser.add_argument("--negative-fraction-threshold", type=float, default=0.6)
    args = parser.parse_args()
    if args.minimum_hotspot_reviews < 1:
        parser.error("--minimum-hotspot-reviews must be positive")
    if not 0 < args.negative_fraction_threshold <= 1:
        parser.error("--negative-fraction-threshold must be in (0, 1]")
    return args


def _processed_review(
    row: Any,
    prediction: dict[str, Any],
    *,
    processed_at: datetime,
    batch_id: int,
) -> dict[str, Any]:
    review_date = datetime.fromisoformat(row["date"].replace("Z", "+00:00"))
    location = row["location"]
    if hasattr(location, "asDict"):
        location = location.asDict(recursive=True)
    if review_date.tzinfo is None:
        review_date = review_date.replace(tzinfo=timezone.utc)
    return {
        "_id": row["review_id"],
        "review_id": row["review_id"],
        "business_id": row["business_id"],
        "text": row["text"],
        "stars": row["stars"],
        "date": review_date.astimezone(timezone.utc),
        "sentiment": row["sentiment"],
        "location": location,
        "primary_category": row["primary_category"],
        "predicted_sentiment": prediction["sentiment"],
        "sentiment_probabilities": prediction["probabilities"],
        "processed_at": processed_at,
        "stream_batch_id": batch_id,
    }


def _risk_probability(features: dict[str, Any], model: dict[str, Any]) -> float:
    feature_names = model["feature_names"]
    coefficients = model["coefficients"]
    if len(feature_names) != len(coefficients):
        raise ValueError("Risk model feature names and coefficients do not match")
    score = float(model["intercept"]) + sum(
        float(coefficient) * float(features[name])
        for name, coefficient in zip(
            feature_names, coefficients
        )
    )
    if score >= 0:
        return 1.0 / (1.0 + math.exp(-score))
    exponent = math.exp(score)
    return exponent / (1.0 + exponent)


def _persist_predictions(
    batch: Any,
    batch_id: int,
    *,
    mongo_uri: str,
    database_name: str,
    risk_model: dict[str, Any],
) -> None:
    from pymongo import MongoClient, ReplaceOne

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        database = client[database_name]
        reviews = database["reviews"]
        processed_at = datetime.now(timezone.utc)
        operations = []
        latest_offsets: dict[str, int] = {}
        affected: dict[str, dict[str, Any]] = {}
        processed_count = 0
        for row in batch.toLocalIterator():
            prediction = json.loads(row["prediction_json"])
            document = _processed_review(
                row,
                prediction,
                processed_at=processed_at,
                batch_id=batch_id,
            )
            operations.append(ReplaceOne({"_id": row["review_id"]}, document, upsert=True))
            partition = str(row["partition"])
            latest_offsets[partition] = max(
                latest_offsets.get(partition, -1), int(row["offset"])
            )
            processed_count += 1
            if len(operations) >= 500:
                reviews.bulk_write(operations, ordered=False)
                operations.clear()
            review_date = datetime.fromisoformat(
                row["date"].replace("Z", "+00:00")
            )
            previous = affected.get(row["business_id"])
            if previous is None or review_date > previous["date"]:
                affected[row["business_id"]] = {
                    "date": review_date,
                    "review_id": row["review_id"],
                }
        if operations:
            reviews.bulk_write(operations, ordered=False)

        feature_collection = database["risk_features"]
        alert_collection = database["alerts"]
        threshold = float(risk_model["threshold"])
        cutoff = datetime.fromisoformat(
            risk_model["training_cutoff_utc"].replace("Z", "+00:00")
        )
        alert_operations = []
        for business_id, latest in affected.items():
            feature_document = feature_collection.find_one(
                {"business_id": business_id}
            )
            if feature_document is None:
                continue
            features = dict(feature_document["feature_values"])
            window_start = latest["date"] - timedelta(days=183)
            recent_reviews = reviews.find(
                {
                    "business_id": business_id,
                    "date": {"$gte": window_start, "$lte": latest["date"]},
                    "$or": [
                        {"date": {"$lte": cutoff}},
                        {"processed_at": {"$exists": True}},
                    ],
                },
                {"stars": 1, "predicted_sentiment": 1},
            )
            negative_count = 0
            review_count = 0
            for recent_review in recent_reviews:
                review_count += 1
                sentiment = recent_review.get("predicted_sentiment")
                if sentiment is None:
                    sentiment = (
                        "negative"
                        if int(recent_review.get("stars", 3)) <= 2
                        else "neutral"
                        if int(recent_review.get("stars", 3)) == 3
                        else "positive"
                    )
                negative_count += int(sentiment == "negative")
            previous_rate = float(features["negative_rate_previous_6m"])
            live_rate = negative_count / review_count if review_count else previous_rate
            features["negative_rate_last_6m"] = live_rate
            features["negative_rate_trend"] = live_rate - previous_rate
            feature_collection.update_one(
                {"business_id": business_id},
                {
                    "$set": {
                        "feature_values": features,
                        "last_updated_by_stream": processed_at,
                    }
                },
            )
            probability = _risk_probability(features, risk_model)
            if probability >= threshold:
                alert_operations.append(
                    ReplaceOne(
                        {"_id": business_id},
                        {
                            "_id": business_id,
                            "business_id": business_id,
                            "location": feature_document["location"],
                            "primary_category": feature_document[
                                "primary_category"
                            ],
                            "risk_probability": probability,
                            "threshold": threshold,
                            "features": features,
                            "last_review_id": latest["review_id"],
                            "alerted_at": processed_at,
                            "model_cutoff_utc": cutoff,
                            "stream_batch_id": batch_id,
                        },
                        upsert=True,
                    )
                )
            else:
                alert_collection.delete_one({"_id": business_id})
        if alert_operations:
            alert_collection.bulk_write(alert_operations, ordered=False)
            alert_collection.create_index(
                [("location", "2dsphere")], name="location_2dsphere"
            )

        status = database["stream_status"]
        previous = status.find_one({"_id": "sentiment-consumer"}) or {}
        offsets = {
            str(partition): int(offset)
            for partition, offset in previous.get("offsets", {}).items()
        }
        for partition, offset in latest_offsets.items():
            offsets[partition] = max(offsets.get(partition, -1), offset)
        status.replace_one(
            {"_id": "sentiment-consumer"},
            {
                "_id": "sentiment-consumer",
                "batch_id": batch_id,
                "offsets": offsets,
                "last_processed_at": processed_at,
                "processed_messages": int(previous.get("processed_messages", 0))
                + processed_count,
            },
            upsert=True,
        )
    finally:
        client.close()


def _persist_hotspots(
    batch: Any,
    batch_id: int,
    *,
    mongo_uri: str,
    database_name: str,
    minimum_reviews: int,
    negative_fraction_threshold: float,
) -> None:
    from pymongo import MongoClient, ReplaceOne

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        collection = client[database_name]["live_hotspots"]
        operations = []
        for row in batch.toLocalIterator():
            count = int(row["review_count"])
            negative_count = int(row["negative_count"])
            fraction = negative_count / count if count else 0.0
            location = {
                "type": "Point",
                "coordinates": [float(row["longitude"]), float(row["latitude"])],
            }
            start = row["window"]["start"]
            end = row["window"]["end"]
            window_start = (
                start.replace(tzinfo=timezone.utc)
                if start.tzinfo is None
                else start.astimezone(timezone.utc)
            )
            window_end = (
                end.replace(tzinfo=timezone.utc)
                if end.tzinfo is None
                else end.astimezone(timezone.utc)
            )
            identifier = f"{row['geohash']}:{window_start.isoformat()}"
            operations.append(
                ReplaceOne(
                    {"_id": identifier},
                    {
                        "_id": identifier,
                        "geohash": row["geohash"],
                        "window_start": window_start,
                        "window_end": window_end,
                        "location": location,
                        "review_count": count,
                        "negative_count": negative_count,
                        "negative_fraction": fraction,
                        "hotspot": (
                            count >= minimum_reviews
                            and fraction >= negative_fraction_threshold
                        ),
                        "stream_batch_id": batch_id,
                        "updated_at": datetime.now(timezone.utc),
                    },
                    upsert=True,
                )
            )
        if operations:
            collection.bulk_write(operations, ordered=False)
            collection.create_index([("location", "2dsphere")], name="location_2dsphere")
            collection.create_index([("window_end", -1)], name="window_end_desc")
    finally:
        client.close()


def main() -> None:
    args = _arguments()
    if not args.model.is_file():
        raise FileNotFoundError(f"Sentiment model not found: {args.model}")
    if not args.risk_model.is_file():
        raise FileNotFoundError(f"Closure risk model not found: {args.risk_model}")
    model = json.loads(args.model.read_text(encoding="utf-8"))
    if model.get("format") != "lifemap-logistic-v1":
        raise ValueError(f"Unsupported sentiment model: {args.model}")
    risk_model = json.loads(args.risk_model.read_text(encoding="utf-8"))
    if risk_model.get("format") != "lifemap-risk-logistic-v1":
        raise ValueError(f"Unsupported closure risk model: {args.risk_model}")
    if model.get("training_cutoff_utc") != risk_model.get(
        "training_cutoff_utc"
    ):
        raise ValueError("Sentiment and risk models must use the same training cutoff")

    if "/opt" not in sys.path:
        sys.path.insert(0, "/opt")
    from api.sentiment import predict_sentiment
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    from lifemap_metrics import encode_geohash, geohash_center

    spark = (
        SparkSession.builder.appName("LifeMapKafkaSentimentStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.addPyFile(
        str(Path(__file__).with_name("lifemap_metrics.py"))
    )
    model_broadcast = spark.sparkContext.broadcast(model)
    predict_udf = F.udf(
        lambda text: json.dumps(
            predict_sentiment(text or "", model_broadcast.value),
            separators=(",", ":"),
        ),
        StringType(),
    )
    geohash_udf = F.udf(
        lambda coordinates: (
            encode_geohash(float(coordinates[1]), float(coordinates[0]), 5)
            if coordinates and len(coordinates) == 2
            else None
        ),
        StringType(),
    )
    center_udf = F.udf(
        lambda cell: geohash_center(cell) if cell else None,
        "struct<longitude:double,latitude:double>",
    )
    schema = (
        "review_id STRING, business_id STRING, text STRING, stars INT, sentiment STRING, "
        "date STRING, location STRUCT<type:STRING,coordinates:ARRAY<DOUBLE>>, "
        "primary_category STRING"
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), schema).alias("review"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("review.*", "partition", "offset")
        .where(
            F.col("review_id").isNotNull()
            & F.col("business_id").isNotNull()
            & F.col("text").isNotNull()
            & F.col("date").isNotNull()
        )
    )
    classified = parsed.withColumn(
        "prediction_json", predict_udf(F.col("text"))
    )
    classified_query = (
        classified.writeStream
        .queryName("lifemap-sentiment-inference")
        .option("checkpointLocation", str(args.checkpoint_root / "sentiment"))
        .foreachBatch(
            lambda batch, batch_id: _persist_predictions(
                batch,
                batch_id,
                mongo_uri=args.mongo_uri,
                database_name=args.database,
                risk_model=risk_model,
            )
        )
        .start()
    )

    from pyspark.sql.functions import window

    event_reviews = (
        parsed.withColumn("event_time", F.to_timestamp("date"))
        .withWatermark("event_time", "7 days")
        .withColumn("prediction_json", predict_udf(F.col("text")))
        .withColumn(
            "predicted_sentiment",
            F.get_json_object(F.col("prediction_json"), "$.sentiment"),
        )
        .withColumn(
            "geohash",
            geohash_udf(F.col("location.coordinates")),
        )
        .where(F.col("geohash").isNotNull())
    )
    windowed = (
        event_reviews.groupBy(
            window(F.col("event_time"), "30 days").alias("window"),
            F.col("geohash"),
        )
        .agg(
            F.count("*").alias("review_count"),
            F.sum(
                F.when(F.col("predicted_sentiment") == "negative", 1).otherwise(0)
            ).alias("negative_count"),
        )
        .withColumn("center", center_udf(F.col("geohash")))
        .withColumn("longitude", F.col("center.longitude"))
        .withColumn("latitude", F.col("center.latitude"))
        .drop("center")
    )
    hotspot_query = (
        windowed.writeStream
        .queryName("lifemap-geohash-hotspots")
        .outputMode("update")
        .option("checkpointLocation", str(args.checkpoint_root / "hotspots"))
        .foreachBatch(
            lambda batch, batch_id: _persist_hotspots(
                batch,
                batch_id,
                mongo_uri=args.mongo_uri,
                database_name=args.database,
                minimum_reviews=args.minimum_hotspot_reviews,
                negative_fraction_threshold=args.negative_fraction_threshold,
            )
        )
        .start()
    )
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in (classified_query, hotspot_query):
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
