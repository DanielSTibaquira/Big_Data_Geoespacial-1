from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pymongo import MongoClient, ReplaceOne

if "/opt" not in sys.path:
    sys.path.insert(0, "/opt")
from api.e2_analytics import kaplan_meier, survival_observation
if __package__:
    from .lifemap_metrics import encode_geohash
else:
    from lifemap_metrics import encode_geohash


FEATURE_NAMES = (
    "mean_stars",
    "review_count",
    "negative_rate_last_6m",
    "negative_rate_previous_6m",
    "negative_rate_trend",
    "age_years",
    "competitors_500m",
)


def _utc_datetime(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(
        timezone.utc
    )


def _cutoff(value: str) -> datetime:
    normalized = f"{value[:-1]}+00:00" if value.endswith("Z") else value
    result = datetime.fromisoformat(normalized)
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError("cutoff must include a timezone")
    return result.astimezone(timezone.utc)


def _competitor_counts(
    businesses: list[dict[str, Any]], collection: Any
) -> dict[str, int]:
    collection.create_index([("location", "2dsphere")], name="location_2dsphere")

    def count_competitors(business: dict[str, Any]) -> tuple[str, int]:
        business_id = str(business["business_id"])
        location = business.get("location")
        if not location:
            return business_id, 0
        pipeline = [
            {
                "$geoNear": {
                    "near": location,
                    "key": "location",
                    "distanceField": "distance_m",
                    "maxDistance": 500,
                    "spherical": True,
                    "query": {
                        "primary_category": business.get("primary_category")
                        or "Restaurants",
                        "business_id": {"$ne": business_id},
                    },
                }
            },
            {"$count": "competitors"},
        ]
        result = next(iter(collection.aggregate(pipeline)), None)
        return business_id, int(result["competitors"]) if result else 0

    with ThreadPoolExecutor(max_workers=8) as executor:
        return dict(executor.map(count_competitors, businesses))


def _business_features(
    db: Any,
    cutoff: datetime,
    horizon_days: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    businesses = list(
        db["businesses"].find(
            {},
            {
                "_id": 1,
                "business_id": 1,
                "is_open": 1,
                "primary_category": 1,
                "location": 1,
                "life.first_review_date": 1,
                "life.last_review_date": 1,
            },
        )
    )
    competitor_count = _competitor_counts(businesses, db["businesses"])
    historical = {}
    six_months_ago = cutoff - timedelta(days=183)
    one_year_ago = cutoff - timedelta(days=365)
    historical_pipeline = [
        {"$match": {"date": {"$lte": cutoff}}},
        {
            "$group": {
                "_id": "$business_id",
                "review_count": {"$sum": 1},
                "mean_stars": {"$avg": "$stars"},
                "first_review_date": {"$min": "$date"},
                "recent_count": {
                    "$sum": {"$cond": [{"$gte": ["$date", six_months_ago]}, 1, 0]}
                },
                "recent_negative": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$date", six_months_ago]},
                                    {"$lte": ["$stars", 2]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "previous_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$date", one_year_ago]},
                                    {"$lt": ["$date", six_months_ago]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
                "previous_negative": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$date", one_year_ago]},
                                    {"$lt": ["$date", six_months_ago]},
                                    {"$lte": ["$stars", 2]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
    ]
    for item in db["reviews"].aggregate(historical_pipeline, allowDiskUse=True):
        historical[item["_id"]] = item

    post_cutoff_pipeline = [
        {"$match": {"date": {"$gt": cutoff}}},
        {"$group": {"_id": "$business_id", "last_review_date": {"$max": "$date"}}},
    ]
    post_cutoff = {
        item["_id"]: _utc_datetime(item["last_review_date"])
        for item in db["reviews"].aggregate(post_cutoff_pipeline, allowDiskUse=True)
    }
    horizon = cutoff + timedelta(days=horizon_days)
    rows = []
    for business in businesses:
        business_id = str(business.get("business_id", business["_id"]))
        history = historical.get(business_id)
        last_review = post_cutoff.get(business_id)
        if history is None or last_review is None:
            continue
        is_open = int(business.get("is_open", 1))
        label = int(is_open == 0 and last_review <= horizon)
        recent_rate = (
            history["recent_negative"] / history["recent_count"]
            if history["recent_count"]
            else 0.0
        )
        previous_rate = (
            history["previous_negative"] / history["previous_count"]
            if history["previous_count"]
            else recent_rate
        )
        first_review = _utc_datetime(history["first_review_date"])
        age_years = max(0.0, (cutoff - first_review).total_seconds() / 31_557_600)
        row = {
            "business_id": business_id,
            "label": label,
            "mean_stars": float(history["mean_stars"] or 0) / 5,
            "review_count": math.log1p(int(history["review_count"])),
            "negative_rate_last_6m": recent_rate,
            "negative_rate_previous_6m": previous_rate,
            "negative_rate_trend": recent_rate - previous_rate,
            "age_years": min(age_years, 100) / 100,
            "competitors_500m": math.log1p(competitor_count.get(business_id, 0)),
            "location": business.get("location"),
            "primary_category": business.get("primary_category") or "Restaurants",
        }
        rows.append(row)
    metadata = {
        "businesses_with_pre_cutoff_reviews": len(historical),
        "businesses_with_post_cutoff_reviews": len(post_cutoff),
        "eligible_businesses": len(rows),
        "excluded_without_post_cutoff_activity": len(businesses) - len(rows),
        "horizon_end_utc": horizon.isoformat().replace("+00:00", "Z"),
        "label_definition": (
            "1 if currently closed and last review is in (T,T+12 months]; "
            "0 if open or activity continues beyond T+12 months; "
            "exclude businesses without reviews after T"
        ),
    }
    return rows, metadata


def _fit_risk_model(
    rows: list[dict[str, Any]],
    *,
    output_path: Path,
    db: Any,
    cutoff: datetime,
) -> dict[str, Any]:
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    spark = SparkSession.builder.appName("LifeMapClosureRiskModel").getOrCreate()
    db["risk_scores"].create_index(
        [("business_id", 1)], unique=True, name="business_id_unique"
    )
    db["risk_features"].create_index(
        [("business_id", 1)], unique=True, name="business_id_unique"
    )
    schema = StructType(
        [
            StructField("business_id", StringType(), False),
            StructField("label", IntegerType(), False),
            *[StructField(name, DoubleType(), False) for name in FEATURE_NAMES],
        ]
    )
    feature_rows = [
        {key: row[key] for key in ("business_id", "label", *FEATURE_NAMES)}
        for row in rows
    ]
    frame = spark.createDataFrame(feature_rows, schema=schema)
    assembler = VectorAssembler(inputCols=list(FEATURE_NAMES), outputCol="features")
    frame = (
        assembler.transform(frame)
        .withColumn("split_bucket", F.pmod(F.xxhash64("business_id"), F.lit(5)))
    )
    train = frame.where(F.col("split_bucket") != 0)
    test = frame.where(F.col("split_bucket") == 0)
    train_count = train.count()
    test_count = test.count()
    class_counts = {
        int(item["label"]): int(item["count"])
        for item in train.groupBy("label").count().collect()
    }
    test_class_counts = {
        int(item["label"]): int(item["count"])
        for item in test.groupBy("label").count().collect()
    }
    if (
        train_count == 0
        or test_count == 0
        or set(class_counts) != {0, 1}
        or set(test_class_counts) != {0, 1}
    ):
        spark.stop()
        raise RuntimeError(
            "Risk model needs both labels in non-empty train and test splits"
        )
    negative_count, positive_count = class_counts[0], class_counts[1]
    train = train.withColumn(
        "class_weight",
        F.when(
            F.col("label") == 1,
            F.lit(train_count / (2 * positive_count)),
        ).otherwise(F.lit(train_count / (2 * negative_count))),
    )
    model = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        weightCol="class_weight",
        maxIter=100,
        regParam=0.01,
    ).fit(train)
    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    test_predictions = model.transform(test).cache()
    auc_roc = evaluator.evaluate(test_predictions)
    evaluator.setMetricName("areaUnderPR")
    auc_pr = evaluator.evaluate(test_predictions)
    confusion = test_predictions.select("label", "prediction").groupBy(
        "label", "prediction"
    ).count().collect()
    counts = {(int(row["label"]), int(row["prediction"])): int(row["count"]) for row in confusion}
    true_positive = counts.get((1, 1), 0)
    false_negative = counts.get((1, 0), 0)
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
    test_positive_prevalence = test_class_counts.get(1, 0) / test_count
    predictions = model.transform(frame).select("business_id", "probability").collect()
    feature_by_id = {row["business_id"]: row for row in rows}
    operations = []
    feature_operations = []
    for prediction in predictions:
        probability = float(prediction["probability"][1])
        business = feature_by_id[prediction["business_id"]]
        operations.append(
            ReplaceOne(
                {"_id": prediction["business_id"]},
                {
                    "_id": prediction["business_id"],
                    "business_id": prediction["business_id"],
                    "risk_probability": probability,
                    "label": business["label"],
                    "location": business["location"],
                    "primary_category": business["primary_category"],
                    "cutoff_utc": cutoff,
                    "updated_at": datetime.now(timezone.utc),
                },
                upsert=True,
            )
        )
        feature_operations.append(
            ReplaceOne(
                {"_id": prediction["business_id"]},
                {
                    "_id": prediction["business_id"],
                    "business_id": prediction["business_id"],
                    "feature_values": {
                        name: float(business[name]) for name in FEATURE_NAMES
                    },
                    "location": business["location"],
                    "primary_category": business["primary_category"],
                    "cutoff_utc": cutoff,
                    "updated_at": datetime.now(timezone.utc),
                },
                upsert=True,
            )
        )
        if len(operations) >= 500:
            db["risk_scores"].bulk_write(operations, ordered=False)
            db["risk_features"].bulk_write(feature_operations, ordered=False)
            operations.clear()
            feature_operations.clear()
    if operations:
        db["risk_scores"].bulk_write(operations, ordered=False)
        db["risk_features"].bulk_write(feature_operations, ordered=False)
    payload = {
        "format": "lifemap-risk-logistic-v1",
        "feature_names": list(FEATURE_NAMES),
        "coefficients": model.coefficients.toArray().tolist(),
        "intercept": float(model.intercept),
        "threshold": 0.5,
        "training_cutoff_utc": cutoff.isoformat().replace("+00:00", "Z"),
        "metrics": {
            "auc_roc": auc_roc,
            "auc_pr": auc_pr,
            "recall_at_threshold": recall,
            "train_rows": train_count,
            "test_rows": test_count,
            "train_label_counts": {
                "0": negative_count,
                "1": positive_count,
            },
            "test_label_counts": {
                "0": test_class_counts.get(0, 0),
                "1": test_class_counts.get(1, 0),
            },
            "test_positive_prevalence": test_positive_prevalence,
            "test_confusion_matrix": {
                "true_positive": true_positive,
                "false_negative": false_negative,
                "false_positive": counts.get((0, 1), 0),
                "true_negative": counts.get((0, 0), 0),
            },
            "threshold_selection": "fixed 0.5; report recall at the default logistic threshold",
            "class_weighting": "inverse-frequency weights computed from training split",
            "split": "deterministic xxhash64(business_id) modulo 5; test bucket 0",
            "imbalance_interpretation": (
                "Compare AUC-PR with test_positive_prevalence; AUC-ROC can look "
                "strong under class imbalance even when precision is low."
            ),
        },
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    test_predictions.unpersist()
    spark.stop()
    return payload["metrics"]


def _build_survival_curves(
    businesses: list[dict[str, Any]],
    db: Any,
    *,
    precision: int,
    minimum_group_size: int,
) -> dict[str, Any]:
    usable = []
    for business in businesses:
        life = business.get("life") or {}
        first = life.get("first_review_date")
        last = life.get("last_review_date")
        coordinates = (business.get("location") or {}).get("coordinates")
        if first is None or last is None or not coordinates:
            continue
        usable.append((business, _utc_datetime(first), _utc_datetime(last)))
    if not usable:
        raise RuntimeError("No businesses have usable review lifetimes")
    origin = min(first for _, first, _ in usable)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for business, first, last in usable:
        observation = survival_observation(
            first,
            last,
            origin,
            event=int(business.get("is_open", 1)) == 0,
        )
        grouped[("category", business.get("primary_category") or "Restaurants")].append(
            observation
        )
        coordinates = business["location"]["coordinates"]
        cell = encode_geohash(float(coordinates[1]), float(coordinates[0]), precision)
        grouped[("geohash", cell)].append(observation)
    operations = []
    suppressed_groups = 0
    for (group_type, key), observations in grouped.items():
        result = kaplan_meier(observations)
        suppressed = len(observations) < minimum_group_size
        suppressed_groups += int(suppressed)
        document = {
            "_id": f"{group_type}:{key}",
            "group_type": group_type,
            "key": key,
            "businesses": len(observations),
            "suppressed": suppressed,
            "origin_date": origin,
            "median_days": None if suppressed else result["median_days"],
            "points": [] if suppressed else result["points"],
            "events": result["events"],
            "censored": result["censored"],
            "entry_method": "delayed entry from each first observed review date",
            "lifetime_proxy": "first-to-last review dates; not actual opening/closing dates",
            "updated_at": datetime.now(timezone.utc),
        }
        operations.append(
            ReplaceOne({"_id": document["_id"]}, document, upsert=True)
        )
        if len(operations) >= 500:
            db["survival_curves"].bulk_write(operations, ordered=False)
            operations.clear()
    if operations:
        db["survival_curves"].bulk_write(operations, ordered=False)
    return {
        "businesses_with_lifetimes": len(usable),
        "groups": len(grouped),
        "suppressed_small_groups": suppressed_groups,
        "minimum_group_size": minimum_group_size,
        "origin_date": origin.isoformat().replace("+00:00", "Z"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build E2 Kaplan-Meier curves and the closure-risk model."
    )
    parser.add_argument("--cutoff", type=_cutoff, default=_cutoff(
        os.getenv("LIFEMAP_TRAINING_CUTOFF", "2021-01-01T00:00:00Z")
    ))
    parser.add_argument("--horizon-days", type=int, default=365)
    parser.add_argument("--geohash-precision", type=int, default=5)
    parser.add_argument("--minimum-group-size", type=int, default=20)
    parser.add_argument(
        "--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    )
    parser.add_argument("--database", default=os.getenv("LIFEMAP_DATABASE", "lifemap"))
    parser.add_argument(
        "--risk-model",
        type=Path,
        default=Path("/lifemap/risk_model_as_of_2021-01-01.json"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("/lifemap/e2_analytics_report.json"),
    )
    args = parser.parse_args()
    if args.horizon_days <= 0 or args.minimum_group_size < 1:
        parser.error("horizon and minimum group size must be positive")
    if not 1 <= args.geohash_precision <= 12:
        parser.error("geohash precision must be between 1 and 12")

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        db = client[args.database]
        businesses = list(db["businesses"].find({}))
        rows, label_metadata = _business_features(
            db, args.cutoff, args.horizon_days
        )
        if not rows:
            raise RuntimeError("No eligible businesses for risk-model training")
        risk_metrics = _fit_risk_model(
            rows,
            output_path=args.risk_model,
            db=db,
            cutoff=args.cutoff,
        )
        survival_metadata = _build_survival_curves(
            businesses,
            db,
            precision=args.geohash_precision,
            minimum_group_size=args.minimum_group_size,
        )
        report = {
            "cutoff_utc": args.cutoff.isoformat().replace("+00:00", "Z"),
            "horizon_days": args.horizon_days,
            "risk_labels": label_metadata,
            "risk_model": risk_metrics,
            "survival": survival_metadata,
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
