from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import time
from pathlib import Path
from typing import Any

from lifemap_metrics import annualized_closure_rate, encode_geohash


LABELS = ("negative", "neutral", "positive")
LABEL_INDEX = {label: index for index, label in enumerate(LABELS)}
PARTITION_COUNTS = (2, 8, 32, 200)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train Yelp sentiment models and compute closure rates with Spark."
    )
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
    parser.add_argument("--database", default=os.getenv("LIFEMAP_DATABASE", "lifemap"))
    parser.add_argument("--businesses-collection", default="businesses")
    parser.add_argument("--reviews-collection", default="reviews")
    parser.add_argument("--closure-collection", default="closure_rates")
    parser.add_argument("--minimum-days-business", type=float, default=365)
    parser.add_argument("--geohash-precision", type=int, default=5)
    parser.add_argument("--model-dir", type=Path, default=Path("/lifemap/models"))
    parser.add_argument(
        "--serving-model",
        type=Path,
        default=Path("/lifemap/model_serving.json"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("/lifemap/batch_report.json"),
    )
    parser.add_argument("--skip-benchmark", action="store_true")
    args = parser.parse_args()
    if args.minimum_days_business <= 0 or not math.isfinite(args.minimum_days_business):
        parser.error("--minimum-days-business must be a positive finite number")
    if not 1 <= args.geohash_precision <= 12:
        parser.error("--geohash-precision must be between 1 and 12")
    return args


def _read_collection(spark: Any, uri: str, database: str, collection: str) -> Any:
    return (
        spark.read.format("mongodb")
        .option("connection.uri", f"{uri.rstrip('/')}/{database}.{collection}")
        .option("batchSize", 16)
        .load()
    )


def _write_collection(frame: Any, uri: str, database: str, collection: str) -> None:
    (
        frame.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", f"{uri.rstrip('/')}/{database}.{collection}")
        .save()
    )


def _closure_emissions(row: Any, precision: int) -> list[tuple[tuple[str, str], tuple[int, float]]]:
    location = row["location"]
    coordinates = location["coordinates"] if location else None
    life = row["life"]
    if not coordinates or not life or life["days_business"] is None:
        return []

    longitude = float(coordinates[0])
    latitude = float(coordinates[1])
    if not (-180 <= longitude <= 180 and -90 <= latitude <= 90):
        return []
    category = row["primary_category"] or "Restaurants"
    closure = int(bool(life["event"]))
    exposure = max(0.0, float(life["days_business"]))
    cell = encode_geohash(latitude, longitude, precision)
    value = (closure, exposure)
    return [(("category", category), value), (("geohash", cell), value)]


def _sum_pair(left: tuple[int, float], right: tuple[int, float]) -> tuple[int, float]:
    return left[0] + right[0], left[1] + right[1]


def _dataframe_closure_totals(base: Any, precision: int) -> dict[tuple[str, str], tuple[int, float]]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    geohash_udf = F.udf(
        lambda latitude, longitude: (
            encode_geohash(float(latitude), float(longitude), precision)
            if latitude is not None and longitude is not None
            else None
        ),
        StringType(),
    )
    category = (
        base.groupBy("primary_category")
        .agg(
            F.sum(F.col("life.event").cast("long")).alias("closures"),
            F.sum("life.days_business").alias("days_business"),
        )
        .collect()
    )
    geohash = (
        base.withColumn(
            "geohash",
            geohash_udf(
                F.col("location.coordinates").getItem(1),
                F.col("location.coordinates").getItem(0),
            ),
        )
        .groupBy("geohash")
        .agg(
            F.sum(F.col("life.event").cast("long")).alias("closures"),
            F.sum("life.days_business").alias("days_business"),
        )
        .collect()
    )
    totals: dict[tuple[str, str], tuple[int, float]] = {}
    for row in category:
        totals[("category", row["primary_category"] or "Restaurants")] = (
            int(row["closures"] or 0),
            float(row["days_business"] or 0),
        )
    for row in geohash:
        if row["geohash"]:
            totals[("geohash", row["geohash"])] = (
                int(row["closures"] or 0),
                float(row["days_business"] or 0),
            )
    return totals


def compute_closure_rates(
    spark: Any,
    businesses: Any,
    *,
    precision: int,
    minimum_days_business: float,
) -> tuple[Any, dict[str, Any]]:
    from pyspark.sql import Row
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    base = businesses.select(
        "business_id",
        "primary_category",
        "location",
        "life",
    ).where(
        businesses["life"]["days_business"].isNotNull()
        & businesses["life"]["event"].isNotNull()
        & businesses["location"]["coordinates"].isNotNull()
    )
    rdd_totals = dict(
        base.rdd.flatMap(lambda row: _closure_emissions(row, precision))
        .reduceByKey(_sum_pair)
        .collect()
    )
    dataframe_totals = _dataframe_closure_totals(base, precision)
    if set(rdd_totals) != set(dataframe_totals):
        raise RuntimeError("RDD and DataFrame closure-rate groups do not match")
    for key, value in rdd_totals.items():
        expected = dataframe_totals[key]
        if value[0] != expected[0] or not math.isclose(
            value[1], expected[1], rel_tol=1e-9, abs_tol=1e-6
        ):
            raise RuntimeError(f"RDD and DataFrame aggregates differ for {key}")

    schema = StructType(
        [
            StructField("group_type", StringType(), False),
            StructField("key", StringType(), False),
            StructField("closures", IntegerType(), False),
            StructField("days_business", DoubleType(), False),
            StructField("rate_pct", DoubleType(), False),
            StructField("minimum_days_business", DoubleType(), False),
        ]
    )
    records = [
        Row(
            group_type=group_type,
            key=key,
            closures=closures,
            days_business=days,
            rate_pct=annualized_closure_rate(closures, days),
            minimum_days_business=minimum_days_business,
        )
        for (group_type, key), (closures, days) in sorted(rdd_totals.items())
        if days >= minimum_days_business and days > 0
    ]
    rate_frame = spark.createDataFrame(records, schema=schema)
    return rate_frame, {
        "groups_compared": len(rdd_totals),
        "groups_persisted": len(records),
        "minimum_days_business": minimum_days_business,
        "formula": "closures / days_business * 365 * 100",
        "rdd_dataframe_match": True,
    }


def _confusion_metrics(predictions: Any) -> dict[str, Any]:
    rows = (
        predictions.groupBy("label", "prediction")
        .count()
        .collect()
    )
    matrix = [[0 for _ in LABELS] for _ in LABELS]
    for row in rows:
        matrix[int(row["label"])][int(row["prediction"])] = int(row["count"])

    per_class: dict[str, dict[str, float | int]] = {}
    total_correct = 0
    total = sum(sum(values) for values in matrix)
    for label, index in LABEL_INDEX.items():
        true_positive = matrix[index][index]
        false_positive = sum(matrix[other][index] for other in range(len(LABELS)) if other != index)
        false_negative = sum(matrix[index][other] for other in range(len(LABELS)) if other != index)
        precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
        recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        support = sum(matrix[index])
        per_class[label] = {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "support": support,
        }
        total_correct += true_positive

    weighted_f1 = (
        sum(per_class[label]["f1"] * per_class[label]["support"] for label in LABELS)
        / total
        if total
        else 0.0
    )
    return {
        "accuracy": total_correct / total if total else 0.0,
        "weighted_f1": weighted_f1,
        "per_class": per_class,
        "confusion_matrix": {
            "labels": list(LABELS),
            "counts": matrix,
        },
        "test_rows": total,
    }


def train_sentiment_models(
    spark: Any,
    reviews: Any,
    model_dir: Path,
    serving_model_path: Path,
) -> dict[str, Any]:
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.classification import LogisticRegression, NaiveBayes
    from pyspark.ml.feature import (
        CountVectorizer,
        CountVectorizerModel,
        IDF,
        IDFModel,
        RegexTokenizer,
        StopWordsRemover,
    )
    from pyspark.sql import functions as F

    labeled = (
        reviews.select("business_id", "text", "sentiment")
        .where(
            F.col("business_id").isNotNull()
            & F.col("text").isNotNull()
            & F.col("sentiment").isin(*LABELS)
        )
        .withColumn(
            "label",
            F.when(F.col("sentiment") == "negative", 0.0)
            .when(F.col("sentiment") == "neutral", 1.0)
            .otherwise(2.0),
        )
        .withColumn(
            "split_bucket",
            F.pmod(F.xxhash64("business_id"), F.lit(100)),
        )
    )
    train = labeled.where(F.col("split_bucket") >= 20).drop("split_bucket")
    test = labeled.where(F.col("split_bucket") < 20).drop("split_bucket")
    train_count, test_count = train.count(), test.count()
    if train_count == 0 or test_count == 0:
        raise RuntimeError("Business-level train/test partition produced an empty split")
    train_businesses = train.select("business_id").distinct().count()
    test_businesses = test.select("business_id").distinct().count()
    class_distribution = {
        row["sentiment"]: int(row["count"])
        for row in labeled.groupBy("sentiment").count().collect()
    }
    overlap = (
        train.select("business_id")
        .distinct()
        .join(test.select("business_id").distinct(), "business_id", "inner")
        .limit(1)
        .count()
    )
    if overlap:
        raise RuntimeError("Business leakage detected between train and test splits")

    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="tokens",
        pattern="\\W+",
        minTokenLength=2,
        toLowercase=True,
    )
    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_tokens",
        stopWords=StopWordsRemover.loadDefaultStopWords("english"),
    )
    vectorizer = CountVectorizer(
        inputCol="filtered_tokens",
        outputCol="raw_features",
        vocabSize=20_000,
        minDF=2,
    )
    idf = IDF(inputCol="raw_features", outputCol="features")
    feature_model = Pipeline(stages=[tokenizer, remover, vectorizer, idf]).fit(train)
    train_features = feature_model.transform(train).select("features", "label")
    test_features = feature_model.transform(test).select(
        "features",
        "label",
        "business_id",
    )
    if train_features.limit(1).count() == 0:
        raise RuntimeError("No training features were produced from Yelp review text")

    classifiers = {
        "naive_bayes": NaiveBayes(
            featuresCol="features",
            labelCol="label",
            modelType="multinomial",
        ),
        "logistic_regression": LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=50,
            regParam=0.01,
            elasticNetParam=0.0,
        ),
    }
    results: dict[str, Any] = {
        "split": {
            "strategy": "deterministic xxhash64(business_id), 80/20 by business",
            "train_rows": train_count,
            "test_rows": test_count,
            "train_businesses": train_businesses,
            "test_businesses": test_businesses,
            "class_distribution": class_distribution,
            "business_overlap": 0,
            "features": "RegexTokenizer + English StopWordsRemover + CountVectorizer + IDF",
        },
        "models": {},
    }
    saved_models: dict[str, Any] = {}
    for model_name, classifier in classifiers.items():
        fitted = classifier.fit(train_features)
        predictions = fitted.transform(test_features).select(
            "label", "prediction"
        )
        metrics = _confusion_metrics(predictions)
        results["models"][model_name] = metrics
        saved_model = PipelineModel(stages=[*feature_model.stages, fitted])
        target = model_dir / model_name
        target.parent.mkdir(parents=True, exist_ok=True)
        saved_model.write().overwrite().save(str(target))
        saved_models[model_name] = fitted

    logistic = saved_models["logistic_regression"]
    vectorizer_model = feature_model.stages[2]
    idf_model = feature_model.stages[3]
    if not isinstance(vectorizer_model, CountVectorizerModel) or not isinstance(
        idf_model, IDFModel
    ):
        raise TypeError("Unexpected feature-model stages for serving export")
    serving_model_path.parent.mkdir(parents=True, exist_ok=True)
    serving_payload = {
        "format": "lifemap-logistic-v1",
        "model_name": "logistic_regression",
        "labels": list(LABELS),
        "stop_words": StopWordsRemover.loadDefaultStopWords("english"),
        "vocabulary": {
            token: index for index, token in enumerate(vectorizer_model.vocabulary)
        },
        "idf": idf_model.idf.toArray().tolist(),
        "coefficients": logistic.coefficientMatrix.toArray().tolist(),
        "intercepts": logistic.interceptVector.toArray().tolist(),
    }
    serving_model_path.write_text(
        json.dumps(serving_payload, ensure_ascii=False),
        encoding="utf-8",
    )
    results["serving_model"] = str(serving_model_path)
    results["saved_models"] = {
        name: str(model_dir / name) for name in classifiers
    }
    return results


def run_balance_benchmark(spark: Any, businesses: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    cached = businesses.select("business_id", "city", "primary_category").cache()
    row_count = cached.count()
    measurements: list[dict[str, Any]] = []
    partition_distributions: dict[str, Any] = {}
    try:
        for partition_count in PARTITION_COUNTS:
            for strategy, key in (("unkeyed", None), ("by_city", "city")):
                distribution_key = f"{strategy}_{partition_count}"
                partitioned = (
                    cached.repartition(partition_count, key)
                    if key
                    else cached.repartition(partition_count)
                )
                counts = (
                    partitioned.select(F.spark_partition_id().alias("partition_id"))
                    .groupBy("partition_id")
                    .count()
                    .collect()
                )
                distribution = [0] * partition_count
                for item in counts:
                    distribution[int(item["partition_id"])] = int(item["count"])
                partition_distributions[distribution_key] = distribution

                partitioned.groupBy("city").count().count()
                durations = []
                for _ in range(3):
                    start = time.perf_counter()
                    partitioned.groupBy("city").count().count()
                    durations.append(time.perf_counter() - start)
                mean = statistics.mean(durations)
                measurements.append(
                    {
                        "partitions": partition_count,
                        "strategy": strategy,
                        "repetitions_seconds": durations,
                        "mean_seconds": mean,
                        "stdev_seconds": statistics.stdev(durations),
                    }
                )
        return {
            "rows": row_count,
            "warmup": "one untimed groupBy(city) per strategy/configuration",
            "measurements": measurements,
            "partition_rows": partition_distributions,
        }
    finally:
        cached.unpersist()


def main() -> None:
    args = parse_args()
    from pyspark import StorageLevel
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    builder = (
        SparkSession.builder.appName("LifeMapYelpBatchAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.mongodb.read.connection.uri",
            f"{args.mongo_uri.rstrip('/')}/{args.database}.{args.businesses_collection}",
        )
        .config(
            "spark.mongodb.write.connection.uri",
            f"{args.mongo_uri.rstrip('/')}/{args.database}.{args.closure_collection}",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.addPyFile(
        str(Path(__file__).with_name("lifemap_metrics.py"))
    )
    report: dict[str, Any] = {
        "minimum_days_business": args.minimum_days_business,
        "closure_formula": "closures / days_business * 365 * 100",
    }
    try:
        businesses = _read_collection(
            spark,
            args.mongo_uri,
            args.database,
            args.businesses_collection,
        )
        reviews = _read_collection(
            spark,
            args.mongo_uri,
            args.database,
            args.reviews_collection,
        ).select("business_id", "text", "sentiment").persist(StorageLevel.DISK_ONLY)
        business_base = businesses.where(
            F.col("life.days_business").isNotNull()
            & F.col("life.event").isNotNull()
            & F.col("location.coordinates").isNotNull()
        ).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            review_count = reviews.count()
            business_count = business_base.count()
            if not review_count or not business_count:
                raise RuntimeError("LifeMap MongoDB collections contain no usable data")

            model_results = train_sentiment_models(
                spark,
                reviews,
                args.model_dir,
                args.serving_model,
            )
            closure_frame, closure_results = compute_closure_rates(
                spark,
                business_base,
                precision=args.geohash_precision,
                minimum_days_business=args.minimum_days_business,
            )
            _write_collection(
                closure_frame,
                args.mongo_uri,
                args.database,
                args.closure_collection,
            )
            report.update(
                {
                    "businesses": business_count,
                    "reviews": review_count,
                    "sentiment": model_results,
                    "closure_rates": closure_results,
                }
            )
            if not args.skip_benchmark:
                report["balance_benchmark"] = run_balance_benchmark(
                    spark,
                    business_base,
                )
        finally:
            business_base.unpersist()
            reviews.unpersist()

        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
