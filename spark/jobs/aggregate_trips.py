from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CELL_SIZE_DEGREES = 0.01


spark = (
    SparkSession.builder
    .appName("TaxiSpatialTemporalAggregation")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

try:
    trips = spark.read.format("mongodb").load()

    longitude = F.col("location.coordinates").getItem(0)
    latitude = F.col("location.coordinates").getItem(1)
    grid_longitude = F.floor(longitude / F.lit(CELL_SIZE_DEGREES)).cast("int")
    grid_latitude = F.floor(latitude / F.lit(CELL_SIZE_DEGREES)).cast("int")

    aggregates = (
        trips.select(
            grid_longitude.alias("grid_lon"),
            grid_latitude.alias("grid_lat"),
            F.hour("started_at").alias("hour_utc"),
        )
        .groupBy("grid_lon", "grid_lat", "hour_utc")
        .agg(F.count("*").alias("trip_count"))
        .withColumn(
            "cell_center",
            F.struct(
                F.lit("Point").alias("type"),
                F.array(
                    (
                        (F.col("grid_lon") + F.lit(0.5))
                        * F.lit(CELL_SIZE_DEGREES)
                    ).cast("double"),
                    (
                        (F.col("grid_lat") + F.lit(0.5))
                        * F.lit(CELL_SIZE_DEGREES)
                    ).cast("double"),
                ).alias("coordinates"),
            ),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        group_count = aggregates.count()
        total_trip_count = aggregates.agg(
            F.sum("trip_count").alias("total_trip_count")
        ).first()["total_trip_count"]
        if not group_count or total_trip_count is None:
            raise RuntimeError("Spark produced no spatial-temporal aggregate rows.")

        (
            aggregates.write.format("mongodb")
            .mode("overwrite")
            .save()
        )
        print(
            f"Saved {group_count} groups covering "
            f"{total_trip_count} trips to taxi_geospatial.trip_aggregates."
        )
    finally:
        aggregates.unpersist()
finally:
    spark.stop()
