from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectMongoSchema")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

try:
    trips = spark.read.format("mongodb").load()

    print("Esquema de taxi_geospatial.trips:")
    trips.printSchema()

    print("Muestra de documentos:")
    trips.select(
        "trip_id",
        "location",
        "destination",
        "started_at",
    ).show(5, truncate=False)
finally:
    spark.stop()