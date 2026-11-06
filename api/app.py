from __future__ import annotations

import math
import os
import socket
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from api.sentiment import load_serving_model, predict_sentiment

RESULT_LIMIT = 500
PROJECTION = {
    "_id": 0,
    "trip_id": 1,
    "call_type": 1,
    "taxi_id": 1,
    "started_at": 1,
    "location": 1,
    "destination": 1,
    "point_count": 1,
    "trip_duration_seconds": 1,
}
BUSINESS_PROJECTION = {
    "_id": 0,
    "business_id": 1,
    "name": 1,
    "city": 1,
    "state": 1,
    "stars": 1,
    "categories": 1,
    "primary_category": 1,
    "location": 1,
    "is_open": 1,
    "life": 1,
    "sentiment": 1,
}


def _tcp_probe(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _optional_category() -> str | None:
    category = request.args.get("category")
    if category is None:
        return None
    category = category.strip()
    if not category or len(category) > 80:
        raise ValueError("Parameter category must contain 1 to 80 characters")
    return category


def _number(
    name: str,
    minimum: float,
    maximum: float,
    *,
    default: float | None = None,
) -> float:
    value = request.args.get(name)
    if value is None:
        if default is None:
            raise ValueError(f"Missing required parameter: {name}")
        return default
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(f"Parameter {name} must be a number") from exc
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise ValueError(
            f"Parameter {name} must be between {minimum} and {maximum}"
        )
    return number


def _limit() -> int:
    return _parse_limit(request.args.get("limit", "100"))


def _parse_limit(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("Parameter limit must be an integer")
    try:
        limit = int(value)
    except ValueError as exc:
        raise ValueError("Parameter limit must be an integer") from exc
    if not 1 <= limit <= RESULT_LIMIT:
        raise ValueError(f"Parameter limit must be between 1 and {RESULT_LIMIT}")
    return limit


def _geojson_polygon(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("type") != "Polygon":
        raise ValueError("polygon must be a GeoJSON Polygon")

    coordinates = value.get("coordinates")
    if not isinstance(coordinates, list) or not coordinates:
        raise ValueError("Polygon coordinates must contain at least one ring")

    for ring_index, ring in enumerate(coordinates):
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValueError(
                f"Polygon ring {ring_index} must contain at least four positions"
            )

        for position in ring:
            if not isinstance(position, list) or len(position) != 2:
                raise ValueError(
                    "Polygon positions must contain longitude and latitude only"
                )
            longitude, latitude = position
            if (
                isinstance(longitude, bool)
                or isinstance(latitude, bool)
                or not isinstance(longitude, (int, float))
                or not isinstance(latitude, (int, float))
            ):
                raise ValueError("Polygon coordinates must be numeric")
            if (
                (isinstance(longitude, float) and not math.isfinite(longitude))
                or (isinstance(latitude, float) and not math.isfinite(latitude))
                or not -180 <= longitude <= 180
                or not -90 <= latitude <= 90
            ):
                raise ValueError(
                    "Polygon coordinates must be within WGS84 longitude "
                    "and latitude ranges"
                )

        if ring[0] != ring[-1]:
            raise ValueError(f"Polygon ring {ring_index} must be closed")
        if len({tuple(position) for position in ring[:-1]}) < 3:
            raise ValueError(
                f"Polygon ring {ring_index} must contain at least three distinct positions"
            )

    return {"type": "Polygon", "coordinates": coordinates}


def _bounding_polygon() -> dict[str, Any]:
    west = _number("min_lon", -180, 180)
    south = _number("min_lat", -90, 90)
    east = _number("max_lon", -180, 180)
    north = _number("max_lat", -90, 90)
    if west >= east or south >= north:
        raise ValueError("Bounding box minimums must be less than maximums")
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]
        ],
    }


def create_app(
    mongo_client: Any | None = None,
    *,
    model_path: Path | None = None,
) -> Flask:
    app = Flask(__name__)
    client = (
        mongo_client
        if mongo_client is not None
        else MongoClient(
            os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            serverSelectionTimeoutMS=5000,
        )
    )
    database = client[os.getenv("MONGO_DATABASE", "taxi_geospatial")]
    trips = database[os.getenv("MONGO_COLLECTION", "trips")]
    aggregates = database[os.getenv("MONGO_AGGREGATES_COLLECTION", "trip_aggregates")]
    lifemap_database = client[os.getenv("LIFEMAP_DATABASE", "lifemap")]
    businesses = lifemap_database[
        os.getenv("LIFEMAP_BUSINESSES_COLLECTION", "businesses")
    ]
    closure_rates = lifemap_database[
        os.getenv("LIFEMAP_CLOSURE_COLLECTION", "closure_rates")
    ]
    serving_model_path = model_path or Path(
        os.getenv("LIFEMAP_MODEL_PATH", "/lifemap/model_serving.json")
    )
    app.extensions["mongo_client"] = client
    app.extensions["sentiment_model"] = load_serving_model(serving_model_path)
    app.config["SERVICE_PROBE"] = _tcp_probe
    app.config["SERVICE_HOSTS"] = {
        "spark": (
            os.getenv("SPARK_MASTER_HOST", "spark-master"),
            int(os.getenv("SPARK_MASTER_PORT", "7077")),
        ),
        "dask": (
            os.getenv("DASK_SCHEDULER_HOST", "dask-scheduler"),
            int(os.getenv("DASK_SCHEDULER_PORT", "8786")),
        ),
    }

    @app.errorhandler(PyMongoError)
    def handle_mongo_error(error: PyMongoError):
        app.logger.error("MongoDB request failed: %s", error)
        return jsonify(error="MongoDB is unavailable"), 503

    @app.get("/health")
    def health():
        client.admin.command("ping")
        probe = app.config["SERVICE_PROBE"]
        services = {"api": "ok", "mongodb": "ok"}
        services.update(
            {
                name: (
                "ok"
                if probe(host, port)
                else "unavailable"
                )
                for name, (host, port) in app.config["SERVICE_HOSTS"].items()
            }
        )
        commit = os.getenv("GIT_COMMIT", "unknown")
        status = "ok" if all(value == "ok" for value in services.values()) else "degraded"
        return jsonify(status=status, services=services, commit=commit), (
            200 if status == "ok" else 503
        )

    @app.get("/")
    def index():
        return render_template("lifemap.html")

    @app.get("/taxi")
    def taxi_index():
        return render_template("index.html")

    @app.get("/api/v1/trips/near")
    def trips_near():
        try:
            longitude = _number("longitude", -180, 180)
            latitude = _number("latitude", -90, 90)
            radius = _number("radius_m", 1, 100_000)
            limit = _limit()
        except ValueError as error:
            return jsonify(error=str(error)), 400

        query = {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [longitude, latitude],
                    },
                    "$maxDistance": radius,
                }
            }
        }
        results = list(trips.find(query, PROJECTION).limit(limit))
        return jsonify(count=len(results), results=results)

    @app.get("/api/v1/trips/within")
    def trips_within():
        try:
            polygon = _bounding_polygon()
            limit = _limit()
        except ValueError as error:
            return jsonify(error=str(error)), 400

        query = {"location": {"$geoWithin": {"$geometry": polygon}}}
        results = list(trips.find(query, PROJECTION).limit(limit))
        return jsonify(count=len(results), results=results)

    @app.post("/api/v1/trips/within")
    def trips_within_polygon():
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify(error="Request body must be a JSON object"), 400
        try:
            polygon = _geojson_polygon(body.get("polygon"))
            limit_value = body.get("limit", 100)
            if isinstance(limit_value, bool) or not isinstance(limit_value, int):
                raise ValueError("Parameter limit must be an integer")
            limit = _parse_limit(limit_value)
        except ValueError as error:
            return jsonify(error=str(error)), 400

        query = {"location": {"$geoWithin": {"$geometry": polygon}}}
        results = list(trips.find(query, PROJECTION).limit(limit))
        return jsonify(count=len(results), results=results)

    @app.get("/api/v1/trips/aggregate/near")
    def aggregate_trips_near():
        try:
            longitude = _number("longitude", -180, 180)
            latitude = _number("latitude", -90, 90)
            radius = _number("radius_m", 1, 100_000)
        except ValueError as error:
            return jsonify(error=str(error)), 400

        trips.create_index([("location", "2dsphere")])
        pipeline = [
            {
                "$geoNear": {
                    "near": {
                        "type": "Point",
                        "coordinates": [longitude, latitude],
                    },
                    "key": "location",
                    "distanceField": "distance_m",
                    "maxDistance": radius,
                    "spherical": True,
                }
            },
            {
                "$group": {
                    "_id": "$call_type",
                    "trip_count": {"$sum": 1},
                    "average_distance_m": {"$avg": "$distance_m"},
                    "average_trip_duration_seconds": {
                        "$avg": "$trip_duration_seconds"
                    },
                }
            },
            {"$sort": {"trip_count": -1, "_id": 1}},
        ]
        results = list(trips.aggregate(pipeline))
        return jsonify(count=len(results), results=results)

    @app.get("/api/v1/aggregates/within")
    def aggregates_within():
        try:
            polygon = _bounding_polygon()
            limit = _limit()
        except ValueError as error:
            return jsonify(error=str(error)), 400

        aggregates.create_index([("cell_center", "2dsphere")])
        query = {"cell_center": {"$geoWithin": {"$geometry": polygon}}}
        results = list(aggregates.find(query, {"_id": 0}).limit(limit))
        return jsonify(count=len(results), results=results)

    @app.get("/businesses/near")
    def businesses_near():
        try:
            longitude = _number("lon", -180, 180)
            latitude = _number("lat", -90, 90)
            radius = _number("radius_m", 1, 100_000)
            category = _optional_category()
            limit = _limit()
        except ValueError as error:
            return jsonify(error=str(error)), 400

        query: dict[str, Any] = {
            "location": {
                "$near": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [longitude, latitude],
                    },
                    "$maxDistance": radius,
                }
            }
        }
        if category:
            query["categories"] = category
        businesses.create_index([("location", "2dsphere")], name="location_2dsphere")
        results = list(businesses.find(query, BUSINESS_PROJECTION).limit(limit))
        return jsonify(count=len(results), results=results)

    @app.route("/businesses/within", methods=["GET", "POST"])
    def businesses_within():
        if request.method == "GET":
            try:
                polygon = _bounding_polygon()
                limit = _limit()
            except ValueError as error:
                return jsonify(error=str(error)), 400
        else:
            body = request.get_json(silent=True)
            if not isinstance(body, dict):
                return jsonify(error="Request body must be a JSON object"), 400
            try:
                polygon = _geojson_polygon(body.get("polygon"))
                limit = _parse_limit(body.get("limit", 100))
            except ValueError as error:
                return jsonify(error=str(error)), 400

        businesses.create_index([("location", "2dsphere")], name="location_2dsphere")
        query = {"location": {"$geoWithin": {"$geometry": polygon}}}
        results = list(businesses.find(query, BUSINESS_PROJECTION).limit(limit))
        return jsonify(count=len(results), results=results)

    @app.get("/zones/summary")
    def zones_summary():
        try:
            longitude = _number("lon", -180, 180)
            latitude = _number("lat", -90, 90)
            radius = _number("radius_m", 1, 100_000)
            minimum_days = _number(
                "min_days_business",
                1,
                1_000_000_000,
                default=365,
            )
            category = _optional_category()
        except ValueError as error:
            return jsonify(error=str(error)), 400

        geo_near: dict[str, Any] = {
            "near": {
                "type": "Point",
                "coordinates": [longitude, latitude],
            },
            "key": "location",
            "distanceField": "distance_m",
            "maxDistance": radius,
            "spherical": True,
        }
        if category:
            geo_near["query"] = {"categories": category}
        businesses.create_index([("location", "2dsphere")], name="location_2dsphere")
        pipeline = [
            {"$geoNear": geo_near},
            {"$match": {"life.days_business": {"$gte": minimum_days}}},
            {
                "$group": {
                    "_id": None,
                    "businesses": {"$sum": 1},
                    "closures": {
                        "$sum": {"$cond": ["$life.event", 1, 0]}
                    },
                    "days_business": {"$sum": "$life.days_business"},
                    "average_sentiment": {"$avg": "$sentiment.mean_score"},
                }
            },
        ]
        result = next(iter(businesses.aggregate(pipeline)), None)
        if result is None:
            return jsonify(
                businesses=0,
                closures=0,
                days_business=0,
                closure_rate_pct=None,
                average_sentiment=None,
                min_days_business=minimum_days,
            )
        exposure = result["days_business"]
        return jsonify(
            businesses=result["businesses"],
            closures=result["closures"],
            days_business=exposure,
            closure_rate_pct=(
                result["closures"] * 365 / exposure * 100 if exposure else None
            ),
            average_sentiment=result.get("average_sentiment"),
            min_days_business=minimum_days,
        )

    @app.get("/stats/closure")
    def closure_stats():
        try:
            category = _optional_category()
            geohash = request.args.get("geohash")
            if geohash is not None and (
                not geohash.strip() or len(geohash.strip()) > 12
            ):
                raise ValueError("Parameter geohash must contain 1 to 12 characters")
            minimum_days = _number(
                "min_days_business",
                1,
                1_000_000_000,
                default=365,
            )
            limit = _limit()
        except ValueError as error:
            return jsonify(error=str(error)), 400
        if (category is None) == (geohash is None):
            return jsonify(error="Specify exactly one of category or geohash"), 400

        if category is not None:
            group_type = "category"
            key = category
        elif geohash is not None:
            group_type = "geohash"
            key = geohash.strip()
        else:
            return jsonify(error="Specify exactly one of category or geohash"), 400
        query = {
            "group_type": group_type,
            "key": key,
            "days_business": {"$gte": minimum_days},
        }
        results = list(
            closure_rates.find(query, {"_id": 0})
            .sort("rate_pct", -1)
            .limit(limit)
        )
        return jsonify(
            count=len(results),
            min_days_business=minimum_days,
            results=results,
        )

    @app.post("/predict")
    def predict():
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify(error="Request body must be a JSON object"), 400
        text = body.get("text")
        if not isinstance(text, str) or not text.strip():
            return jsonify(error="text must be a non-empty string"), 400
        if len(text) > 20_000:
            return jsonify(error="text must not exceed 20000 characters"), 400

        model = app.extensions["sentiment_model"]
        if model is None:
            model = load_serving_model(serving_model_path)
            app.extensions["sentiment_model"] = model
        if model is None:
            return jsonify(error="Sentiment model has not been trained yet"), 503
        return jsonify(predict_sentiment(text, model))

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
