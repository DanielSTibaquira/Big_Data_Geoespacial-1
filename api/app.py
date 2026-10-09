from __future__ import annotations

import math
import os
from typing import Any

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import PyMongoError

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


def _number(name: str, minimum: float, maximum: float) -> float:
    value = request.args.get(name)
    if value is None:
        raise ValueError(f"Missing required parameter: {name}")
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


def create_app(mongo_client: Any | None = None) -> Flask:
    app = Flask(__name__)
    client = mongo_client or MongoClient(
        os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=5000,
    )
    database = client[os.getenv("MONGO_DATABASE", "taxi_geospatial")]
    trips = database[os.getenv("MONGO_COLLECTION", "trips")]
    aggregates = database[os.getenv("MONGO_AGGREGATES_COLLECTION", "trip_aggregates")]
    app.extensions["mongo_client"] = client

    @app.errorhandler(PyMongoError)
    def handle_mongo_error(error: PyMongoError):
        app.logger.error("MongoDB request failed: %s", error)
        return jsonify(error="MongoDB is unavailable"), 503

    @app.get("/health")
    def health():
        client.admin.command("ping")
        return jsonify(status="ok")

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

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
