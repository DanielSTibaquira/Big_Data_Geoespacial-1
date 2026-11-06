from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from api.app import create_app

VALID_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-8.62, 41.14],
            [-8.60, 41.14],
            [-8.61, 41.16],
            [-8.62, 41.14],
        ]
    ],
}


class FakeCursor(list):
    def __init__(self, documents):
        super().__init__(documents)
        self.limit_count = None

    def limit(self, count):
        self.limit_count = count
        del self[count:]
        return self

    def sort(self, key, direction):
        self.sort_spec = (key, direction)
        return self


class FakeCollection:
    def __init__(self, documents=None, aggregate_result=None):
        self.documents = documents or []
        self.query = None
        self.pipeline = None
        self.projection = None
        self.indexes = []
        self.cursor = None
        self.aggregate_result = aggregate_result or [
            {"_id": "A", "trip_count": 3}
        ]

    def find(self, query, projection=None):
        self.query = query
        self.projection = projection
        self.cursor = FakeCursor(self.documents)
        return self.cursor

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))

    def aggregate(self, pipeline):
        self.pipeline = pipeline
        return FakeCursor(self.aggregate_result)


class FakeDatabase:
    def __init__(self):
        self.collections = {
            "trips": FakeCollection(
                [
                    {
                        "trip_id": "trip-1",
                        "started_at": datetime(2013, 7, 1, tzinfo=timezone.utc),
                    },
                    {"trip_id": "trip-2"},
                ]
            ),
            "trip_aggregates": FakeCollection([{"trip_count": 3}]),
            "businesses": FakeCollection(
                [{"business_id": "b1", "name": "Restaurant"}],
                aggregate_result=[
                    {
                        "_id": None,
                        "businesses": 4,
                        "closures": 1,
                        "days_business": 730,
                        "average_sentiment": 0.25,
                    }
                ],
            ),
            "closure_rates": FakeCollection(
                [{"group_type": "category", "key": "Italian", "rate_pct": 5.0}]
            ),
            "reviews": FakeCollection(),
        }

    def __getitem__(self, name):
        return self.collections[name]


class FakeClient:
    def __init__(self):
        self.database = FakeDatabase()
        self.admin = self

    def __getitem__(self, name):
        return self.database

    def command(self, command):
        assert command == "ping"
        return {"ok": 1}


def create_test_client(model_path=None):
    mongo = FakeClient()
    app = create_app(mongo, model_path=model_path)
    app.config["SERVICE_PROBE"] = lambda host, port: True
    app.config.update(TESTING=True)
    return app.test_client(), mongo


def test_health_endpoint_checks_mongodb():
    client, _ = create_test_client()

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json["status"] == "ok"
    assert response.json["services"] == {
        "api": "ok",
        "mongodb": "ok",
        "spark": "ok",
        "dask": "ok",
    }


def test_homepage_serves_lifemap_leaflet_frontend():
    client, _ = create_test_client()

    response = client.get("/")

    assert response.status_code == 200
    assert b"LifeMap | Restaurantes de Philadelphia" in response.data
    assert b"id=\"business-search\"" in response.data
    assert b"id=\"map\"" in response.data


def test_taxi_frontend_remains_available():
    client, _ = create_test_client()

    response = client.get("/taxi")

    assert response.status_code == 200
    assert b"Taxi Geo Explorer" in response.data
    assert b"id=\"map\"" in response.data


def test_near_endpoint_builds_meter_based_geo_near_query_and_caps_results():
    client, mongo = create_test_client()

    response = client.get(
        "/api/v1/trips/near?longitude=-8.61&latitude=41.14&radius_m=500&limit=1"
    )

    assert response.status_code == 200
    assert response.json["count"] == 1
    assert mongo.database.collections["trips"].query == {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [-8.61, 41.14],
                },
                "$maxDistance": 500,
            }
        }
    }


def test_within_endpoint_uses_a_closed_geojson_bounding_polygon():
    client, mongo = create_test_client()

    response = client.get(
        "/api/v1/trips/within"
        "?min_lon=-8.7&min_lat=41.1&max_lon=-8.5&max_lat=41.2"
    )

    assert response.status_code == 200
    geometry = mongo.database.collections["trips"].query["location"]["$geoWithin"][
        "$geometry"
    ]
    assert geometry["type"] == "Polygon"
    assert geometry["coordinates"][0][0] == [-8.7, 41.1]
    assert geometry["coordinates"][0][-1] == [-8.7, 41.1]


def test_within_post_accepts_geojson_polygon_and_applies_limit():
    client, mongo = create_test_client()

    response = client.post(
        "/api/v1/trips/within",
        json={"polygon": VALID_POLYGON, "limit": 1},
    )

    trips = mongo.database.collections["trips"]
    assert response.status_code == 200
    assert response.json["count"] == 1
    assert len(response.json["results"]) == 1
    assert trips.query == {
        "location": {"$geoWithin": {"$geometry": VALID_POLYGON}}
    }
    assert trips.cursor.limit_count == 1


def test_within_post_defaults_limit_to_100():
    client, mongo = create_test_client()

    response = client.post(
        "/api/v1/trips/within",
        json={"polygon": VALID_POLYGON},
    )

    assert response.status_code == 200
    assert mongo.database.collections["trips"].cursor.limit_count == 100


@pytest.mark.parametrize(
    "polygon",
    [
        None,
        {"type": "Point", "coordinates": [-8.61, 41.14]},
        {"type": "Polygon", "coordinates": []},
        {"type": "Polygon", "coordinates": [[[-8.62, 41.14], [-8.60, 41.14]]]},
        {
            "type": "Polygon",
            "coordinates": [[[-8.62, 41.14], [-8.60, 41.14], [-8.61, 41.16]]],
        },
        {
            "type": "Polygon",
            "coordinates": [
                [[-181, 41.14], [-8.60, 41.14], [-8.61, 41.16], [-181, 41.14]]
            ],
        },
        {
            "type": "Polygon",
            "coordinates": [
                [[-8.62, 91], [-8.60, 41.14], [-8.61, 41.16], [-8.62, 91]]
            ],
        },
        {
            "type": "Polygon",
            "coordinates": [
                [
                    [-8.62, 41.14, 0],
                    [-8.60, 41.14, 0],
                    [-8.61, 41.16, 0],
                    [-8.62, 41.14, 0],
                ]
            ],
        },
    ],
)
def test_within_post_rejects_invalid_polygons(polygon):
    client, mongo = create_test_client()

    response = client.post(
        "/api/v1/trips/within",
        json={"polygon": polygon},
    )

    assert response.status_code == 400
    assert mongo.database.collections["trips"].query is None


@pytest.mark.parametrize("limit", [0, 501, 1.5, "2", True, None])
def test_within_post_rejects_invalid_limits(limit):
    client, mongo = create_test_client()

    response = client.post(
        "/api/v1/trips/within",
        json={"polygon": VALID_POLYGON, "limit": limit},
    )

    assert response.status_code == 400
    assert mongo.database.collections["trips"].query is None


def test_within_post_rejects_malformed_json():
    client, mongo = create_test_client()

    response = client.post(
        "/api/v1/trips/within",
        data="{",
        content_type="application/json",
    )

    assert response.status_code == 400
    assert mongo.database.collections["trips"].query is None


def test_near_aggregation_uses_geonear_then_groups_by_call_type():
    client, mongo = create_test_client()

    response = client.get(
        "/api/v1/trips/aggregate/near"
        "?longitude=-8.61&latitude=41.14&radius_m=500"
    )

    trips = mongo.database.collections["trips"]
    assert response.status_code == 200
    assert response.json["results"] == [{"_id": "A", "trip_count": 3}]
    assert trips.pipeline[0]["$geoNear"]["near"] == {
        "type": "Point",
        "coordinates": [-8.61, 41.14],
    }
    assert trips.pipeline[0]["$geoNear"]["maxDistance"] == 500
    assert trips.pipeline[1]["$group"]["_id"] == "$call_type"
    assert trips.indexes == [([("location", "2dsphere")], {})]


def test_aggregate_endpoint_ensures_geospatial_index():
    client, mongo = create_test_client()

    response = client.get(
        "/api/v1/aggregates/within"
        "?min_lon=-8.7&min_lat=41.1&max_lon=-8.5&max_lat=41.2"
    )

    assert response.status_code == 200
    assert mongo.database.collections["trip_aggregates"].indexes == [
        ([("cell_center", "2dsphere")], {})
    ]


def test_api_rejects_invalid_coordinates_and_bounding_boxes():
    client, _ = create_test_client()

    invalid_coordinate = client.get(
        "/api/v1/trips/near?longitude=NaN&latitude=41.14&radius_m=500"
    )
    invalid_box = client.get(
        "/api/v1/trips/within"
        "?min_lon=-8.5&min_lat=41.1&max_lon=-8.7&max_lat=41.2"
    )

    assert invalid_coordinate.status_code == 400
    assert invalid_box.status_code == 400


def test_businesses_near_uses_geo_near_and_optional_category():
    client, mongo = create_test_client()

    response = client.get(
        "/businesses/near?lon=-75.16&lat=39.95&radius_m=1000"
        "&category=Italian&limit=5"
    )

    businesses = mongo.database.collections["businesses"]
    assert response.status_code == 200
    assert response.json["count"] == 1
    assert businesses.query == {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [-75.16, 39.95],
                },
                "$maxDistance": 1000,
            }
        },
        "categories": "Italian",
    }
    assert businesses.indexes == [
        ([("location", "2dsphere")], {"name": "location_2dsphere"})
    ]


def test_businesses_within_accepts_geojson_polygon():
    client, mongo = create_test_client()

    response = client.post(
        "/businesses/within",
        json={"polygon": VALID_POLYGON, "limit": 1},
    )

    businesses = mongo.database.collections["businesses"]
    assert response.status_code == 200
    assert businesses.query == {
        "location": {"$geoWithin": {"$geometry": VALID_POLYGON}}
    }
    assert businesses.cursor.limit_count == 1


def test_zones_summary_uses_geonear_and_annualized_exposure_rate():
    client, mongo = create_test_client()

    response = client.get(
        "/zones/summary?lon=-75.16&lat=39.95&radius_m=1000"
        "&min_days_business=100"
    )

    businesses = mongo.database.collections["businesses"]
    assert response.status_code == 200
    assert response.json["closure_rate_pct"] == pytest.approx(50.0)
    assert businesses.pipeline[0]["$geoNear"]["maxDistance"] == 1000
    assert businesses.pipeline[1]["$match"] == {
        "life.days_business": {"$gte": 100.0}
    }


def test_closure_stats_requires_one_group_and_minimum_exposure():
    client, mongo = create_test_client()

    response = client.get(
        "/stats/closure?category=Italian&min_days_business=500"
    )

    closure_rates = mongo.database.collections["closure_rates"]
    assert response.status_code == 200
    assert closure_rates.query == {
        "group_type": "category",
        "key": "Italian",
        "days_business": {"$gte": 500.0},
    }
    assert closure_rates.cursor.sort_spec == ("rate_pct", -1)

    invalid = client.get("/stats/closure")
    assert invalid.status_code == 400


def test_predict_uses_exported_logistic_model(tmp_path):
    model_path = tmp_path / "model_serving.json"
    model_path.write_text(
        json.dumps(
            {
                "format": "lifemap-logistic-v1",
                "model_name": "logistic_regression",
                "labels": ["negative", "neutral", "positive"],
                "stop_words": [],
                "vocabulary": {"good": 0, "bad": 1},
                "idf": [1.0, 1.0],
                "coefficients": [[-2.0, 2.0], [0.0, 0.0], [2.0, -2.0]],
                "intercepts": [0.0, 0.0, 0.0],
            }
        ),
        encoding="utf-8",
    )
    client, _ = create_test_client(model_path)

    response = client.post("/predict", json={"text": "Good food"})

    assert response.status_code == 200
    assert response.json["sentiment"] == "positive"
    assert sum(response.json["probabilities"].values()) == pytest.approx(1.0)


def test_predict_reports_missing_model_and_rejects_empty_text():
    client, _ = create_test_client()

    assert client.post("/predict", json={"text": "Nice"}).status_code == 503
    assert client.post("/predict", json={"text": "  "}).status_code == 400
