from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from api.e2_analytics import kaplan_meier
from api.sentiment import predict_sentiment
from spark.jobs.lifemap_metrics import encode_geohash, geohash_center
from spark.jobs.lifemap_e2_analytics import _competitor_counts
from spark.jobs.lifemap_streaming import _processed_review, _risk_probability


def test_kaplan_meier_accounts_for_delayed_entry_and_censoring():
    result = kaplan_meier(
        [
            {"entry_time_days": 0, "exit_time_days": 5, "event": True},
            {"entry_time_days": 4, "exit_time_days": 8, "event": False},
            {"entry_time_days": 6, "exit_time_days": 10, "event": True},
        ]
    )

    assert result["businesses"] == 3
    assert result["events"] == 2
    assert result["censored"] == 1
    assert result["points"][0]["at_risk"] == 2
    assert result["points"][0]["survival_probability"] == pytest.approx(0.5)
    assert result["median_days"] == 5


def test_kaplan_meier_rejects_exit_before_delayed_entry():
    with pytest.raises(ValueError, match="after its delayed entry"):
        kaplan_meier(
            [{"entry_time_days": 4, "exit_time_days": 3, "event": True}]
        )


def test_geohash_center_round_trips_expected_philadelphia_cell():
    cell = encode_geohash(39.9526, -75.1636, precision=5)

    center = geohash_center(cell)

    assert abs(center["latitude"] - 39.9526) < 0.03
    assert abs(center["longitude"] + 75.1636) < 0.03


def test_streamed_review_preserves_bson_ready_date_and_geojson():
    class SparkRow(dict):
        def asDict(self, recursive=False):
            assert recursive
            return dict(self)

    document = _processed_review(
        {
            "review_id": "r-1",
            "business_id": "b-1",
            "text": "Great",
            "stars": 5,
            "date": "2021-02-01T00:00:00Z",
            "sentiment": "positive",
            "location": SparkRow(
                type="Point",
                coordinates=[-75.16, 39.95],
            ),
            "primary_category": "Italian",
        },
        {"sentiment": "positive", "probabilities": {"positive": 0.9}},
        processed_at=datetime.now(timezone.utc),
        batch_id=7,
    )

    assert document["date"].tzinfo is not None
    assert document["location"] == {
        "type": "Point",
        "coordinates": [-75.16, 39.95],
    }
    assert document["predicted_sentiment"] == "positive"
    assert document["stream_batch_id"] == 7


def test_risk_probability_uses_exported_feature_order():
    probability = _risk_probability(
        {"x": 2.0},
        {
            "feature_names": ["x"],
            "coefficients": [1.0],
            "intercept": 0.0,
        },
    )

    assert probability == pytest.approx(1 / (1 + math.exp(-2)))


def test_sentiment_inference_rejects_mismatched_model_shapes():
    with pytest.raises(ValueError, match="inconsistent class coefficients"):
        predict_sentiment(
            "review",
            {
                "stop_words": [],
                "vocabulary": {},
                "idf": [],
                "intercepts": [0.0],
                "coefficients": [],
                "labels": ["positive"],
            },
        )


def test_competitor_feature_uses_geo_near_with_category_and_500m():
    class GeoCollection:
        def __init__(self):
            self.pipelines = []
            self.indexes = []

        def create_index(self, keys, **kwargs):
            self.indexes.append((keys, kwargs))

        def aggregate(self, pipeline):
            self.pipelines.append(pipeline)
            return [{"competitors": 3}]

    collection = GeoCollection()
    businesses = [
        {
            "business_id": "b-1",
            "primary_category": "Italian",
            "location": {
                "type": "Point",
                "coordinates": [-75.16, 39.95],
            },
        }
    ]

    counts = _competitor_counts(businesses, collection)

    assert counts == {"b-1": 3}
    geo_near = collection.pipelines[0][0]["$geoNear"]
    assert geo_near["maxDistance"] == 500
    assert geo_near["query"] == {
        "primary_category": "Italian",
        "business_id": {"$ne": "b-1"},
    }
