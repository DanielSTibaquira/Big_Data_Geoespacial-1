from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pytest

from ingestion.replay_reviews import (
    make_review_message,
    parse_utc_cutoff,
    replay_reviews,
)


class Cursor(list):
    def sort(self, specification):
        self.sort_specification = specification
        return self

    def limit(self, count):
        del self[count:]
        return self


class Collection:
    def __init__(self, documents):
        self.documents = documents
        self.query = None
        self.indexes = []

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))

    def find(self, query, projection=None):
        self.query = query
        return Cursor(self.documents.copy())


class Delivery:
    def get(self, timeout):
        assert timeout == 30


class FakeProducer:
    def __init__(self):
        self.records = []
        self.flushed = False

    def send(self, topic, *, key, value):
        self.records.append((topic, key, value))
        return Delivery()

    def flush(self):
        self.flushed = True


def test_cutoff_requires_timezone_and_normalizes_to_utc():
    assert parse_utc_cutoff("2021-01-01T01:00:00+01:00") == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )
    with pytest.raises(argparse.ArgumentTypeError, match="timezone"):
        parse_utc_cutoff("2021-01-01T00:00:00")


def test_replay_orders_post_cutoff_reviews_and_uses_business_key():
    cutoff = datetime(2021, 1, 1, tzinfo=timezone.utc)
    review_date = datetime(2021, 2, 1, tzinfo=timezone.utc)
    review_docs = [
        {
            "_id": "r-1",
            "review_id": "r-1",
            "business_id": "b-1",
            "text": "Great food",
            "stars": 5,
            "sentiment": "positive",
            "date": review_date,
        }
    ]
    review_collection = Collection(review_docs)
    business_collection = Collection(
        [
            {
                "_id": "b-1",
                "location": {
                    "type": "Point",
                    "coordinates": [-75.16, 39.95],
                },
                "primary_category": "Italian",
            }
        ]
    )
    producer = FakeProducer()
    sent = replay_reviews(
        review_collection,
        business_collection,
        producer,
        cutoff=cutoff,
        rate=5,
        clock=lambda: 0.0,
        sleep=lambda delay: None,
    )

    assert sent == 1
    assert producer.flushed
    assert review_collection.query == {"date": {"$gt": cutoff}}
    assert review_collection.indexes[0][0] == [("date", 1), ("_id", 1)]
    topic, key, message = producer.records[0]
    assert topic == "reviews"
    assert key == b"b-1"
    assert message["date"] == "2021-02-01T00:00:00Z"
    assert message["sentiment"] == "positive"
    assert message["location"]["coordinates"] == [-75.16, 39.95]


def test_message_rejects_missing_business_location():
    with pytest.raises(ValueError, match="GeoJSON location"):
        make_review_message(
            {
                "_id": "r-1",
                "business_id": "b-1",
                "date": datetime(2021, 2, 1, tzinfo=timezone.utc),
            },
            {},
        )
