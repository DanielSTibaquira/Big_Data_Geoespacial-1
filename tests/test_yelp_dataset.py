import json

import dask.dataframe as dd
import pandas as pd
import pytest
from dask.distributed import Client

from ingestion.yelp_dataset import (
    inspect_yelp_data,
    select_target_businesses,
    validate_dataset_dir,
)
from ingestion.yelp_ingest import (
    _read_limited_reviews,
    build_business_document,
    build_review_documents,
)


def test_validate_dataset_dir_requires_both_jsonl_files(tmp_path):
    (tmp_path / "yelp_academic_dataset_business.json").write_text(
        json.dumps({"business_id": "b1"}) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError, match="review.json"):
        validate_dataset_dir(tmp_path)


def test_inspect_yelp_data_reads_partitioned_json_lines(tmp_path):
    businesses = [
        {
            "business_id": f"b{index}",
            "name": f"Business {index}",
            "city": "Philadelphia",
            "state": "PA",
            "latitude": 39.95,
            "longitude": -75.16,
            "categories": "Restaurants",
        }
        for index in range(200)
    ]
    reviews = [
        {
            "review_id": f"r{index}",
            "business_id": f"b{index}",
            "stars": 5,
            "date": "2025-01-01",
            "text": "Great food",
        }
        for index in range(200)
    ]
    for filename, records in (
        ("yelp_academic_dataset_business.json", businesses),
        ("yelp_academic_dataset_review.json", reviews),
    ):
        (tmp_path / filename).write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    inspection = inspect_yelp_data(tmp_path, rows=2, blocksize="1 KiB")

    assert inspection["business.json"]["partitions"] > 1
    assert inspection["review.json"]["partitions"] > 1
    assert len(inspection["business.json"]["preview"]) == 2
    assert inspection["business.json"]["preview"][0]["city"] == "Philadelphia"
    assert "text" not in inspection["review.json"]["preview"][0]


def test_select_target_businesses_uses_region_and_restaurant_family():
    frame = dd.from_pandas(
        pd.DataFrame(
            [
                {
                    "business_id": "center",
                    "categories": "Restaurants, Italian",
                    "state": "PA",
                    "latitude": 39.9526,
                    "longitude": -75.1636,
                },
                {
                    "business_id": "nearby",
                    "categories": "Restaurants, Mexican",
                    "state": "NJ",
                    "latitude": 39.934,
                    "longitude": -75.03,
                },
                {
                    "business_id": "far",
                    "categories": "Restaurants",
                    "state": "PA",
                    "latitude": 39.2904,
                    "longitude": -76.6122,
                },
                {
                    "business_id": "not-restaurant",
                    "categories": "Bars, Pubs",
                    "state": "PA",
                    "latitude": 39.9526,
                    "longitude": -75.1636,
                },
                {
                    "business_id": "wrong-state",
                    "categories": "Restaurants",
                    "state": "NV",
                    "latitude": 39.9803,
                    "longitude": -74.859,
                },
            ]
        ),
        npartitions=2,
    )

    selected = select_target_businesses(frame)["business_id"].compute().tolist()

    assert set(selected) == {"center", "nearby"}


def test_build_review_documents_labels_sentiment_and_tracks_rejections():
    partition = pd.DataFrame(
        [
            {
                "review_id": "positive",
                "business_id": "b1",
                "stars": 5,
                "text": "  Excellent food  ",
                "date": "2025-01-02 12:00:00",
            },
            {
                "review_id": "positive",
                "business_id": "b1",
                "stars": 5,
                "text": "Duplicate review",
                "date": "2025-01-02 12:00:00",
            },
            {
                "review_id": "neutral",
                "business_id": "b1",
                "stars": 3,
                "text": "Average",
                "date": "2025-01-01",
            },
            {
                "review_id": "negative",
                "business_id": "b1",
                "stars": 1,
                "text": "Poor",
                "date": "2025-01-03",
            },
            {
                "review_id": "outside",
                "business_id": "b2",
                "stars": 5,
                "text": "Outside region",
                "date": "2025-01-01",
            },
            {
                "review_id": "empty",
                "business_id": "b1",
                "stars": 5,
                "text": "   ",
                "date": "2025-01-01",
            },
            {
                "review_id": "invalid-stars",
                "business_id": "b1",
                "stars": 6,
                "text": "Invalid rating",
                "date": "2025-01-01",
            },
            {
                "review_id": "invalid-date",
                "business_id": "b1",
                "stars": 4,
                "text": "Invalid date",
                "date": "not-a-date",
            },
        ]
    )

    documents, counters = build_review_documents(partition, {"b1"})

    assert [document["sentiment"] for document in documents] == [
        "positive",
        "neutral",
        "negative",
    ]
    assert documents[0]["text"] == "Excellent food"
    assert documents[0]["date"].tzinfo is not None
    assert counters == {
        "rows_read": 8,
        "valid": 3,
        "duplicate_reviews": 1,
        "outside_target_businesses": 1,
        "missing_text": 1,
        "invalid_stars": 1,
        "invalid_date": 1,
    }


def test_build_business_document_has_geojson_and_business_identity():
    document = build_business_document(
        {
            "business_id": "b1",
            "name": "Restaurant",
            "city": "Philadelphia",
            "state": "PA",
            "latitude": 39.95,
            "longitude": -75.16,
            "stars": 4.5,
            "review_count": 20,
            "is_open": 1,
            "categories": "Restaurants, Italian",
        }
    )

    assert document["_id"] == "b1"
    assert document["location"] == {
        "type": "Point",
        "coordinates": [-75.16, 39.95],
    }
    assert document["categories"] == ["Restaurants", "Italian"]
    assert document["restaurant_family"] == "Restaurants"


def test_read_limited_reviews_uses_requested_dask_client():
    frame = dd.from_pandas(
        pd.DataFrame(
            {
                "review_id": [f"r{index}" for index in range(10)],
                "business_id": [f"b{index}" for index in range(10)],
                "stars": [5] * 10,
                "text": ["Great"] * 10,
                "date": ["2025-01-01"] * 10,
            }
        ),
        npartitions=5,
    )

    with Client(processes=False, n_workers=1, threads_per_worker=1) as client:
        sample = _read_limited_reviews(frame, 4, client).compute()

    assert sample["review_id"].tolist() == ["r0", "r1", "r2", "r3"]
