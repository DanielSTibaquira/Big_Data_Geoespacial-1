from datetime import datetime, timezone

import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client

from ingestion.ingest import (
    _read_limited_sample,
    build_document,
    transform_partition,
)


def valid_row(**overrides):
    row = {
        "TRIP_ID": "trip-1",
        "CALL_TYPE": "A",
        "ORIGIN_CALL": "123",
        "ORIGIN_STAND": "",
        "TAXI_ID": "1001",
        "TIMESTAMP": "1372636858",
        "DAY_TYPE": "A",
        "MISSING_DATA": "False",
        "POLYLINE": "[[-8.61,41.14],[-8.62,41.15]]",
    }
    row.update(overrides)
    return row


def test_build_document_uses_geojson_coordinate_order_and_utc():
    document, reason = build_document(valid_row())

    assert reason is None
    assert document is not None
    assert len(document["_id"]) == 64
    assert document["trip_id"] == "trip-1"
    assert document["location"] == {
        "type": "Point",
        "coordinates": [-8.61, 41.14],
    }
    assert document["destination"]["coordinates"] == [-8.62, 41.15]
    assert document["trip_duration_seconds"] == 15
    assert document["started_at"] == datetime.fromtimestamp(
        1372636858, tz=timezone.utc
    )


def test_build_document_rejects_missing_data():
    document, reason = build_document(valid_row(MISSING_DATA="True"))

    assert document is None
    assert reason == "missing_data"


def test_build_document_rejects_empty_polyline():
    document, reason = build_document(valid_row(POLYLINE="[]"))

    assert document is None
    assert reason == "empty_polyline"


def test_build_document_rejects_out_of_range_coordinate():
    document, reason = build_document(
        valid_row(POLYLINE="[[-8.61,41.14],[181,41.15]]")
    )

    assert document is None
    assert reason == "invalid_coordinates"


def test_build_document_rejects_invalid_json():
    document, reason = build_document(valid_row(POLYLINE="not-json"))

    assert document is None
    assert reason == "invalid_polyline"


def test_distinct_records_with_the_same_trip_id_keep_distinct_ids():
    first, first_reason = build_document(valid_row(CALL_TYPE="B"))
    second, second_reason = build_document(
        valid_row(
            CALL_TYPE="C",
            POLYLINE="[[-8.62,41.15],[-8.63,41.16]]",
        )
    )

    assert first_reason is None
    assert second_reason is None
    assert first is not None and second is not None
    assert first["trip_id"] == second["trip_id"]
    assert first["_id"] != second["_id"]


def test_identical_records_get_the_same_id_for_idempotent_upserts():
    first, first_reason = build_document(valid_row())
    second, second_reason = build_document(valid_row())

    assert first_reason is None
    assert second_reason is None
    assert first is not None and second is not None
    assert first["_id"] == second["_id"]


def test_transform_partition_reports_valid_and_rejected_rows():
    partition = pd.DataFrame(
        [
            valid_row(),
            valid_row(TRIP_ID="trip-2", MISSING_DATA="True"),
            valid_row(TRIP_ID="trip-3", POLYLINE="[]"),
        ]
    )

    documents, counters = transform_partition(partition)

    assert [document["trip_id"] for document in documents] == ["trip-1"]
    assert counters == {
        "rows_read": 3,
        "valid": 1,
        "missing_data": 1,
        "empty_polyline": 1,
    }


def test_limited_sample_returns_only_first_requested_rows():
    frame = pd.DataFrame({"value": range(10)})
    dataframe = dd.from_pandas(frame, npartitions=5)

    with Client(processes=False, n_workers=2, threads_per_worker=1) as client:
        sample = _read_limited_sample(dataframe, 4, client)

    assert sample["value"].tolist() == [0, 1, 2, 3]
