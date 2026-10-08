import pandas as pd

from benchmark.dask_aggregate import _aggregate_partition


def test_dask_partition_extracts_valid_start_points_and_utc_hours():
    partition = pd.DataFrame(
        [
            ("1372636858", "False", "[[-8.61,41.14],[-8.62,41.15]]"),
            ("1372636858", "False", "[[-8.61,41.14]]"),
            ("1372636858", "True", "[[-8.61,41.14]]"),
            ("1372636858", "False", "[]"),
            ("1372636858", "False", "[[181,41.14]]"),
            ("not-a-timestamp", "False", "[[-8.61,41.14]]"),
        ],
        columns=["TIMESTAMP", "MISSING_DATA", "POLYLINE"],
    )

    result = _aggregate_partition(partition)

    assert result.to_records(index=False).tolist() == [
        (-861, 4114, 0),
        (-861, 4114, 0),
    ]
