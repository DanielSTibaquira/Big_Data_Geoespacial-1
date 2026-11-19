import argparse
from datetime import datetime, timezone

import pytest

from spark.jobs.lifemap_batch import _parse_utc_cutoff


def test_parse_utc_cutoff_normalizes_zulu_timestamp():
    assert _parse_utc_cutoff("2021-01-01T00:00:00Z") == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )


def test_parse_utc_cutoff_converts_offset_to_utc():
    assert _parse_utc_cutoff("2021-01-01T01:00:00+01:00") == datetime(
        2021, 1, 1, tzinfo=timezone.utc
    )


@pytest.mark.parametrize(
    "value",
    ["2021-01-01T00:00:00", "not-a-timestamp"],
)
def test_parse_utc_cutoff_rejects_invalid_or_timezone_naive_values(value):
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_utc_cutoff(value)
