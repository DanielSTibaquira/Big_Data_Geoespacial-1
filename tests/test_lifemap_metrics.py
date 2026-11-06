import pytest

from spark.jobs.lifemap_metrics import annualized_closure_rate, encode_geohash


def test_encode_geohash_has_expected_precision_and_location():
    assert encode_geohash(39.9526, -75.1636, precision=5) == "dr4e3"


def test_encode_geohash_rejects_invalid_coordinates_and_precision():
    with pytest.raises(ValueError, match="WGS84"):
        encode_geohash(91, -75)
    with pytest.raises(ValueError, match="positive"):
        encode_geohash(39, -75, precision=0)


def test_annualized_closure_rate_uses_business_year_exposure():
    assert annualized_closure_rate(1, 365) == pytest.approx(100.0)
    assert annualized_closure_rate(2, 730) == pytest.approx(100.0)
    assert annualized_closure_rate(1, 0) is None


def test_annualized_closure_rate_rejects_negative_inputs():
    with pytest.raises(ValueError, match="non-negative"):
        annualized_closure_rate(-1, 365)
