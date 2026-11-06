from __future__ import annotations

import argparse
from dask.base import compute
import json
import logging
from pathlib import Path
from typing import Any

import dask.dataframe as dd
import numpy as np

LOGGER = logging.getLogger("yelp_dataset")
DATASET_HANDLE = "yelp-dataset/yelp-dataset"
REGION_NAME = "Philadelphia metropolitan study area"
REGION_CENTER = (39.9526, -75.1636)
REGION_RADIUS_KM = 50
REGION_STATES = ("DE", "NJ", "PA")
RESTAURANT_PATTERN = r"(?:^|,\s*)Restaurants(?:\s*,|$)"
DATASET_FILE_CANDIDATES = {
    "business.json": (
        "business.json",
        "yelp_academic_dataset_business.json",
    ),
    "review.json": (
        "review.json",
        "yelp_academic_dataset_review.json",
    ),
}
PREVIEW_FIELDS = {
    "business.json": (
        "business_id",
        "name",
        "city",
        "state",
        "latitude",
        "longitude",
        "stars",
        "review_count",
        "categories",
    ),
    "review.json": ("review_id", "business_id", "stars", "date"),
}


def select_target_businesses(frame: dd.DataFrame) -> dd.DataFrame:
    masks = _business_filter_masks(frame)
    return frame[masks["within_region"]]


def _business_filter_masks(frame: dd.DataFrame) -> dict[str, dd.Series]:
    required = {"business_id", "categories", "latitude", "longitude", "state"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Business data is missing columns: {', '.join(sorted(missing))}")

    restaurants = frame["categories"].fillna("").str.contains(
        RESTAURANT_PATTERN,
        regex=True,
        na=False,
    )
    valid_business_id = frame["business_id"].fillna("").str.strip().str.len() > 0
    in_region_states = frame["state"].fillna("").str.upper().isin(REGION_STATES)
    restaurants_in_states = restaurants & in_region_states & valid_business_id
    latitude = np.radians(frame["latitude"])
    longitude = np.radians(frame["longitude"])
    center_latitude, center_longitude = np.radians(REGION_CENTER)
    delta_latitude = latitude - center_latitude
    delta_longitude = longitude - center_longitude
    haversine = (
        np.sin(delta_latitude / 2) ** 2
        + np.cos(center_latitude)
        * np.cos(latitude)
        * np.sin(delta_longitude / 2) ** 2
    )
    distance_km = 2 * 6371 * np.arcsin(np.sqrt(haversine))
    valid_coordinates = (
        frame["latitude"].between(-90, 90)
        & frame["longitude"].between(-180, 180)
    )
    valid_restaurant_coordinates = restaurants_in_states & valid_coordinates
    within_region = valid_restaurant_coordinates & (distance_km <= REGION_RADIUS_KM)
    return {
        "restaurants": restaurants,
        "restaurants_in_states": restaurants_in_states,
        "valid_coordinates": valid_restaurant_coordinates,
        "within_region": within_region,
    }


def business_filter_counts(
    frame: dd.DataFrame, *, scheduler: Any = None
) -> dict[str, int]:
    masks = _business_filter_masks(frame)
    values = compute(
        frame["business_id"].count(),
        masks["restaurants"].sum(),
        masks["restaurants_in_states"].sum(),
        masks["valid_coordinates"].sum(),
        masks["within_region"].sum(),
        scheduler=scheduler,
    )
    return {
        "businesses_read": int(values[0]),
        "restaurant_category": int(values[1]),
        "restaurant_in_study_states_with_id": int(values[2]),
        "restaurant_in_states_with_valid_coordinates": int(values[3]),
        "restaurant_within_radius": int(values[4]),
    }


def _resolve_dataset_files(dataset_dir: Path) -> dict[str, Path]:
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    resolved = {
        logical_name: next(
            (
                dataset_dir / filename
                for filename in candidates
                if (dataset_dir / filename).is_file()
            ),
            None,
        )
        for logical_name, candidates in DATASET_FILE_CANDIDATES.items()
    }
    missing = [name for name, path in resolved.items() if path is None]
    if missing:
        raise FileNotFoundError(
            f"Missing required Yelp dataset files in {dataset_dir}: "
            f"{', '.join(missing)}"
        )
    return {name: path for name, path in resolved.items() if path is not None}


def validate_dataset_dir(dataset_dir: Path) -> Path:
    _resolve_dataset_files(dataset_dir)
    return dataset_dir


def download_yelp_dataset(*, force_download: bool = False) -> Path:
    try:
        import kagglehub
    except ImportError as error:
        raise RuntimeError(
            "KaggleHub is required; install ingestion/requirements.txt first."
        ) from error

    dataset_dir = Path(
        kagglehub.dataset_download(
            DATASET_HANDLE,
            force_download=force_download,
        )
    )
    return validate_dataset_dir(dataset_dir)


def read_yelp_data(
    dataset_dir: Path, *, blocksize: str = "16 MiB"
) -> dict[str, dd.DataFrame]:
    dataset_files = _resolve_dataset_files(dataset_dir)
    return {
        filename: dd.read_json(
            str(path),
            lines=True,
            blocksize=blocksize,
        )
        for filename, path in dataset_files.items()
    }


def inspect_yelp_data(
    dataset_dir: Path, *, rows: int = 3, blocksize: str = "16 MiB"
) -> dict[str, dict[str, Any]]:
    if rows <= 0:
        raise ValueError("rows must be greater than zero")

    frames = read_yelp_data(dataset_dir, blocksize=blocksize)
    dataset_files = _resolve_dataset_files(dataset_dir)
    inspection: dict[str, dict[str, Any]] = {}
    for filename, frame in frames.items():
        fields = [field for field in PREVIEW_FIELDS[filename] if field in frame.columns]
        preview = frame[fields].head(rows)
        inspection[filename] = {
            "source_file": dataset_files[filename].name,
            "bytes": dataset_files[filename].stat().st_size,
            "partitions": frame.npartitions,
            "columns": list(frame.columns),
            "preview": preview.to_dict(orient="records"),
        }
        if filename == "business.json":
            inspection[filename]["selection_counts"] = business_filter_counts(frame)
    return inspection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and inspect Yelp Open Dataset JSONL files with Dask."
    )
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        help="Read an existing KaggleHub dataset directory instead of downloading.",
    )
    parser.add_argument(
        "--force-download",
        action="store_true",
        help="Ask KaggleHub to download the dataset again.",
    )
    parser.add_argument("--rows", type=int, default=3)
    parser.add_argument("--blocksize", default="16 MiB")
    args = parser.parse_args()
    if args.rows <= 0:
        parser.error("--rows must be greater than zero")
    if args.force_download and args.dataset_dir:
        parser.error("--force-download cannot be used with --dataset-dir")
    return args


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    dataset_dir = (
        validate_dataset_dir(args.dataset_dir)
        if args.dataset_dir
        else download_yelp_dataset(force_download=args.force_download)
    )
    LOGGER.info("Dataset directory: %s", dataset_dir)
    print(
        json.dumps(
            inspect_yelp_data(
                dataset_dir,
                rows=args.rows,
                blocksize=args.blocksize,
            ),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
