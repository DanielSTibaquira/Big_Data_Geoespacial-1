from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any


def load_serving_model(path: Path) -> dict[str, Any] | None:
    try:
        with path.open(encoding="utf-8") as model_file:
            model = json.load(model_file)
    except FileNotFoundError:
        return None
    if not isinstance(model, dict) or model.get("format") != "lifemap-logistic-v1":
        raise ValueError(f"Unsupported sentiment serving model: {path}")
    return model


def predict_sentiment(text: str, model: dict[str, Any]) -> dict[str, Any]:
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    stop_words = set(model["stop_words"])
    vocabulary = model["vocabulary"]
    counts: dict[int, float] = {}
    for token in tokens:
        if token in stop_words:
            continue
        index = vocabulary.get(token)
        if index is not None:
            counts[index] = counts.get(index, 0.0) + 1.0

    feature_values = {
        index: count * float(model["idf"][index])
        for index, count in counts.items()
    }
    scores = [
        float(intercept)
        + sum(
            feature_values.get(index, 0.0) * float(weight)
            for index, weight in enumerate(coefficients)
            if index in feature_values
        )
        for intercept, coefficients in zip(
            model["intercepts"], model["coefficients"], strict=True
        )
    ]
    maximum = max(scores)
    exponentials = [math.exp(score - maximum) for score in scores]
    denominator = sum(exponentials)
    probabilities = {
        label: probability / denominator
        for label, probability in zip(model["labels"], exponentials, strict=True)
    }
    prediction = max(probabilities, key=probabilities.__getitem__)
    return {
        "sentiment": prediction,
        "probabilities": probabilities,
        "model": model.get("model_name", "logistic_regression"),
    }
