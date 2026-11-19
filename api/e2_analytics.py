from __future__ import annotations

from datetime import datetime
from typing import Any


def kaplan_meier(
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    usable: list[tuple[float, float, bool]] = []
    for observation in observations:
        entry = float(observation["entry_time_days"])
        exit_time = float(observation["exit_time_days"])
        if entry < 0 or exit_time < entry:
            raise ValueError("Survival exit time must be after its delayed entry")
        usable.append((entry, exit_time, bool(observation["event"])))
    if not usable:
        return {"businesses": 0, "points": [], "median_days": None}

    survival = 1.0
    points = []
    event_times = sorted(
        {exit_time for _, exit_time, event in usable if event}
    )
    median_days = None
    for event_time in event_times:
        at_risk = sum(
            1 for entry, exit_time, _ in usable
            if entry <= event_time <= exit_time
        )
        events = sum(
            1 for entry, exit_time, event in usable
            if event and entry <= event_time and exit_time == event_time
        )
        if at_risk == 0:
            continue
        survival *= 1.0 - events / at_risk
        points.append(
            {
                "time_days": event_time,
                "at_risk": at_risk,
                "events": events,
                "survival_probability": survival,
            }
        )
        if median_days is None and survival <= 0.5:
            median_days = event_time
    return {
        "businesses": len(usable),
        "events": sum(int(event) for _, _, event in usable),
        "censored": sum(int(not event) for _, _, event in usable),
        "points": points,
        "median_days": median_days,
    }


def survival_observation(
    first_review: datetime,
    last_review: datetime,
    origin: datetime,
    *,
    event: bool,
) -> dict[str, Any]:
    entry = (first_review - origin).total_seconds() / 86400
    exit_time = (last_review - origin).total_seconds() / 86400
    return {
        "entry_time_days": max(0.0, entry),
        "exit_time_days": max(0.0, exit_time),
        "event": event,
    }
