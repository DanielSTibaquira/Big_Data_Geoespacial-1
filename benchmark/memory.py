from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

CGROUP_MEMORY_PEAK_FILES = (
    Path("/sys/fs/cgroup/memory.peak"),
    Path("/sys/fs/cgroup/memory/memory.max_usage_in_bytes"),
)


def peak_memory_bytes(
    paths: Sequence[Path] = CGROUP_MEMORY_PEAK_FILES,
) -> int:
    for path in paths:
        try:
            value = path.read_text(encoding="ascii").strip()
        except OSError:
            continue
        try:
            peak = int(value)
        except ValueError as error:
            raise ValueError(
                f"Invalid cgroup memory peak value in {path}: {value!r}"
            ) from error
        if peak < 0:
            raise ValueError(f"Invalid cgroup memory peak value in {path}: {peak}")
        return peak

    searched_paths = ", ".join(str(path) for path in paths)
    raise RuntimeError(
        "Container memory peak is unavailable; expected a cgroup v2 or v1 "
        f"memory file at one of: {searched_paths}"
    )
