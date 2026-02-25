from __future__ import annotations

from typing import Any


def bounded_int(
    value: Any,
    *,
    default: int,
    min_value: int,
    max_value: int,
) -> int:
    try:
        n = int(value)
    except Exception:
        n = int(default)
    if n < int(min_value):
        return int(min_value)
    if n > int(max_value):
        return int(max_value)
    return int(n)


def bounded_limit(value: Any, *, default: int, max_limit: int, min_limit: int = 1) -> int:
    return bounded_int(value, default=default, min_value=min_limit, max_value=max_limit)


def bounded_offset(value: Any, *, default: int = 0, max_offset: int = 10000) -> int:
    return bounded_int(value, default=default, min_value=0, max_value=max_offset)


def bounded_pagination(
    *,
    limit: Any,
    offset: Any,
    default_limit: int,
    max_limit: int,
    default_offset: int = 0,
    max_offset: int = 10000,
) -> tuple[int, int]:
    return (
        bounded_limit(limit, default=default_limit, max_limit=max_limit),
        bounded_offset(offset, default=default_offset, max_offset=max_offset),
    )


def bounded_scan_limit(
    *,
    limit: int,
    offset: int,
    floor: int = 2000,
    headroom: int = 200,
    max_scan: int = 6200,
) -> int:
    target = int(limit) + int(offset) + int(headroom)
    if target < int(floor):
        target = int(floor)
    if target > int(max_scan):
        target = int(max_scan)
    return int(target)
