"""Shared pricing primitives used by price feeds and orchestration."""

from __future__ import annotations

from datetime import datetime, timezone


class PriceResolution(str):
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


def normalize_price_timestamp(at: datetime, resolution: str) -> datetime:
    normalized_at = at.astimezone(timezone.utc)
    if resolution == PriceResolution.MINUTE:
        return normalized_at.replace(second=0, microsecond=0)
    if resolution == PriceResolution.HOUR:
        return normalized_at.replace(minute=0, second=0, microsecond=0)
    return normalized_at.replace(hour=0, minute=0, second=0, microsecond=0)
