from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from src.model import Transaction
from src.shared import PriceResolution, normalize_price_timestamp


def build_low_resolution_warnings(
    *,
    transactions: list[Transaction],
    resolution: str,
    enabled: bool,
) -> list[str]:
    if not enabled or resolution != PriceResolution.DAY:
        return []

    counts_by_asset_day: dict[tuple[str, datetime], int] = defaultdict(int)
    for transaction in transactions:
        bucketed_at = normalize_price_timestamp(transaction.timestamp, PriceResolution.DAY)
        counts_by_asset_day[(transaction.asset, bucketed_at)] += 1

    affected_assets = sorted(
        {
            asset
            for (asset, _), count in counts_by_asset_day.items()
            if count > 1
        }
    )
    if not affected_assets:
        return []

    assets_preview = ", ".join(affected_assets[:5])
    if len(affected_assets) > 5:
        assets_preview += ", ..."

    return [
        "Daily price resolution is active and repeated same-day activity was detected "
        f"for asset(s): {assets_preview}. Daily bucketing reduces cache misses but may "
        "materially distort gain/loss calculations on volatile assets with intraday trades."
    ]
