"""Shared structural interfaces used across functional packages."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from src.model import Transaction


class PriceFeed(Protocol):
    """Historical pricing source used by parsers and orchestration."""

    def get_historical_price_eur(self, coin_id: str, at: datetime) -> Decimal:
        ...

    def convert_asset_amount_to_eur(
        self,
        amount: Decimal,
        coin_id: str,
        at: datetime,
    ) -> Decimal:
        ...

    def convert_fiat_amount_to_eur(
        self,
        amount: Decimal,
        currency: str,
        at: datetime,
    ) -> Decimal:
        ...


class CsvParser(Protocol):
    """CSV adapter interface implemented by exchange and wallet parsers."""

    source: str

    def parse(
        self,
        path: str | Path,
        *,
        price_feed: PriceFeed | None = None,
        coin_ids: dict[str, str] | None = None,
        ignored_assets: set[str] | None = None,
    ) -> list[Transaction]:
        ...
