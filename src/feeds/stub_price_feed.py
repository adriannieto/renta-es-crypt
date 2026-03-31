from __future__ import annotations

from datetime import datetime
from decimal import Decimal


class StubPriceFeed:
    """Deterministic in-memory price feed useful for tests."""

    def __init__(
        self,
        prices_by_coin_id: dict[str, Decimal] | None = None,
        fiat_rates_to_eur: dict[str, Decimal] | None = None,
    ) -> None:
        self.prices_by_coin_id = prices_by_coin_id or {}
        self.fiat_rates_to_eur = {
            currency.upper(): value
            for currency, value in (fiat_rates_to_eur or {}).items()
        }

    def get_historical_price_eur(self, coin_id: str, at: datetime) -> Decimal:
        try:
            return self.prices_by_coin_id[coin_id]
        except KeyError as exc:
            raise ValueError(f"Stub price missing for coin id {coin_id} at {at}") from exc

    def convert_asset_amount_to_eur(self, amount: Decimal, coin_id: str, at: datetime) -> Decimal:
        return amount * self.get_historical_price_eur(coin_id, at)

    def convert_fiat_amount_to_eur(self, amount: Decimal, currency: str, at: datetime) -> Decimal:
        normalized_currency = currency.strip().upper()
        if normalized_currency == "EUR":
            return amount
        try:
            return amount * self.fiat_rates_to_eur[normalized_currency]
        except KeyError as exc:
            raise ValueError(f"Stub FX rate missing for {normalized_currency} at {at}") from exc
