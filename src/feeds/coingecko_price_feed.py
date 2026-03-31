from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable

import requests
from requests import HTTPError

from src.feeds.price_cache import PriceCache
from src.shared import PriceResolution, normalize_price_timestamp


class CoinGeckoPriceFeed:
    """CoinGecko historical EUR price lookup with optional disk persistence."""

    def __init__(
        self,
        cache_path: str | Path = ".cache/coingecko_prices.json",
        base_url: str = "https://api.coingecko.com/api/v3",
        api_key: str | None = None,
        resolution: str = PriceResolution.DAY,
        coin_ids: dict[str, str] | None = None,
        price_cache: PriceCache | None = None,
        session: requests.Session | None = None,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.resolution = resolution
        self.coin_ids = {asset.upper(): coin_id for asset, coin_id in (coin_ids or {}).items()}
        self.price_cache = price_cache or PriceCache(
            persist_dir=cache_path,
            persist_providers={"coingecko"},
        )
        self.session = session or requests.Session()
        self.event_logger = event_logger
        self._warnings: list[str] = []

    def get_warnings(self) -> list[str]:
        return list(self._warnings)

    def get_historical_price_eur(self, coin_id: str, at: datetime) -> Decimal:
        prices = self.get_historical_prices(coin_id=coin_id, at=at)
        try:
            return prices["eur"]
        except KeyError as exc:
            raise ValueError(f"EUR price not available for {coin_id} at {at.date()}") from exc

    def get_historical_prices(self, coin_id: str, at: datetime) -> dict[str, Decimal]:
        resolved_coin_id = self._resolve_coin_id(coin_id)
        normalized_at = at.astimezone(timezone.utc)
        bucketed_at = normalize_price_timestamp(normalized_at, self.resolution)
        cached_value = self.price_cache.get(
            provider="coingecko",
            asset_id=resolved_coin_id,
            requested_at=normalized_at,
            resolution=self.resolution,
        )
        if cached_value is not None:
            return cached_value

        if not self.api_key:
            message = (
                f"CoinGecko fallback could not be used for {resolved_coin_id} at {normalized_at.isoformat()} "
                "because no API key was provided. Set pricing.coingecko.api_key in config or pass --coingecko-api-key."
            )
            self._warnings.append(message)
            raise ValueError(message)

        url = f"{self.base_url}/coins/{resolved_coin_id}/history"
        response = self.session.get(
            url,
            params={
                "date": bucketed_at.strftime("%d-%m-%Y"),
                "localization": "false",
            },
            headers=self._build_headers(),
            timeout=30,
        )
        try:
            response.raise_for_status()
        except HTTPError as exc:
            message = (
                f"CoinGecko could not resolve historical price data for {resolved_coin_id} "
                f"at {normalized_at.isoformat()}. Verify the configured coin id and that "
                "the asset exists on CoinGecko for that date."
            )
            self._warnings.append(message)
            raise ValueError(message) from exc
        payload = response.json()

        try:
            current_prices = payload["market_data"]["current_price"]
        except KeyError as exc:
            message = f"CoinGecko did not return historical price data for {resolved_coin_id} at {normalized_at.isoformat()}."
            self._warnings.append(message)
            raise ValueError(message) from exc

        prices = {
            currency: Decimal(str(value))
            for currency, value in current_prices.items()
        }
        self.price_cache.set(
            provider="coingecko",
            asset_id=resolved_coin_id,
            requested_at=normalized_at,
            resolution=self.resolution,
            prices=prices,
        )
        return prices

    def convert_asset_amount_to_eur(self, amount: Decimal, coin_id: str, at: datetime) -> Decimal:
        return amount * self.get_historical_price_eur(coin_id=coin_id, at=at)

    def convert_fiat_amount_to_eur(
        self,
        amount: Decimal,
        currency: str,
        at: datetime,
    ) -> Decimal:
        normalized_currency = currency.strip().upper()
        if normalized_currency == "EUR":
            return amount
        raise ValueError(
            f"CoinGecko fiat conversion for {normalized_currency} is not supported by this tool."
        )

    def _build_headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        if "pro-api.coingecko.com" in self.base_url:
            return {"x-cg-pro-api-key": self.api_key}
        return {"x-cg-demo-api-key": self.api_key}

    def _resolve_coin_id(self, coin_id: str) -> str:
        raw_coin_id = coin_id.strip()
        normalized_asset = raw_coin_id.upper()
        if normalized_asset in self.coin_ids:
            return self.coin_ids[normalized_asset]
        # The application passes asset tickers such as BTC or TRUMP into the
        # shared price feed. For CoinGecko we require an explicit mapping for
        # those tickers instead of guessing that the ticker is a valid coin id.
        if raw_coin_id == normalized_asset:
            message = (
                f"No CoinGecko coin id configured for asset {normalized_asset}. "
                "Add it under pricing.coingecko.coin_ids in config or avoid using "
                "CoinGecko fallback for this asset."
            )
            self._warnings.append(message)
            raise ValueError(message)
        return raw_coin_id

    def _log(self, message: str) -> None:
        if self.event_logger is not None:
            self.event_logger(message)
