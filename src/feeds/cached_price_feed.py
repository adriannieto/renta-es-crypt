from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from typing import Callable

from src.feeds.cryptodatadownload_backfill import CryptoDataDownloadBackfill
from src.feeds.price_cache import PriceCache
from src.shared import normalize_price_timestamp


VALUE_PROVIDER = "cache"


class CachedPriceFeed:
    """
    Shared cache-facing price feed used by the application.

    Resolution order:
    1. final EUR value already stored in the shared cache
    2. local pair resolution through the configured backfill provider
    3. external provider as last resort
    """

    def __init__(
        self,
        *,
        cache_path: str | Path = ".cache/price_cache.json",
        resolution: str,
        backfill_provider: CryptoDataDownloadBackfill | None,
        external_provider,
        price_cache: PriceCache | None = None,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        self.resolution = resolution
        self.price_cache = price_cache or PriceCache(cache_path)
        self.backfill_provider = backfill_provider
        self.external_provider = external_provider
        self.event_logger = event_logger

    def get_warnings(self) -> list[str]:
        warnings: list[str] = []
        for provider in (self.backfill_provider, self.external_provider):
            if provider is None:
                continue
            get_warnings = getattr(provider, "get_warnings", None)
            if callable(get_warnings):
                warnings.extend(get_warnings())
        return warnings

    def preheat_requests(self, requests: list[tuple[str, datetime]]) -> list[dict[str, Any]]:
        summary_records: list[dict[str, Any]] = []
        seen: set[tuple[str, datetime]] = set()
        for coin_id, at in sorted(requests, key=lambda item: (item[0], item[1])):
            normalized_coin_id = coin_id.strip().upper()
            normalized_at = normalize_price_timestamp(at.astimezone(timezone.utc), self.resolution)
            request_key = (normalized_coin_id, normalized_at)
            if request_key in seen:
                continue
            seen.add(request_key)
            self._preheat_request(normalized_coin_id, normalized_at, summary_records)
        return summary_records

    def get_historical_price_eur(self, coin_id: str, at: datetime) -> Decimal:
        normalized_at = at.astimezone(timezone.utc)
        normalized_coin_id = coin_id.strip().upper()
        cached = self.price_cache.get(
            provider=VALUE_PROVIDER,
            asset_id=normalized_coin_id,
            requested_at=normalized_at,
            resolution=self.resolution,
        )
        if cached is not None and "eur" in cached:
            return cached["eur"]

        eur_price = self._resolve_uncached_historical_price_eur(
            normalized_coin_id,
            normalized_at,
        )

        self.price_cache.set(
            provider=VALUE_PROVIDER,
            asset_id=normalized_coin_id,
            requested_at=normalized_at,
            resolution=self.resolution,
            prices={"eur": eur_price},
        )
        return eur_price

    def convert_asset_amount_to_eur(self, amount: Decimal, coin_id: str, at: datetime) -> Decimal:
        return amount * self.get_historical_price_eur(coin_id=coin_id, at=at)

    def convert_fiat_amount_to_eur(self, amount: Decimal, currency: str, at: datetime) -> Decimal:
        normalized_currency = currency.strip().upper()
        if normalized_currency == "EUR":
            return amount
        return self.backfill_provider.convert_fiat_amount_to_eur(amount, normalized_currency, at)

    def _log(self, message: str) -> None:
        if self.event_logger is not None:
            self.event_logger(message)

    def _preheat_request(
        self,
        normalized_coin_id: str,
        normalized_at: datetime,
        summary_records: list[dict[str, Any]],
    ) -> None:
        cached = self.price_cache.get(
            provider=VALUE_PROVIDER,
            asset_id=normalized_coin_id,
            requested_at=normalized_at,
            resolution=self.resolution,
        )
        if cached is not None and "eur" in cached:
            return

        backfill_price = None
        backfill_resolution = None
        if self.backfill_provider is not None:
            backfill_price, backfill_resolution = self.backfill_provider.resolve_historical_price_eur_with_metadata(
                normalized_coin_id,
                normalized_at,
            )

        eur_price = backfill_price
        if eur_price is None:
            return

        summary_records.append(
            {
                "asset": normalized_coin_id,
                "requested_at": normalized_at,
                "requested_resolution": self.resolution,
                "used_resolution": backfill_resolution or self.resolution,
            }
        )

        self.price_cache.set(
            provider=VALUE_PROVIDER,
            asset_id=normalized_coin_id,
            requested_at=normalized_at,
            resolution=self.resolution,
            prices={"eur": eur_price},
        )

    def _resolve_uncached_historical_price_eur(
        self,
        normalized_coin_id: str,
        normalized_at: datetime,
    ) -> Decimal:
        eur_price = None
        if self.backfill_provider is not None:
            eur_price = self.backfill_provider.resolve_historical_price_eur(
                normalized_coin_id,
                normalized_at,
            )
        if eur_price is None:
            try:
                eur_price = self.external_provider.get_historical_price_eur(
                    normalized_coin_id,
                    normalized_at,
                )
            except ValueError as exc:
                local_failure_detail = None
                if self.backfill_provider is not None:
                    describe_failure = getattr(self.backfill_provider, "describe_last_resolution_failure", None)
                    if callable(describe_failure):
                        local_failure_detail = describe_failure(normalized_coin_id, normalized_at)
                raise ValueError(
                    f"Could not resolve EUR price for {normalized_coin_id} at {normalized_at.isoformat()}. "
                    f"{local_failure_detail + ' ' if local_failure_detail else ''}"
                    f"External fallback failed: {exc}"
                ) from exc
        return eur_price
