from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.shared import normalize_price_timestamp


class PriceCache:
    """Shared price cache with optional per-provider disk persistence."""

    def __init__(
        self,
        cache_path: str | Path = ".cache/price_cache.json",
        *,
        persist_dir: str | Path | None = None,
        persist_providers: set[str] | None = None,
    ) -> None:
        self.cache_path = Path(cache_path)
        self.persist_dir = Path(persist_dir).expanduser() if persist_dir is not None else None
        self.persist_providers = set(persist_providers or set())
        self._cache: dict[str, dict[str, Any]] = {}

    def get(
        self,
        *,
        provider: str,
        asset_id: str,
        requested_at: datetime,
        resolution: str,
    ) -> dict[str, Decimal] | None:
        bucketed_at = normalize_price_timestamp(requested_at, resolution)
        cache_key = self._build_cache_key(
            provider=provider,
            asset_id=asset_id,
            bucketed_at=bucketed_at,
            resolution=resolution,
        )
        entry = self._cache.get(cache_key)
        if entry is None and provider in self.persist_providers:
            entry = self._load_persisted_entry(
                provider=provider,
                asset_id=asset_id,
                bucketed_at=bucketed_at,
                resolution=resolution,
            )
            if entry is not None:
                self._cache[cache_key] = entry
        if entry is None:
            return None
        return {
            currency: Decimal(value)
            for currency, value in entry["prices"].items()
        }

    def set(
        self,
        *,
        provider: str,
        asset_id: str,
        requested_at: datetime,
        resolution: str,
        prices: dict[str, Decimal],
    ) -> None:
        normalized_requested_at = requested_at.astimezone(timezone.utc)
        bucketed_at = normalize_price_timestamp(normalized_requested_at, resolution)
        self._cache[
            self._build_cache_key(
                provider=provider,
                asset_id=asset_id,
                bucketed_at=bucketed_at,
                resolution=resolution,
            )
        ] = entry = {
            "provider": provider,
            "asset_id": asset_id,
            "requested_at": normalized_requested_at.isoformat(),
            "bucketed_at": bucketed_at.isoformat(),
            "resolution": resolution,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "prices": {
                currency: str(value)
                for currency, value in prices.items()
            },
        }
        self._persist_entry_if_needed(entry)

    def set_many(
        self,
        *,
        provider: str,
        asset_id: str,
        resolution: str,
        price_points: dict[datetime, dict[str, Decimal]],
    ) -> None:
        if not price_points:
            return

        fetched_at = datetime.now(timezone.utc).isoformat()
        for requested_at, prices in price_points.items():
            normalized_requested_at = requested_at.astimezone(timezone.utc)
            bucketed_at = normalize_price_timestamp(normalized_requested_at, resolution)
            self._cache[
                self._build_cache_key(
                    provider=provider,
                    asset_id=asset_id,
                    bucketed_at=bucketed_at,
                    resolution=resolution,
                )
            ] = entry = {
                "provider": provider,
                "asset_id": asset_id,
                "requested_at": normalized_requested_at.isoformat(),
                "bucketed_at": bucketed_at.isoformat(),
                "resolution": resolution,
                "fetched_at": fetched_at,
                "prices": {
                    currency: str(value)
                    for currency, value in prices.items()
                },
            }
            self._persist_entry_if_needed(entry)

    def _build_cache_key(
        self,
        *,
        provider: str,
        asset_id: str,
        bucketed_at: datetime,
        resolution: str,
    ) -> str:
        return f"{provider}:{asset_id}:{resolution}:{bucketed_at.isoformat()}"

    def _persist_entry_if_needed(self, entry: dict[str, Any]) -> None:
        provider = entry["provider"]
        if provider not in self.persist_providers or self.persist_dir is None:
            return
        persistence_path = self._persistence_path(
            provider=provider,
            asset_id=entry["asset_id"],
            resolution=entry["resolution"],
        )
        persistence_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._read_persistence_payload(persistence_path)
        payload[entry["bucketed_at"]] = entry
        persistence_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _load_persisted_entry(
        self,
        *,
        provider: str,
        asset_id: str,
        bucketed_at: datetime,
        resolution: str,
    ) -> dict[str, Any] | None:
        persistence_path = self._persistence_path(
            provider=provider,
            asset_id=asset_id,
            resolution=resolution,
        )
        payload = self._read_persistence_payload(persistence_path)
        return payload.get(bucketed_at.isoformat())

    def _persistence_path(self, *, provider: str, asset_id: str, resolution: str) -> Path:
        assert self.persist_dir is not None
        safe_provider = self._slug(provider)
        safe_asset_id = self._slug(asset_id)
        safe_resolution = self._slug(resolution)
        return self.persist_dir / safe_provider / f"{safe_asset_id}__{safe_resolution}.json"

    def _read_persistence_payload(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return {}
        return json.loads(raw)

    def _slug(self, value: str) -> str:
        return "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value)
