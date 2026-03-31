from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from src.feeds import PriceCache


class PriceCacheTestCase(TestCase):
    def test_cache_entry_stores_provider_resolution_and_timestamps_in_memory(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "price-cache.json"
            cache = PriceCache(cache_path)
            requested_at = datetime(2025, 1, 10, 12, 34, 56, tzinfo=timezone.utc)

            cache.set(
                provider="coingecko",
                asset_id="bitcoin",
                requested_at=requested_at,
                resolution="hour",
                prices={"eur": Decimal("123.45")},
            )

            loaded = cache.get(
                provider="coingecko",
                asset_id="bitcoin",
                requested_at=requested_at,
                resolution="hour",
            )

            self.assertEqual(loaded, {"eur": Decimal("123.45")})
            self.assertFalse(cache_path.exists())
            self.assertEqual(cache.cache_path, cache_path)

    def test_persisted_provider_uses_one_file_per_asset_and_resolution(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            persist_dir = Path(tmp_dir) / "external_prices"
            cache = PriceCache(
                persist_dir=persist_dir,
                persist_providers={"coingecko"},
            )
            requested_at = datetime(2025, 1, 10, 12, 34, 56, tzinfo=timezone.utc)

            cache.set(
                provider="coingecko",
                asset_id="bitcoin",
                requested_at=requested_at,
                resolution="hour",
                prices={"eur": Decimal("123.45")},
            )

            persisted_file = persist_dir / "coingecko" / "bitcoin__hour.json"
            self.assertTrue(persisted_file.exists())
            payload = json.loads(persisted_file.read_text(encoding="utf-8"))
            entry = next(iter(payload.values()))
            self.assertEqual(entry["provider"], "coingecko")
            self.assertEqual(entry["asset_id"], "bitcoin")
            self.assertEqual(entry["resolution"], "hour")

    def test_persisted_provider_is_loaded_on_new_cache_instance(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            persist_dir = Path(tmp_dir) / "external_prices"
            requested_at = datetime(2025, 1, 10, 12, 34, 56, tzinfo=timezone.utc)
            first_cache = PriceCache(
                persist_dir=persist_dir,
                persist_providers={"coingecko"},
            )
            first_cache.set(
                provider="coingecko",
                asset_id="bitcoin",
                requested_at=requested_at,
                resolution="hour",
                prices={"eur": Decimal("123.45")},
            )

            second_cache = PriceCache(
                persist_dir=persist_dir,
                persist_providers={"coingecko"},
            )
            loaded = second_cache.get(
                provider="coingecko",
                asset_id="bitcoin",
                requested_at=requested_at,
                resolution="hour",
            )

            self.assertEqual(loaded, {"eur": Decimal("123.45")})


if __name__ == "__main__":
    main()
