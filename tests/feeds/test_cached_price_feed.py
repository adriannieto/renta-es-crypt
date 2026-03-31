from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from src.feeds import CachedPriceFeed, CoinGeckoPriceFeed, CryptoDataDownloadBackfill, PriceCache
from src.shared import PriceResolution

from tests.feeds.test_cryptodatadownload_backfill import FakeResponse, FakeSession, build_hour_csv


class ExternalOnlySession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, dict]] = []

    def get(self, url: str, params: dict, headers: dict, timeout: int):
        self.calls.append((url, params, headers))

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"market_data": {"current_price": {"eur": "1234.56"}}}

        return Response()


class CachedPriceFeedTestCase(TestCase):
    def test_cached_feed_uses_backfill_first_and_skips_external(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = f"{tmp_dir}/price_cache.json"
            price_cache = PriceCache(cache_path)
            backfill = CryptoDataDownloadBackfill(
                cache_path=cache_path,
                price_cache=price_cache,
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                session=FakeSession(
                    {
                        "https://example.test/cdd/Binance_ETHEUR_1h.csv": FakeResponse(
                            200,
                            build_hour_csv("ETHEUR", "2100"),
                        )
                    }
                ),
            )
            external_session = ExternalOnlySession()
            external = CoinGeckoPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                api_key="cg-key",
                coin_ids={"ETH": "ethereum"},
                session=external_session,
                resolution=PriceResolution.HOUR,
            )
            feed = CachedPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                resolution=PriceResolution.HOUR,
                backfill_provider=backfill,
                external_provider=external,
            )

            value = feed.get_historical_price_eur(
                "ETH",
                datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(value, Decimal("2100"))
            self.assertEqual(external_session.calls, [])

    def test_cached_feed_falls_back_to_external_provider(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = f"{tmp_dir}/price_cache.json"
            price_cache = PriceCache(cache_path)
            events: list[str] = []
            backfill = CryptoDataDownloadBackfill(
                cache_path=cache_path,
                price_cache=price_cache,
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                session=FakeSession({}),
                event_logger=events.append,
            )
            external_session = ExternalOnlySession()
            external = CoinGeckoPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                api_key="cg-key",
                coin_ids={"ETH": "ethereum"},
                session=external_session,
                resolution=PriceResolution.HOUR,
                event_logger=events.append,
            )
            feed = CachedPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                resolution=PriceResolution.HOUR,
                backfill_provider=backfill,
                external_provider=external,
                event_logger=events.append,
            )

            value = feed.get_historical_price_eur(
                "ETH",
                datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(value, Decimal("1234.56"))
            self.assertEqual(len(external_session.calls), 1)
            self.assertEqual(feed.get_warnings(), [])
            self.assertFalse(any("Trying external provider" in event for event in events))
            self.assertFalse(any("Falling back to CoinGecko" in event for event in events))

    def test_cached_feed_reports_missing_external_api_key_clearly(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = f"{tmp_dir}/price_cache.json"
            price_cache = PriceCache(cache_path)
            backfill = CryptoDataDownloadBackfill(
                cache_path=cache_path,
                price_cache=price_cache,
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                session=FakeSession({}),
            )
            external = CoinGeckoPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                api_key=None,
                coin_ids={"ETH": "ethereum"},
                session=ExternalOnlySession(),
                resolution=PriceResolution.HOUR,
            )
            feed = CachedPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                resolution=PriceResolution.HOUR,
                backfill_provider=backfill,
                external_provider=external,
            )

            with self.assertRaises(ValueError) as context:
                feed.get_historical_price_eur(
                    "ETH",
                    datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                )

            self.assertIn("Could not resolve EUR price for ETH", str(context.exception))
            self.assertIn("Tried local pairs:", str(context.exception))
            self.assertIn("no API key was provided", str(context.exception))

    def test_preheat_requests_returns_backfill_summary_records(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = f"{tmp_dir}/price_cache.json"
            price_cache = PriceCache(cache_path)
            backfill = CryptoDataDownloadBackfill(
                cache_path=cache_path,
                price_cache=price_cache,
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                session=FakeSession(
                    {
                        "https://example.test/cdd/Binance_ETHEUR_1h.csv": FakeResponse(
                            200,
                            build_hour_csv("ETHEUR", "2100"),
                        )
                    }
                ),
            )
            external = CoinGeckoPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                api_key="cg-key",
                coin_ids={"ETH": "ethereum"},
                session=ExternalOnlySession(),
                resolution=PriceResolution.HOUR,
            )
            feed = CachedPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                resolution=PriceResolution.HOUR,
                backfill_provider=backfill,
                external_provider=external,
            )

            records = feed.preheat_requests(
                [("ETH", datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc))]
            )

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["asset"], "ETH")
            self.assertEqual(records[0]["used_resolution"], PriceResolution.HOUR)

    def test_preheat_requests_does_not_call_external_provider_for_unresolved_asset(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = f"{tmp_dir}/price_cache.json"
            price_cache = PriceCache(cache_path)
            backfill = CryptoDataDownloadBackfill(
                cache_path=cache_path,
                price_cache=price_cache,
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                session=FakeSession({}),
            )
            external_session = ExternalOnlySession()
            external = CoinGeckoPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                api_key="cg-key",
                coin_ids={"ETH": "ethereum"},
                session=external_session,
                resolution=PriceResolution.HOUR,
            )
            feed = CachedPriceFeed(
                cache_path=cache_path,
                price_cache=price_cache,
                resolution=PriceResolution.HOUR,
                backfill_provider=backfill,
                external_provider=external,
            )

            records = feed.preheat_requests(
                [("ETH", datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc))]
            )

            self.assertEqual(records, [])
            self.assertEqual(external_session.calls, [])


if __name__ == "__main__":
    main()
