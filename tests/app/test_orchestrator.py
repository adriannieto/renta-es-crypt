from __future__ import annotations

from unittest import TestCase, main

from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

from src.app.orchestrator import (
    build_price_feed,
    collect_input_year_markers,
    collect_price_feed_warnings,
    filter_ignored_assets,
    format_preheat_downgrade_summary_lines,
    format_preheat_summary_lines,
)
from src.app.settings import AppSettings, PriceFeedProvider
from src.feeds import CachedPriceFeed
from src.model import Transaction, TransactionType
from src.shared import PriceResolution


class DummyWarningFeed:
    def get_warnings(self) -> list[str]:
        return ["warning-1"]


class OrchestratorTestCase(TestCase):
    def test_build_price_feed_defaults_to_cached_feed(self) -> None:
        settings = AppSettings()

        feed = build_price_feed(
            settings=settings,
            resolution=PriceResolution.HOUR,
        )

        self.assertIsInstance(feed, CachedPriceFeed)
        self.assertEqual(feed.resolution, PriceResolution.HOUR)

    def test_build_price_feed_passes_provider_settings(self) -> None:
        settings = AppSettings.model_validate(
            {
                "tax_year": 2024,
                "pricing": {
                    "cryptodatadownload": {
                        "base_url": "https://example.test/cdd",
                        "exchanges": ["Kraken"],
                        "quote_priority": ["EUR", "BTC"],
                        "symbols": {"XBT": "BTC"},
                    },
                    "coingecko": {"api_key": "cg-key", "coin_ids": {"ETH": "ethereum"}},
                },
            }
        )

        feed = build_price_feed(
            settings=settings,
            resolution=PriceResolution.HOUR,
        )

        self.assertIsInstance(feed, CachedPriceFeed)
        self.assertEqual(feed.backfill_provider.base_url, "https://example.test/cdd")
        self.assertEqual(feed.backfill_provider.exchanges, ["Kraken"])
        self.assertEqual(feed.backfill_provider.quote_priority, ["EUR", "BTC"])
        self.assertEqual(feed.backfill_provider.symbols, {"XBT": "BTC"})
        self.assertFalse(feed.backfill_provider.allow_resolution_downgrade)
        self.assertEqual(feed.backfill_provider.tax_year, 2024)
        self.assertEqual(feed.external_provider.api_key, "cg-key")
        self.assertEqual(feed.external_provider.coin_ids, {"ETH": "ethereum"})

    def test_build_price_feed_passes_resolution_downgrade_flag(self) -> None:
        settings = AppSettings.model_validate(
            {
                "pricing": {
                    "allow_backfill_resolution_downgrade": True,
                },
            }
        )

        feed = build_price_feed(
            settings=settings,
            resolution=PriceResolution.MINUTE,
        )

        self.assertIsInstance(feed, CachedPriceFeed)
        self.assertTrue(feed.backfill_provider.allow_resolution_downgrade)

    def test_build_price_feed_rejects_unknown_provider(self) -> None:
        settings = AppSettings.model_validate({"pricing": {"external_provider": "broken"}})

        with self.assertRaises(ValueError):
            build_price_feed(settings=settings, resolution=PriceResolution.HOUR)

    def test_build_price_feed_allows_no_backfill_provider(self) -> None:
        settings = AppSettings.model_validate({"pricing": {"backfill_provider": "none"}})

        feed = build_price_feed(settings=settings, resolution=PriceResolution.HOUR)

        self.assertIsInstance(feed, CachedPriceFeed)
        self.assertIsNone(feed.backfill_provider)

    def test_collect_price_feed_warnings_uses_optional_feed_hook(self) -> None:
        self.assertEqual(collect_price_feed_warnings(DummyWarningFeed()), ["warning-1"])
        self.assertEqual(collect_price_feed_warnings(None), [])

    def test_filter_ignored_assets_removes_asset_and_counter_asset_matches(self) -> None:
        settings = AppSettings.model_validate({"ignored_assets": ["BSV"]})
        transactions = [
            Transaction(
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                asset="BSV",
                type=TransactionType.SELL,
                amount=Decimal("1"),
                price_eur=Decimal("10"),
            ),
            Transaction(
                timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.BUY,
                amount=Decimal("0.1"),
                price_eur=Decimal("30000"),
                counter_asset="BSV",
                counter_amount=Decimal("1"),
            ),
            Transaction(
                timestamp=datetime(2024, 1, 3, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.BUY,
                amount=Decimal("1"),
                price_eur=Decimal("2000"),
            ),
        ]

        filtered = filter_ignored_assets(transactions, settings=settings)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0].asset, "ETH")

    def test_format_preheat_summary_lines_groups_by_asset_and_resolution(self) -> None:
        line = format_preheat_summary_lines(
            [
                {
                    "asset": "BTC",
                    "requested_at": datetime(2017, 3, 31, 10, tzinfo=timezone.utc),
                    "used_resolution": "day",
                },
                {
                    "asset": "BTC",
                    "requested_at": datetime(2018, 1, 1, 12, tzinfo=timezone.utc),
                    "used_resolution": "hour",
                },
                {
                    "asset": "ETH",
                    "requested_at": datetime(2018, 1, 2, 12, tzinfo=timezone.utc),
                    "used_resolution": "hour",
                },
            ]
        )

        self.assertEqual(line, "BTCEUR, ETHEUR")

    def test_format_preheat_downgrade_summary_lines_groups_sparse_daily_points(self) -> None:
        lines = format_preheat_downgrade_summary_lines(
            [
                {
                    "asset": "XTZ",
                    "requested_at": datetime(2021, 7, 22, 14, tzinfo=timezone.utc),
                    "requested_resolution": "hour",
                    "used_resolution": "day",
                },
                {
                    "asset": "XTZ",
                    "requested_at": datetime(2021, 7, 26, 14, tzinfo=timezone.utc),
                    "requested_resolution": "hour",
                    "used_resolution": "day",
                },
                {
                    "asset": "XTZ",
                    "requested_at": datetime(2021, 7, 29, 14, tzinfo=timezone.utc),
                    "requested_resolution": "hour",
                    "used_resolution": "day",
                },
            ]
        )

        self.assertEqual(
            lines,
            [
                "XTZEUR: (2021-07-22 -> 2021-07-29) [day]",
            ],
        )

    def test_collect_input_year_markers_reads_years_from_kraken_input(self) -> None:
        years = collect_input_year_markers(
            "kraken",
            str(Path(__file__).resolve().parents[1] / "data" / "parsers" / "kraken_cli_sample.csv"),
        )

        self.assertEqual(years, [2025])


if __name__ == "__main__":
    main()
