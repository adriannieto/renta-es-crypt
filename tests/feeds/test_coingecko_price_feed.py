from __future__ import annotations
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

import requests

from src.feeds import CoinGeckoPriceFeed


class FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class FakeErrorResponse:
    def raise_for_status(self) -> None:
        raise requests.HTTPError("404 not found")

    def json(self) -> dict:
        return {}


class FakeSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, dict]] = []

    def get(self, url: str, params: dict, headers: dict, timeout: int) -> FakeResponse:
        self.calls.append((url, params, headers))
        return FakeResponse(
            {
                "market_data": {
                    "current_price": {
                        "eur": "123.45",
                        "usd": "130.00",
                    }
                }
            }
        )


class CoinGeckoPriceFeedTestCase(TestCase):
    def test_history_is_cached_after_first_request(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "prices.json"
            session = FakeSession()
            feed = CoinGeckoPriceFeed(
                cache_path=cache_path,
                api_key="demo-key",
                session=session,
            )
            when = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)

            first = feed.get_historical_price_eur("bitcoin", when)
            second = feed.get_historical_price_eur("bitcoin", when)

            self.assertEqual(first, Decimal("123.45"))
            self.assertEqual(second, Decimal("123.45"))
            self.assertEqual(len(session.calls), 1)

    def test_uppercase_asset_requires_explicit_coin_id_mapping(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "prices.json"
            feed = CoinGeckoPriceFeed(cache_path=cache_path, api_key="demo-key")

            with self.assertRaises(ValueError) as context:
                feed.get_historical_price_eur(
                    "TRUMP",
                    datetime(2025, 1, 19, 12, 0, tzinfo=timezone.utc),
                )

            self.assertIn("No CoinGecko coin id configured for asset TRUMP", str(context.exception))

    def test_http_404_is_raised_as_actionable_value_error(self) -> None:
        class ErrorSession:
            def get(self, url: str, params: dict, headers: dict, timeout: int) -> FakeErrorResponse:
                return FakeErrorResponse()

        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "prices.json"
            feed = CoinGeckoPriceFeed(
                cache_path=cache_path,
                api_key="demo-key",
                coin_ids={"TRUMP": "official-trump"},
                session=ErrorSession(),
            )

            with self.assertRaises(ValueError) as context:
                feed.get_historical_price_eur(
                    "TRUMP",
                    datetime(2025, 1, 19, 12, 0, tzinfo=timezone.utc),
                )

            self.assertIn("CoinGecko could not resolve historical price data for official-trump", str(context.exception))

    def test_history_is_reused_across_feed_instances_from_disk_cache(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir) / "prices"
            when = datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc)

            first_session = FakeSession()
            first_feed = CoinGeckoPriceFeed(
                cache_path=cache_dir,
                api_key="demo-key",
                session=first_session,
            )
            self.assertEqual(first_feed.get_historical_price_eur("bitcoin", when), Decimal("123.45"))
            self.assertEqual(len(first_session.calls), 1)

            second_session = FakeSession()
            second_feed = CoinGeckoPriceFeed(
                cache_path=cache_dir,
                api_key="demo-key",
                session=second_session,
            )
            self.assertEqual(second_feed.get_historical_price_eur("bitcoin", when), Decimal("123.45"))
            self.assertEqual(second_session.calls, [])

    def test_pro_host_uses_pro_header_with_generic_api_key(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "prices.json"
            session = FakeSession()
            feed = CoinGeckoPriceFeed(
                cache_path=cache_path,
                base_url="https://pro-api.coingecko.com/api/v3",
                api_key="pro-key",
                session=session,
            )

            feed.get_historical_price_eur(
                "ethereum",
                datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc),
            )

            url, _, headers = session.calls[0]
            self.assertTrue(url.startswith("https://pro-api.coingecko.com/api/v3/"))
            self.assertEqual(headers["x-cg-pro-api-key"], "pro-key")

    def test_hour_resolution_buckets_requests_by_hour(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "prices.json"
            session = FakeSession()
            feed = CoinGeckoPriceFeed(
                cache_path=cache_path,
                api_key="demo-key",
                resolution="hour",
                session=session,
            )

            first_at = datetime(2025, 1, 10, 12, 1, tzinfo=timezone.utc)
            second_at = datetime(2025, 1, 10, 12, 59, tzinfo=timezone.utc)

            feed.get_historical_price_eur("bitcoin", first_at)
            feed.get_historical_price_eur("bitcoin", second_at)

            self.assertEqual(len(session.calls), 1)

    def test_api_key_is_required_when_fetching_network_price(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_path = Path(tmp_dir) / "prices.json"
            feed = CoinGeckoPriceFeed(cache_path=cache_path, api_key=None)

            with self.assertRaises(ValueError):
                feed.get_historical_price_eur(
                    "ethereum",
                    datetime(2025, 1, 10, 12, 0, tzinfo=timezone.utc),
                )


if __name__ == "__main__":
    main()
