from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from src.feeds.cryptodatadownload_backfill import CryptoDataDownloadBackfill
from src.shared import PriceResolution


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "feeds"


class FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400 and self.status_code != 404:
            raise ValueError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses: dict[str, FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, timeout: int) -> FakeResponse:
        self.calls.append(url)
        return self.responses.get(url, FakeResponse(404))

    @classmethod
    def from_files(cls, mapping: dict[str, Path]) -> "FakeSession":
        return cls(
            {
                url: FakeResponse(200, path.read_text(encoding="utf-8"))
                for url, path in mapping.items()
            }
        )

    @classmethod
    def from_texts(cls, mapping: dict[str, str]) -> "FakeSession":
        return cls({url: FakeResponse(200, text) for url, text in mapping.items()})


def build_csv(*rows: tuple[int, str, str]) -> str:
    rendered_rows = [
        f"{unix_value},{date_text},{symbol},{close},{close},{close},{close}"
        for unix_value, date_text, symbol, close in rows
    ]
    return (
        "https://www.CryptoDataDownload.com\n"
        "unix,date,symbol,open,high,low,close\n"
        + "\n".join(rendered_rows)
        + "\n"
    )


def build_hour_csv(pair: str, close: str) -> str:
    return build_csv((1735725600, "2025-01-01 10:00:00", pair, close))


def trim_fixture_rows(path: Path, *, total_lines: int) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    return "\n".join(lines[:total_lines]) + "\n"


class CryptoDataDownloadBackfillTestCase(TestCase):
    def test_multiple_exchanges_are_queried_until_pair_is_found_from_real_hour_fixture(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            pair_url = "https://example.test/cdd/{exchange}_BTCEUR_1h.csv"
            session = FakeSession.from_files(
                {
                    pair_url.format(exchange="Bitstamp"): DATA_DIR / "Bitstamp_BTCEUR_1h.csv",
                }
            )
            events: list[str] = []
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance", "Bitfinex", "Bitstamp"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=session,
                event_logger=events.append,
            )

            at = datetime(2026, 3, 29, 23, 20, tzinfo=timezone.utc)
            self.assertEqual(feed.resolve_historical_price_eur("BTC", at), Decimal("57437"))
            self.assertEqual(
                session.calls,
                [
                    pair_url.format(exchange="Binance"),
                    pair_url.format(exchange="Bitfinex"),
                    pair_url.format(exchange="Bitstamp"),
                ],
            )
            self.assertEqual(events, [])


    def test_btc_eur_resolves_across_multiple_dates_with_hour_resolution_from_real_fixture(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            url = "https://example.test/cdd/Binance_BTCEUR_1h.csv"
            session = FakeSession.from_files(
                {
                    url: DATA_DIR / "Binance_BTCEUR_1h.csv",
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=session,
            )

            self.assertEqual(
                feed.resolve_historical_price_eur("BTC", datetime(2026, 3, 29, 23, 10, tzinfo=timezone.utc)),
                Decimal("57429.51"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("BTC", datetime(2026, 3, 29, 22, 40, tzinfo=timezone.utc)),
                Decimal("57306.31"),
            )
            self.assertEqual(session.calls, [url])

    def test_eth_eur_resolves_across_multiple_dates_with_hour_resolution_from_real_fixture(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            url = "https://example.test/cdd/Binance_ETHEUR_1h.csv"
            session = FakeSession.from_files(
                {
                    url: DATA_DIR / "Binance_ETHEUR_1h.csv",
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=session,
            )

            self.assertEqual(
                feed.resolve_historical_price_eur("ETH", datetime(2026, 3, 29, 23, 20, tzinfo=timezone.utc)),
                Decimal("1726.29"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("ETH", datetime(2026, 3, 29, 22, 5, tzinfo=timezone.utc)),
                Decimal("1717.85"),
            )

    def test_less_common_assets_resolve_to_eur_for_multiple_dates_from_real_fixtures(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession.from_files(
                {
                    "https://example.test/cdd/Binance_ADAEUR_1h.csv": DATA_DIR / "Binance_ADAEUR_1h.csv",
                    "https://example.test/cdd/Binance_SOLEUR_1h.csv": DATA_DIR / "Binance_SOLEUR_1h.csv",
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=session,
            )

            self.assertEqual(
                feed.resolve_historical_price_eur("ADA", datetime(2026, 3, 29, 23, 25, tzinfo=timezone.utc)),
                Decimal("0.2086"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("ADA", datetime(2026, 3, 29, 22, 10, tzinfo=timezone.utc)),
                Decimal("0.2078"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("SOL", datetime(2026, 3, 29, 23, 5, tzinfo=timezone.utc)),
                Decimal("70.84"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("SOL", datetime(2026, 3, 29, 22, 15, tzinfo=timezone.utc)),
                Decimal("70.38"),
            )

    def test_minute_resolution_uses_year_suffix_and_buckets_minutes(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            url = "https://example.test/cdd/Bitstamp_BTCEUR_2017_minute.csv"
            session = FakeSession(
                {
                    url: FakeResponse(
                        200,
                        build_csv(
                            (1514764740, "2017-12-31 23:59:00", "BTCEUR", "11620.12"),
                            (1514764680, "2017-12-31 23:58:00", "BTCEUR", "11625.55"),
                        ),
                    ),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Bitstamp"],
                resolution=PriceResolution.MINUTE,
                tax_year=2017,
                session=session,
            )

            self.assertEqual(
                feed.resolve_historical_price_eur("BTC", datetime(2017, 12, 31, 23, 59, 20, tzinfo=timezone.utc)),
                Decimal("11620.12"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("BTC", datetime(2017, 12, 31, 23, 58, 30, tzinfo=timezone.utc)),
                Decimal("11625.55"),
            )
            self.assertEqual(session.calls, [url])

    def test_day_resolution_uses_daily_suffix(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            url = "https://example.test/cdd/Gemini_BTCEUR_d.csv"
            session = FakeSession(
                {
                    url: FakeResponse(
                        200,
                        build_csv(
                            (1707091200, "2024-02-05 00:00:00", "BTCEUR", "40123.11"),
                            (1707177600, "2024-02-06 00:00:00", "BTCEUR", "40777.00"),
                        ),
                    )
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Gemini"],
                resolution=PriceResolution.DAY,
                tax_year=2024,
                session=session,
            )

            self.assertEqual(
                feed.resolve_historical_price_eur("BTC", datetime(2024, 2, 5, 18, 0, tzinfo=timezone.utc)),
                Decimal("40123.11"),
            )
            self.assertEqual(
                feed.resolve_historical_price_eur("BTC", datetime(2024, 2, 6, 9, 0, tzinfo=timezone.utc)),
                Decimal("40777.00"),
            )
            self.assertEqual(session.calls, [url])

    def test_multiple_exchange_prices_are_averaged_for_same_bucket(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Binance_BTCEUR_1h.csv": FakeResponse(
                        200,
                        build_csv((1711965600, "2024-04-01 10:00:00", "BTCEUR", "60000")),
                    ),
                    "https://example.test/cdd/Bitfinex_BTCEUR_1h.csv": FakeResponse(
                        200,
                        build_csv((1711965600, "2024-04-01 10:00:00", "BTCEUR", "60300")),
                    ),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance", "Bitfinex"],
                resolution=PriceResolution.HOUR,
                tax_year=2024,
                session=session,
            )

            at = datetime(2024, 4, 1, 10, 30, tzinfo=timezone.utc)
            self.assertEqual(feed.resolve_historical_price_eur("BTC", at), Decimal("60150"))

    def test_gap_in_one_exchange_is_filled_by_other_available_exchange(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Binance_ETHEUR_1h.csv": FakeResponse(
                        200,
                        build_csv((1711962000, "2024-04-01 09:00:00", "ETHEUR", "3000")),
                    ),
                    "https://example.test/cdd/Bitfinex_ETHEUR_1h.csv": FakeResponse(
                        200,
                        build_csv((1711965600, "2024-04-01 10:00:00", "ETHEUR", "3150")),
                    ),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance", "Bitfinex"],
                resolution=PriceResolution.HOUR,
                tax_year=2024,
                session=session,
            )

            at = datetime(2024, 4, 1, 10, 15, tzinfo=timezone.utc)
            self.assertEqual(feed.resolve_historical_price_eur("ETH", at), Decimal("3150"))

    def test_anchor_assets_are_limited_to_quote_priority(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Binance_ETHEUR_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Binance_EURETH_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Binance_USDETH_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Binance_ETHUSD_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Binance_BTCETH_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Binance_ETHBTC_1h.csv": FakeResponse(404),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                quote_priority=["EUR", "USD", "BTC"],
                tax_year=2024,
                session=session,
            )

            self.assertIsNone(feed.resolve_historical_price_eur("ETH", datetime(2024, 4, 1, 10, 15, tzinfo=timezone.utc)))
            self.assertFalse(any("ETHSOL" in call or "SOLETH" in call for call in session.calls))

    def test_downloaded_csv_is_reused_from_local_cache_on_same_day(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            url = "https://example.test/cdd/Binance_ETHEUR_1h.csv"
            now = datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc)
            first_session = FakeSession(
                {
                    url: FakeResponse(
                        200,
                        build_csv((1774825200, "2026-03-29 23:00:00", "ETHEUR", "1726.29")),
                    )
                }
            )
            first_feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=first_session,
                now_provider=lambda: now,
            )

            at = datetime(2026, 3, 29, 23, 10, tzinfo=timezone.utc)
            self.assertEqual(first_feed.resolve_historical_price_eur("ETH", at), Decimal("1726.29"))
            self.assertEqual(first_session.calls, [url])

            second_session = FakeSession({})
            events: list[str] = []
            second_feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/other_price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=second_session,
                now_provider=lambda: now,
                event_logger=events.append,
            )
            self.assertEqual(second_feed.resolve_historical_price_eur("ETH", at), Decimal("1726.29"))
            self.assertEqual(second_session.calls, [])
            self.assertEqual(events, [])

    def test_downloaded_csv_is_redownloaded_when_cached_copy_is_from_previous_day(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            url = "https://example.test/cdd/Binance_ETHEUR_1h.csv"
            first_feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=FakeSession(
                    {
                        url: FakeResponse(
                            200,
                            build_csv((1774825200, "2026-03-29 23:00:00", "ETHEUR", "1726.29")),
                        )
                    }
                ),
                now_provider=lambda: datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
            )

            at = datetime(2026, 3, 29, 23, 10, tzinfo=timezone.utc)
            self.assertEqual(first_feed.resolve_historical_price_eur("ETH", at), Decimal("1726.29"))
            cache_file = Path(tmp_dir) / "cryptodatadownload" / "Binance_ETHEUR_1h.csv"
            stale_timestamp = datetime(2026, 3, 29, 10, 0, tzinfo=timezone.utc).timestamp()
            os.utime(cache_file, (stale_timestamp, stale_timestamp))

            second_session = FakeSession(
                {
                    url: FakeResponse(
                        200,
                        build_csv((1774825200, "2026-03-29 23:00:00", "ETHEUR", "1800.00")),
                    )
                }
            )
            second_feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/other_price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance"],
                resolution=PriceResolution.HOUR,
                tax_year=2026,
                session=second_session,
                now_provider=lambda: datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
            )
            self.assertEqual(second_feed.resolve_historical_price_eur("ETH", at), Decimal("1800.00"))
            self.assertEqual(second_session.calls, [url])

    def test_missing_minute_pair_does_not_downgrade_when_disabled(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Bitstamp_BTCEUR_2017_minute.csv": FakeResponse(404),
                    "https://example.test/cdd/Bitstamp_EURBTC_2017_minute.csv": FakeResponse(404),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Bitstamp"],
                resolution=PriceResolution.MINUTE,
                allow_resolution_downgrade=False,
                tax_year=2017,
                session=session,
            )

            at = datetime(2017, 12, 31, 23, 59, 21, tzinfo=timezone.utc)
            self.assertIsNone(feed.resolve_historical_price_eur("BTC", at))
            self.assertEqual(feed.get_warnings(), [])
            self.assertIn("BTCEUR", feed.describe_last_resolution_failure("BTC", at))

    def test_missing_minute_pair_downgrades_to_hour_when_enabled(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Bitstamp_BTCEUR_2017_minute.csv": FakeResponse(404),
                    "https://example.test/cdd/Bitstamp_EURBTC_2017_minute.csv": FakeResponse(404),
                    "https://example.test/cdd/Bitstamp_BTCEUR_1h.csv": FakeResponse(
                        200,
                        build_csv((1514761200, "2017-12-31 23:00:00", "BTCEUR", "11620.12")),
                    ),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Bitstamp"],
                resolution=PriceResolution.MINUTE,
                allow_resolution_downgrade=True,
                tax_year=2017,
                session=session,
            )

            at = datetime(2017, 12, 31, 23, 59, 21, tzinfo=timezone.utc)
            self.assertEqual(feed.resolve_historical_price_eur("BTC", at), Decimal("11620.12"))
            self.assertTrue(
                any(
                    "Some historical prices were resolved using lower-resolution backfill data" in warning
                    for warning in feed.get_warnings()
                )
            )

    def test_missing_hour_pair_downgrades_to_day_when_enabled(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Bitstamp_BTCEUR_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Bitstamp_EURBTC_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Bitstamp_BTCEUR_d.csv": FakeResponse(
                        200,
                        build_csv((1514678400, "2017-12-31 00:00:00", "BTCEUR", "11750.00")),
                    ),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Bitstamp"],
                resolution=PriceResolution.HOUR,
                allow_resolution_downgrade=True,
                tax_year=2017,
                session=session,
            )

            at = datetime(2017, 12, 31, 23, 10, tzinfo=timezone.utc)
            self.assertEqual(feed.resolve_historical_price_eur("BTC", at), Decimal("11750.00"))
            self.assertTrue(
                any(
                    "Some historical prices were resolved using lower-resolution backfill data" in warning
                    for warning in feed.get_warnings()
                )
            )

    def test_minute_resolution_triangulates_ethbtc_btcusd_then_usdeur_for_2017(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Bitstamp_BTCUSD_2017_minute.csv": FakeResponse(
                        200,
                        build_csv((1514764740, "2017-12-31 23:59:00", "BTCUSD", "13880.00")),
                    ),
                    "https://example.test/cdd/Bitstamp_ETHBTC_2017_minute.csv": FakeResponse(
                        200,
                        build_csv((1514764740, "2017-12-31 23:59:00", "ETHBTC", "0.05344001")),
                    ),
                }
            )
            session.responses["https://example.test/cdd/Bitstamp_USDEUR_2017_minute.csv"] = FakeResponse(
                200,
                build_csv((1514764740, "2017-12-31 23:59:00", "USD/EUR", "0.8333")),
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Bitstamp"],
                resolution=PriceResolution.MINUTE,
                tax_year=2017,
                session=session,
            )

            at = datetime(2017, 12, 31, 23, 59, 21, tzinfo=timezone.utc)
            self.assertEqual(
                feed.resolve_historical_price_eur("ETH", at).quantize(Decimal("0.000000000001")),
                (Decimal("0.05344001") * Decimal("13880.00") * Decimal("0.8333")).quantize(
                    Decimal("0.000000000001")
                ),
            )
            self.assertIn("https://example.test/cdd/Bitstamp_ETHBTC_2017_minute.csv", session.calls)
            self.assertIn("https://example.test/cdd/Bitstamp_BTCUSD_2017_minute.csv", session.calls)
            self.assertIn("https://example.test/cdd/Bitstamp_USDEUR_2017_minute.csv", session.calls)

    def test_inverse_pair_is_cached_after_backfilling_eurusd(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            session = FakeSession(
                {
                    "https://example.test/cdd/Bitstamp_USDEUR_1h.csv": FakeResponse(404),
                    "https://example.test/cdd/Bitstamp_EURUSD_1h.csv": FakeResponse(
                        200,
                        build_csv((1711965600, "2024-04-01 10:00:00", "EURUSD", "1.0811")),
                    ),
                }
            )
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Bitstamp"],
                resolution=PriceResolution.HOUR,
                tax_year=2024,
                session=session,
            )

            at = datetime(2024, 4, 1, 10, 10, tzinfo=timezone.utc)
            self.assertEqual(
                feed.convert_fiat_amount_to_eur(Decimal("1"), "USD", at).quantize(Decimal("0.000000000001")),
                (Decimal("1") / Decimal("1.0811")).quantize(Decimal("0.000000000001")),
            )
            self.assertEqual(session.calls, [
                "https://example.test/cdd/Bitstamp_USDEUR_1h.csv",
                "https://example.test/cdd/Bitstamp_EURUSD_1h.csv",
            ])
            self.assertEqual(
                feed.convert_fiat_amount_to_eur(Decimal("2"), "USD", at).quantize(Decimal("0.000000000001")),
                (Decimal("2") / Decimal("1.0811")).quantize(Decimal("0.000000000001")),
            )
            self.assertEqual(
                session.calls,
                [
                    "https://example.test/cdd/Bitstamp_USDEUR_1h.csv",
                    "https://example.test/cdd/Bitstamp_EURUSD_1h.csv",
                ],
            )

    def test_missing_pair_records_symbol_level_resolution_failure_detail(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            feed = CryptoDataDownloadBackfill(
                cache_path=f"{tmp_dir}/price_cache.json",
                base_url="https://example.test/cdd",
                exchanges=["Binance", "Gemini"],
                resolution=PriceResolution.HOUR,
                tax_year=2025,
                session=FakeSession({}),
            )

            at = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
            self.assertIsNone(feed.resolve_historical_price_eur("ETH", at))
            failure = feed.describe_last_resolution_failure("ETH", at)
            self.assertIsNotNone(failure)
            self.assertIn("ETH", failure)
            self.assertIn("2025-01-01T10:00:00+00:00", failure)
            self.assertIn("ETHEUR", failure)


if __name__ == "__main__":
    main()
