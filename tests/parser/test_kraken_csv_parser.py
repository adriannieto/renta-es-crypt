from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from src.feeds import StubPriceFeed
from src.parser import parse_kraken_csv
from src.parser.kraken_csv_parser import KrakenCsvParser


DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "parsers"


class KrakenCsvParserTestCase(TestCase):
    def setUp(self) -> None:
        self.price_feed = StubPriceFeed(
            prices_by_coin_id={
                "bitcoin": Decimal("31000"),
                "ethereum": Decimal("2000"),
            },
            fiat_rates_to_eur={
                "USD": Decimal("0.92"),
            },
        )

    def test_ignored_asset_is_skipped_before_price_lookup(self) -> None:
        class SpyPriceFeed:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            def get_historical_price_eur(self, coin_id: str, at):
                self.calls.append(("price", coin_id))
                return Decimal("1")

            def convert_asset_amount_to_eur(self, amount: Decimal, coin_id: str, at):
                self.calls.append(("asset", coin_id))
                return amount

            def convert_fiat_amount_to_eur(self, amount: Decimal, currency: str, at):
                self.calls.append(("fiat", currency))
                return amount

        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_bsv.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2018-11-19 00:15:34\",\"transfer\",\"\",\"currency\",\"crypto\",\"BSV\",\"spot / main\",\"0.6368800000\",\"0\",\"0.6368800000\"\n"
                ),
                encoding="utf-8",
            )
            spy_feed = SpyPriceFeed()

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=spy_feed,
                ignored_assets={"BSV"},
            )

        self.assertEqual(parsed, [])
        self.assertEqual(spy_feed.calls, [])

    def test_kraken_parser_falls_back_to_price_service(self) -> None:
        parsed = parse_kraken_csv(
            DATA_DIR / "kraken_missing_price.csv",
            price_service=self.price_feed,
            coin_ids={"BTC": "bitcoin"},
        )

        self.assertEqual(parsed[0].price_eur, Decimal("31000"))
        self.assertEqual(parsed[0].fee_eur, Decimal("3.1000"))

    def test_trade_row_is_expanded_into_permuta_sell_and_buy(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "trade.csv"
            trade_csv.write_text(
                (
                    "time,asset,type,amount,price_eur,received_asset,received_amount,txid\n"
                    "2025-02-01T10:00:00Z,BTC,TRADE,0.1,40000,ETH,2,swap-1\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(trade_csv)

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].type.value, "SELL")
        self.assertEqual(parsed[1].type.value, "BUY")
        self.assertIn("Permuta", parsed[0].flags)
        self.assertEqual(parsed[1].asset, "ETH")

    def test_trade_row_uses_only_sold_asset_price_for_crypto_to_crypto(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "trade.csv"
            trade_csv.write_text(
                (
                    "time,asset,type,amount,received_asset,received_amount,txid\n"
                    "2025-02-01T10:00:00Z,BTC,TRADE,0.1,ETH,2,swap-1\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                trade_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"bitcoin": Decimal("31000")}),
                coin_ids={"BTC": "bitcoin", "ETH": "ethereum"},
            )

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].type.value, "SELL")
        self.assertEqual(parsed[0].price_eur, Decimal("31000"))
        self.assertEqual(parsed[1].type.value, "BUY")
        self.assertEqual(parsed[1].price_eur, Decimal("1550"))

    def test_trade_row_uses_exchange_counter_btc_before_direct_asset_feed(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "trade.csv"
            trade_csv.write_text(
                (
                    "time,asset,type,amount,received_asset,received_amount,txid\n"
                    "2025-02-01T10:00:00Z,ETH,TRADE,2,BTC,0.1,swap-eth-btc\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                trade_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"BTC": Decimal("31000")}),
                coin_ids={"BTC": "BTC"},
            )

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].type.value, "SELL")
        self.assertEqual(parsed[0].price_eur, Decimal("1550"))
        self.assertEqual(parsed[1].type.value, "BUY")
        self.assertEqual(parsed[1].price_eur, Decimal("31000"))

    def test_trade_row_with_fiat_counterparty_becomes_buy_without_price_feed_lookup(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "fiat_trade.csv"
            trade_csv.write_text(
                (
                    "time,asset,type,amount,received_asset,received_amount,txid\n"
                    "2025-02-01T10:00:00Z,EUR,TRADE,30000,BTC,1,swap-1\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(trade_csv)

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "BUY")
        self.assertEqual(parsed[0].asset, "BTC")
        self.assertEqual(parsed[0].price_eur, Decimal("30000"))

    def test_trade_row_with_usd_counterparty_uses_fiat_fx_conversion(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "usd_trade.csv"
            trade_csv.write_text(
                (
                    "time,asset,type,amount,received_asset,received_amount,txid\n"
                    "2025-02-01T10:00:00Z,USD,TRADE,1000,ETH,0.5,swap-1\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                trade_csv,
                price_service=self.price_feed,
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "BUY")
        self.assertEqual(parsed[0].asset, "ETH")
        self.assertEqual(parsed[0].price_eur, Decimal("1840"))

    def test_kraken_ledger_trade_rows_are_grouped_by_refid_into_permuta(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "kraken_ledger_trade.csv"
            trade_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"order-1\",\"2025-02-01T10:00:00Z\",\"trade\",\"\",\"currency\",\"\",\"XBT\",\"spot\",\"-0.1\",\"0.0001\",\"0.9\"\n"
                    "\"ledger-2\",\"order-1\",\"2025-02-01T10:00:00Z\",\"trade\",\"\",\"currency\",\"\",\"ETH\",\"spot\",\"1.5\",\"0\",\"5\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                trade_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"bitcoin": Decimal("31000")}),
                coin_ids={"BTC": "bitcoin", "ETH": "ethereum"},
            )

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].type.value, "SELL")
        self.assertEqual(parsed[0].asset, "BTC")
        self.assertEqual(parsed[0].fee_eur, Decimal("3.1000"))
        self.assertIn("Permuta", parsed[0].flags)
        self.assertEqual(parsed[1].type.value, "BUY")
        self.assertEqual(parsed[1].asset, "ETH")
        self.assertEqual(parsed[1].pair_id, "order-1")
        self.assertEqual(parsed[1].price_eur, Decimal("2066.666666666666666666666667"))

    def test_kraken_ledger_trade_group_uses_btc_counter_leg_before_direct_asset_feed(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "kraken_ledger_trade.csv"
            trade_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"order-eth-btc\",\"2025-02-01T10:00:00Z\",\"trade\",\"\",\"currency\",\"\",\"ETH\",\"spot\",\"-2\",\"0\",\"3\"\n"
                    "\"ledger-2\",\"order-eth-btc\",\"2025-02-01T10:00:00Z\",\"trade\",\"\",\"currency\",\"\",\"XBT\",\"spot\",\"0.1\",\"0\",\"1.1\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                trade_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"BTC": Decimal("31000")}),
                coin_ids={"BTC": "BTC"},
            )

        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].asset, "ETH")
        self.assertEqual(parsed[0].type.value, "SELL")
        self.assertEqual(parsed[0].price_eur, Decimal("1550"))
        self.assertEqual(parsed[1].asset, "BTC")
        self.assertEqual(parsed[1].price_eur, Decimal("31000"))

    def test_kraken_preheat_requests_for_crypto_trade_follow_actual_valuation_dependency(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "kraken_ledger_trade.csv"
            trade_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"order-bch-btc\",\"2017-11-29T11:43:46Z\",\"trade\",\"tradespot\",\"currency\",\"crypto\",\"BCH\",\"spot\",\"0.0279260460\",\"0\",\"0.0279260460\"\n"
                    "\"ledger-2\",\"order-bch-btc\",\"2017-11-29T11:43:46Z\",\"trade\",\"tradespot\",\"currency\",\"crypto\",\"XBT\",\"spot\",\"-0.0029561612\",\"0.0000067991\",\"0.0468798626\"\n"
                ),
                encoding="utf-8",
            )

            parser = KrakenCsvParser()
            requests = parser.collect_price_preheat_requests(trade_csv)

        self.assertEqual(
            requests,
            [("BTC", datetime(2017, 11, 29, 11, 43, 46, tzinfo=timezone.utc))],
        )

    def test_kraken_group_with_multiple_same_asset_debits_is_collapsed_before_trade_parsing(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "kraken_ledger_multi_debit_trade.csv"
            trade_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2025-01-19 07:23:44\",\"trade\",\"tradespot\",\"currency\",\"fiat\",\"EUR\",\"earn / liquid\",\"-5136.0000\",\"0\",\"38727.6907\"\n"
                    "\"ledger-2\",\"ref-1\",\"2025-01-19 07:23:44\",\"trade\",\"tradespot\",\"currency\",\"fiat\",\"EUR\",\"spot / main\",\"-0.0001\",\"0\",\"38727.6906\"\n"
                    "\"ledger-3\",\"ref-1\",\"2025-01-19 07:23:44\",\"trade\",\"tradespot\",\"currency\",\"crypto\",\"TRUMP\",\"spot / main\",\"81.341352\",\"0.181508\",\"81.159844\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                trade_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"TRUMP": Decimal("10")}),
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "BUY")
        self.assertEqual(parsed[0].asset, "TRUMP")
        self.assertEqual(parsed[0].amount, Decimal("81.341352"))
        self.assertEqual(parsed[0].price_eur, Decimal("5136.0001") / Decimal("81.341352"))

    def test_kraken_incomplete_group_is_skipped_with_warning(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "kraken_ledger_incomplete_trade.csv"
            trade_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"TYGGOZ-QWI6A-QYXXOC\",\"2021-02-06 22:57:55\",\"trade\",\"tradespot\",\"currency\",\"crypto\",\"DOGE\",\"spot / main\",\"-0.00006528\",\"0\",\"123.456789\"\n"
                ),
                encoding="utf-8",
            )

            parser = KrakenCsvParser()
            parsed = parser.parse(trade_csv, price_feed=self.price_feed)

        self.assertEqual(parsed, [])
        self.assertEqual(
            parser.get_warnings(),
            [
                "Skipped 1 incomplete Kraken grouped ledger record(s). These grouped rows did not contain a complete debit/credit pair and were excluded from the calculation."
            ],
        )

    def test_kraken_dust_sweeping_group_is_skipped_with_warning(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            trade_csv = Path(tmp_dir) / "kraken_ledger_dust_sweeping.csv"
            trade_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"dust-ref-1\",\"2025-02-01 09:00:00\",\"spend\",\"dustsweeping\",\"currency\",\"crypto\",\"ETHW\",\"spot / main\",\"-0.12345678\",\"0\",\"0\"\n"
                    "\"ledger-2\",\"dust-ref-1\",\"2025-02-01 09:00:00\",\"spend\",\"dustsweeping\",\"currency\",\"crypto\",\"SHIB\",\"spot / main\",\"-1000\",\"0\",\"0\"\n"
                    "\"ledger-3\",\"dust-ref-1\",\"2025-02-01 09:00:00\",\"spend\",\"dustsweeping\",\"currency\",\"crypto\",\"UST\",\"spot / main\",\"-50\",\"0\",\"0\"\n"
                    "\"ledger-4\",\"dust-ref-1\",\"2025-02-01 09:00:00\",\"receive\",\"dustsweeping\",\"currency\",\"fiat\",\"EUR\",\"spot / main\",\"0.12\",\"0\",\"0.12\"\n"
                ),
                encoding="utf-8",
            )

            parser = KrakenCsvParser()
            parsed = parser.parse(trade_csv, price_feed=self.price_feed)

        self.assertEqual(parsed, [])
        self.assertEqual(
            parser.get_warnings(),
            [
                "Skipped 1 Kraken dust-sweeping ledger group(s). These administrative consolidation records were excluded from the calculation."
            ],
        )

    def test_kraken_ledger_reward_and_staking_transfers_are_mapped_explicitly(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_staking.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2025-02-10T10:00:00Z\",\"earn\",\"reward\",\"currency\",\"\",\"ETH\",\"spot / flexible\",\"0.2\",\"0\",\"5.2\"\n"
                    "\"ledger-2\",\"ref-2\",\"2025-02-11T10:00:00Z\",\"transfer\",\"spottostaking\",\"currency\",\"\",\"ETH\",\"spot\",\"-1\",\"0\",\"4.2\"\n"
                    "\"ledger-3\",\"ref-2\",\"2025-02-11T10:00:00Z\",\"transfer\",\"stakingfromspot\",\"currency\",\"\",\"ETH\",\"earn\",\"1\",\"0\",\"1\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=self.price_feed,
                coin_ids={"ETH": "ethereum"},
            )

        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed[0].type.value, "STAKE_REWARD")
        self.assertEqual(parsed[0].price_eur, Decimal("2000"))
        self.assertEqual(parsed[1].type.value, "INTERNAL_TRANSFER")
        self.assertEqual(parsed[2].type.value, "INTERNAL_TRANSFER")

    def test_kraken_staking_row_with_empty_subtype_is_treated_as_reward_and_suffix_is_normalized(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_xtz_staking.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2020-06-04 16:28:38\",\"staking\",\"\",\"currency\",\"staking_on_chain\",\"XTZ.S\",\"spot / main\",\"0.10552497\",\"0\",\"145.68552497\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"tezos": Decimal("2.40")}),
                coin_ids={"XTZ": "tezos"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "STAKE_REWARD")
        self.assertEqual(parsed[0].asset, "XTZ")
        self.assertEqual(parsed[0].price_eur, Decimal("2.40"))

    def test_kraken_eth_staking_row_with_empty_subtype_is_treated_as_reward_and_fee_is_converted(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_eth_staking.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2023-04-16 01:48:52\",\"staking\",\"\",\"currency\",\"crypto\",\"ETH\",\"spot / main\",\"0.0002303521\",\"0.0000181914\",\"0.8217072976\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"ethereum": Decimal("2000")}),
                coin_ids={"ETH": "ethereum"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "STAKE_REWARD")
        self.assertEqual(parsed[0].asset, "ETH")
        self.assertEqual(parsed[0].price_eur, Decimal("2000"))
        self.assertEqual(parsed[0].fee_eur, Decimal("0.0363828000"))

    def test_kraken_negative_eth2_hold_staking_row_is_treated_as_internal_transfer(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_eth2_release.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2023-04-18 13:00:45\",\"staking\",\"\",\"currency\",\"hold\",\"ETH2\",\"spot / main\",\"-0.1111167376\",\"0\",\"-0.0036976389\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"ethereum": Decimal("2000")}),
                coin_ids={"ETH": "ethereum"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "INTERNAL_TRANSFER")
        self.assertEqual(parsed[0].asset, "ETH")
        self.assertIn("Kraken-Staking-Release", parsed[0].flags)
        self.assertEqual(parsed[0].price_eur, Decimal("0"))

    def test_kraken_staking_variant_asset_with_numeric_program_code_is_normalized(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_atom21_staking.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2024-01-23 03:03:34\",\"staking\",\"\",\"currency\",\"staking_on_chain\",\"ATOM21.S\",\"spot / main\",\"0.00000915\",\"0\",\"0.00419329\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"cosmos": Decimal("10")}),
                coin_ids={"ATOM": "cosmos"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "STAKE_REWARD")
        self.assertEqual(parsed[0].asset, "ATOM")
        self.assertEqual(parsed[0].price_eur, Decimal("10"))

    def test_kraken_staking_transfer_variant_asset_with_numeric_program_code_is_normalized(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_sol03_transfer.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2024-01-17 06:45:56\",\"transfer\",\"stakingfromspot\",\"currency\",\"staking_on_chain\",\"SOL03.S\",\"spot / main\",\"4.5362372026\",\"0\",\"4.5362372026\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"solana": Decimal("100")}),
                coin_ids={"SOL": "solana"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "INTERNAL_TRANSFER")
        self.assertEqual(parsed[0].asset, "SOL")

    def test_kraken_non_staking_numeric_asset_is_not_collapsed(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_luna2_airdrop.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2022-05-28 14:27:01\",\"dividend\",\"airdrop\",\"currency\",\"crypto\",\"LUNA2\",\"spot / main\",\"2.53790919\",\"0\",\"2.53790919\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"terra-luna-2": Decimal("6.25")}),
                coin_ids={"LUNA2": "terra-luna-2"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].asset, "LUNA2")

    def test_kraken_dividend_airdrop_is_mapped_to_airdrop(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_luna2_airdrop.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2022-05-28 14:27:01\",\"dividend\",\"airdrop\",\"currency\",\"crypto\",\"LUNA2\",\"spot / main\",\"2.53790919\",\"0\",\"2.53790919\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"terra-luna-2": Decimal("6.25")}),
                coin_ids={"LUNA2": "terra-luna-2"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "AIRDROP")
        self.assertEqual(parsed[0].asset, "LUNA2")
        self.assertIn("Kraken-Airdrop", parsed[0].flags)
        self.assertEqual(parsed[0].price_eur, Decimal("6.25"))

    def test_kraken_earn_airdrop_is_mapped_to_airdrop(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_flr_airdrop.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2024-04-06 01:49:54\",\"earn\",\"airdrop\",\"currency\",\"crypto\",\"FLR\",\"earn / flexible\",\"1.5467\",\"0\",\"233.7883\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"flare-networks": Decimal("0.03")}),
                coin_ids={"FLR": "flare-networks"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "AIRDROP")
        self.assertEqual(parsed[0].asset, "FLR")
        self.assertIn("Kraken-Airdrop", parsed[0].flags)
        self.assertEqual(parsed[0].price_eur, Decimal("0.03"))

    def test_kraken_margin_rows_are_skipped_with_warning(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_margin.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2023-12-29 07:36:58\",\"margin\",\"\",\"currency\",\"fiat\",\"USD\",\"spot / main\",\"-0.4076\",\"0\",\"3.1055\"\n"
                    "\"ledger-2\",\"ref-1\",\"2023-12-29 07:36:58\",\"margin\",\"\",\"currency\",\"crypto\",\"BTC\",\"spot / main\",\"-0.0015720252\",\"0.0000419124\",\"-0.0372443099\"\n"
                ),
                encoding="utf-8",
            )

            parser = KrakenCsvParser()
            parsed = parser.parse(
                ledger_csv,
                price_feed=StubPriceFeed(prices_by_coin_id={"bitcoin": Decimal("30000")}),
                coin_ids={"BTC": "bitcoin"},
            )

        self.assertEqual(parsed, [])
        self.assertEqual(
            parser.get_warnings(),
            [
                "Skipped 2 Kraken margin ledger row(s). Margin activity is not modeled by the spot FIFO engine and was excluded from the calculation."
            ],
        )

    def test_kraken_delisting_conversion_transfer_is_skipped_with_warning(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_delisting_conversion.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2025-03-31 16:24:01\",\"transfer\",\"delistingconversion\",\"currency\",\"stable_coin\",\"USDT\",\"earn / liquid\",\"-0.00013064\",\"0\",\"105.99251062\"\n"
                ),
                encoding="utf-8",
            )

            parser = KrakenCsvParser()
            parsed = parser.parse(ledger_csv, price_feed=self.price_feed)

        self.assertEqual(parsed, [])
        self.assertEqual(
            parser.get_warnings(),
            [
                "Skipped 1 Kraken delisting-conversion ledger row(s). These administrative transfer records were excluded from the calculation."
            ],
        )

    def test_kraken_earn_migration_group_is_treated_as_internal_transfer(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            ledger_csv = Path(tmp_dir) / "kraken_ledger_earn_migration.csv"
            ledger_csv.write_text(
                (
                    "\"txid\",\"refid\",\"time\",\"type\",\"subtype\",\"aclass\",\"subclass\",\"asset\",\"wallet\",\"amount\",\"fee\",\"balance\"\n"
                    "\"ledger-1\",\"ref-1\",\"2024-02-22 12:45:14\",\"earn\",\"migration\",\"currency\",\"staking_on_chain\",\"XTZ.S\",\"spot / main\",\"-3.82223180\",\"0\",\"-51.38118421\"\n"
                    "\"ledger-2\",\"ref-1\",\"2024-02-22 12:45:14\",\"earn\",\"migration\",\"currency\",\"crypto\",\"XTZ\",\"earn / flexible\",\"3.36414676\",\"0\",\"-73.40398624\"\n"
                ),
                encoding="utf-8",
            )

            parsed = parse_kraken_csv(
                ledger_csv,
                price_service=StubPriceFeed(prices_by_coin_id={"tezos": Decimal("2.40")}),
                coin_ids={"XTZ": "tezos"},
            )

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].type.value, "INTERNAL_TRANSFER")
        self.assertEqual(parsed[0].asset, "XTZ")
        self.assertEqual(parsed[0].pair_id, "ref-1")
        self.assertIn("Kraken-Earn-Migration", parsed[0].flags)


if __name__ == "__main__":
    main()
