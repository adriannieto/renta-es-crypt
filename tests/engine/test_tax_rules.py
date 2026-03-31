from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

import pandas as pd

from src.app.settings import TwoMonthRuleMode, UnmatchedTransferInMode
from src.engine import FifoEngine, InsufficientInventoryError, UnmatchedInboundTransfersError
from src.feeds import StubPriceFeed
from src.model import Transaction, TransactionType
from src.parser import parse_kraken_csv


class TaxRulesTestCase(TestCase):
    def setUp(self) -> None:
        self.price_feed = StubPriceFeed(
            {
                "bitcoin": Decimal("31000"),
                "ethereum": Decimal("2000"),
            }
        )

    def test_internal_transfer_is_matched_and_excluded_from_taxable_events(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.BUY,
                amount=Decimal("1"),
                price_eur=Decimal("30000"),
                fee_eur=Decimal("0"),
                source="Kraken",
            ),
            Transaction(
                timestamp=datetime(2025, 1, 10, 10, 0, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.TRANSFER_OUT,
                amount=Decimal("0.5"),
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                source="Kraken",
                location="Kraken Spot",
            ),
            Transaction(
                timestamp=datetime(2025, 1, 10, 11, 30, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.TRANSFER_IN,
                amount=Decimal("0.497"),
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                source="Ledger",
                location="Ledger Nano",
            ),
        ]

        report = FifoEngine().process_transactions(transactions)

        self.assertEqual(len(report.realized_gains), 0)
        self.assertEqual(len(report.internal_transfers), 1)
        self.assertEqual(len(report.unmatched_transfers), 0)
        self.assertEqual(report.open_lots["BTC"][0].amount_remaining, Decimal("1"))

    def test_unmatched_transfer_in_can_be_added_with_zero_cost_basis(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.TRANSFER_IN,
                amount=Decimal("1"),
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                tx_id="legacy-mine-1",
                source="Ledger",
                location="Ledger Nano",
            ),
            Transaction(
                timestamp=datetime(2025, 3, 1, 10, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.SELL,
                amount=Decimal("1"),
                price_eur=Decimal("2500"),
                fee_eur=Decimal("0"),
                tx_id="sell-1",
            ),
        ]

        report = FifoEngine().process_transactions(
            transactions,
            unmatched_transfer_in_mode=UnmatchedTransferInMode.ZERO_COST_BASIS,
        )

        self.assertEqual(len(report.unmatched_transfers), 1)
        self.assertEqual(len(report.realized_gains), 1)
        self.assertEqual(report.realized_gains[0].acquisition_value_eur, Decimal("0"))
        self.assertEqual(report.realized_gains[0].result_eur, Decimal("2500"))
        self.assertIn("Zero-Cost-Basis-Assumption", report.realized_gains[0].flags)
        self.assertEqual(len(report.processing_warnings), 1)
        self.assertIn(
            "may not be fully compliant with Spanish tax law",
            report.processing_warnings[0],
        )

    def test_unmatched_transfer_in_fails_by_default_after_collecting_all_inbounds(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.TRANSFER_IN,
                amount=Decimal("1"),
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                tx_id="legacy-mine-1",
                source="Ledger",
                location="Ledger Nano",
            ),
            Transaction(
                timestamp=datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.TRANSFER_IN,
                amount=Decimal("0.5"),
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                tx_id="legacy-mine-2",
                source="Ledger",
                location="Ledger Nano",
            ),
        ]

        with self.assertRaises(UnmatchedInboundTransfersError) as ctx:
            FifoEngine().process_transactions(transactions)

        self.assertEqual(len(ctx.exception.issues), 2)
        self.assertEqual(
            {issue.tx_id for issue in ctx.exception.issues},
            {"legacy-mine-1", "legacy-mine-2"},
        )

    def test_insufficient_inventory_error_includes_sale_and_open_lot_dump(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.BUY,
                amount=Decimal("1"),
                price_eur=Decimal("2000"),
                fee_eur=Decimal("10"),
                tx_id="buy-1",
                source="Kraken",
                location="spot / main",
            ),
            Transaction(
                timestamp=datetime(2025, 2, 1, 10, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.SELL,
                amount=Decimal("2"),
                price_eur=Decimal("2500"),
                fee_eur=Decimal("5"),
                tx_id="sell-1",
                source="Kraken",
                location="spot / main",
            ),
        ]

        with self.assertRaises(InsufficientInventoryError) as ctx:
            FifoEngine().process_transactions(transactions)

        message = str(ctx.exception)
        self.assertIn("Insufficient inventory for ETH: trying to sell 2, available 1.", message)
        self.assertIn("Sale transaction:", message)
        self.assertIn("tx_id=sell-1", message)
        self.assertIn("Open lots before sale:", message)
        self.assertIn("tx_id=buy-1", message)
        self.assertIn("amount_remaining=1", message)

    def test_two_month_rule_flags_loss_with_repurchase(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2024, 6, 1, 8, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.BUY,
                amount=Decimal("2"),
                price_eur=Decimal("2500"),
                fee_eur=Decimal("0"),
            ),
            Transaction(
                timestamp=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.SELL,
                amount=Decimal("1"),
                price_eur=Decimal("2000"),
                fee_eur=Decimal("0"),
            ),
            Transaction(
                timestamp=datetime(2025, 2, 20, 8, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.BUY,
                amount=Decimal("0.5"),
                price_eur=Decimal("2100"),
                fee_eur=Decimal("0"),
            ),
        ]

        report = FifoEngine().process_transactions(transactions)
        self.assertEqual(len(report.realized_gains), 1)
        self.assertIn("Wash-Sale-Warning", report.realized_gains[0].flags)

    def test_two_month_rule_can_be_disabled_while_keeping_warning(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2024, 6, 1, 8, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.BUY,
                amount=Decimal("1"),
                price_eur=Decimal("2500"),
                fee_eur=Decimal("0"),
                tx_id="buy-1",
            ),
            Transaction(
                timestamp=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.SELL,
                amount=Decimal("1"),
                price_eur=Decimal("2000"),
                fee_eur=Decimal("0"),
                tx_id="sell-1",
            ),
            Transaction(
                timestamp=datetime(2025, 2, 20, 8, 0, tzinfo=timezone.utc),
                asset="ETH",
                type=TransactionType.BUY,
                amount=Decimal("1"),
                price_eur=Decimal("2100"),
                fee_eur=Decimal("0"),
                tx_id="buy-2",
            ),
        ]

        report = FifoEngine().process_transactions(
            transactions,
            two_month_rule_mode=TwoMonthRuleMode.DISABLED,
        )
        self.assertNotIn("Wash-Sale-Warning", report.realized_gains[0].flags)
        self.assertIn("Two-Month-Rule-Disabled-Warning", report.realized_gains[0].flags)
        self.assertEqual(len(report.processing_warnings), 1)

    def test_airdrop_is_tracked_separately_from_staking_and_enters_fifo(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                asset="SOL",
                type=TransactionType.AIRDROP,
                amount=Decimal("10"),
                price_eur=Decimal("20"),
                fee_eur=Decimal("0"),
                tx_id="airdrop-1",
            ),
            Transaction(
                timestamp=datetime(2025, 2, 10, 8, 0, tzinfo=timezone.utc),
                asset="SOL",
                type=TransactionType.SELL,
                amount=Decimal("4"),
                price_eur=Decimal("25"),
                fee_eur=Decimal("2"),
                tx_id="sell-1",
            ),
        ]

        report = FifoEngine().process_transactions(transactions)
        self.assertEqual(len(report.airdrop_income), 1)
        self.assertEqual(report.airdrop_income[0].income_eur, Decimal("200"))
        self.assertEqual(len(report.staking_income), 0)
        self.assertIn("Airdrop", report.realized_gains[0].flags)

    def test_parser_uses_price_service_when_eur_price_is_missing(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "kraken.csv"
            pd.DataFrame(
                [
                    {
                        "time": "2025-01-05T10:00:00Z",
                        "asset": "BTC",
                        "type": "BUY",
                        "amount": "0.1",
                        "fee": "0.0001",
                        "fee_currency": "BTC",
                        "txid": "abc123",
                    }
                ]
            ).to_csv(csv_path, index=False)

            parsed = parse_kraken_csv(
                csv_path,
                price_service=self.price_feed,
                coin_ids={"BTC": "bitcoin"},
            )

            self.assertEqual(len(parsed), 1)
            self.assertEqual(parsed[0].price_eur, Decimal("31000"))
            self.assertEqual(parsed[0].fee_eur, Decimal("3.1000"))

    def test_permuta_trade_row_generates_sale_and_new_fifo_lot(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "trade.csv"
            pd.DataFrame(
                [
                    {
                        "time": "2025-01-01T10:00:00Z",
                        "asset": "BTC",
                        "type": "BUY",
                        "amount": "0.1",
                        "price_eur": "30000",
                        "fee_eur": "0",
                        "txid": "buy-1",
                    },
                    {
                        "time": "2025-02-01T10:00:00Z",
                        "asset": "BTC",
                        "type": "TRADE",
                        "amount": "0.1",
                        "price_eur": "40000",
                        "received_asset": "ETH",
                        "received_amount": "2",
                        "txid": "swap-1",
                    },
                ]
            ).to_csv(csv_path, index=False)

            parsed = parse_kraken_csv(csv_path)
            report = FifoEngine().process_transactions(parsed)

            self.assertEqual(len(report.realized_gains), 1)
            self.assertEqual(report.realized_gains[0].result_eur, Decimal("1000"))
            self.assertIn("Permuta", report.realized_gains[0].flags)
            self.assertIn("ETH", report.open_lots)
            self.assertEqual(report.open_lots["ETH"][0].amount_remaining, Decimal("2"))


if __name__ == "__main__":
    main()
