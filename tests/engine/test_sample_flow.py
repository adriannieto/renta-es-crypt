from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase, main

from src.engine import FifoEngine
from src.model import Transaction, TransactionType


class SampleFlowTestCase(TestCase):
    def test_buy_stake_reward_sell_flow(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2025, 1, 10, 10, 0, tzinfo=timezone.utc),
                asset="btc",
                type=TransactionType.BUY,
                amount=Decimal("1.0"),
                price_eur=Decimal("30000"),
                fee_eur=Decimal("100"),
                source="Kraken",
            ),
            Transaction(
                timestamp=datetime(2025, 2, 15, 12, 0, tzinfo=timezone.utc),
                asset="btc",
                type=TransactionType.STAKE_REWARD,
                amount=Decimal("0.1"),
                price_eur=Decimal("35000"),
                fee_eur=Decimal("0"),
                source="Ledger",
            ),
            Transaction(
                timestamp=datetime(2025, 3, 10, 14, 0, tzinfo=timezone.utc),
                asset="btc",
                type=TransactionType.SELL,
                amount=Decimal("1.05"),
                price_eur=Decimal("40000"),
                fee_eur=Decimal("50"),
                source="Coinbase",
            ),
        ]

        report = FifoEngine().process_transactions(transactions)

        self.assertEqual(len(report.staking_income), 1)
        self.assertEqual(report.staking_income[0].income_eur, Decimal("3500.0"))

        total_gain = sum(item.result_eur for item in report.realized_gains)
        self.assertEqual(total_gain.quantize(Decimal("0.01")), Decimal("10100.00"))

        self.assertEqual(len(report.realized_gains), 2)
        self.assertEqual(report.realized_gains[0].source_lot_type, TransactionType.BUY)
        self.assertEqual(
            report.realized_gains[1].source_lot_type, TransactionType.STAKE_REWARD
        )
        self.assertIn("Staking", report.realized_gains[1].flags)

        self.assertIn("BTC", report.open_lots)
        remaining_lot = report.open_lots["BTC"][0]
        self.assertEqual(remaining_lot.amount_remaining, Decimal("0.05"))
        self.assertEqual(remaining_lot.total_cost_eur, Decimal("3500.0"))


if __name__ == "__main__":
    main()
