from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest import TestCase, main

from src.app.settings import UnmatchedTransferInMode
from src.engine import FifoEngine
from src.model import Transaction, TransactionType


SCENARIO_DIR = Path(__file__).resolve().parents[1] / "data" / "scenarios"


def load_transaction(payload: dict[str, str]) -> Transaction:
    return Transaction(
        timestamp=datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00")),
        asset=payload["asset"],
        type=TransactionType(payload["type"]),
        amount=Decimal(payload["amount"]),
        price_eur=Decimal(payload["price_eur"]),
        fee_eur=Decimal(payload["fee_eur"]),
        tx_id=payload.get("tx_id"),
        source=payload.get("source"),
        location=payload.get("location"),
    )


class ScenarioDatasetTestCase(TestCase):
    def test_all_scenarios(self) -> None:
        for scenario_path in sorted(SCENARIO_DIR.glob("*.json")):
            with self.subTest(scenario=scenario_path.stem):
                scenario = json.loads(scenario_path.read_text(encoding="utf-8"))
                transactions = [
                    load_transaction(item) for item in scenario["transactions"]
                ]
                expected = scenario["expected"]
                unmatched_transfer_in_mode = scenario.get(
                    "unmatched_transfer_in_mode"
                )

                report = FifoEngine().process_transactions(
                    transactions,
                    unmatched_transfer_in_mode=(
                        unmatched_transfer_in_mode or UnmatchedTransferInMode.FAIL
                    ),
                )

                self.assertEqual(
                    len(report.realized_gains), expected["realized_gain_count"]
                )
                self.assertEqual(
                    len(report.staking_income), expected["staking_income_count"]
                )
                self.assertEqual(
                    len(report.internal_transfers),
                    expected["internal_transfer_count"],
                )
                self.assertEqual(
                    len(report.unmatched_transfers),
                    expected["unmatched_transfer_count"],
                )

                total_gain = sum(
                    (item.result_eur for item in report.realized_gains),
                    Decimal("0"),
                )
                total_staking_income = sum(
                    (item.income_eur for item in report.staking_income),
                    Decimal("0"),
                )
                self.assertEqual(
                    total_gain.quantize(Decimal("0.01")),
                    Decimal(expected["total_gain_eur"]).quantize(Decimal("0.01")),
                )
                self.assertEqual(
                    total_staking_income.quantize(Decimal("0.01")),
                    Decimal(expected["staking_income_total_eur"]).quantize(
                        Decimal("0.01")
                    ),
                )

                for index, flags in enumerate(expected["gain_flags_by_index"]):
                    self.assertEqual(report.realized_gains[index].flags, flags)

                expected_open_lots = expected["open_lots"]
                self.assertEqual(set(report.open_lots), set(expected_open_lots))
                for asset, lots in expected_open_lots.items():
                    self.assertEqual(len(report.open_lots[asset]), len(lots))
                    for lot, expected_lot in zip(report.open_lots[asset], lots):
                        self.assertEqual(
                            lot.amount_remaining,
                            Decimal(expected_lot["amount_remaining"]),
                        )
                        self.assertEqual(
                            lot.total_cost_eur,
                            Decimal(expected_lot["total_cost_eur"]),
                        )


if __name__ == "__main__":
    main()
