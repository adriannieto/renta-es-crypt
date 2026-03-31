from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from src.engine import FifoEngine
from src.model import Transaction, TransactionType
from src.report import (
    build_airdrop_income_frame,
    build_internal_transfer_frame,
    build_modelo100_frame,
    build_staking_income_frame,
    build_unmatched_transfer_frame,
    build_warning_frame,
    export_report,
)


class CsvReporterTestCase(TestCase):
    def test_export_report_writes_modelo100_and_supporting_outputs(self) -> None:
        transactions = [
            Transaction(
                timestamp=datetime(2024, 12, 1, 8, 0, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.BUY,
                amount=Decimal("1"),
                price_eur=Decimal("25000"),
                fee_eur=Decimal("10"),
            ),
            Transaction(
                timestamp=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.STAKE_REWARD,
                amount=Decimal("0.1"),
                price_eur=Decimal("30000"),
                fee_eur=Decimal("0"),
            ),
            Transaction(
                timestamp=datetime(2025, 2, 10, 8, 0, tzinfo=timezone.utc),
                asset="BTC",
                type=TransactionType.SELL,
                amount=Decimal("0.5"),
                price_eur=Decimal("35000"),
                fee_eur=Decimal("5"),
            ),
        ]
        report = FifoEngine().process_transactions(transactions)

        modelo100 = build_modelo100_frame(report, tax_year=2025)
        staking = build_staking_income_frame(report, tax_year=2025)
        airdrops = build_airdrop_income_frame(report, tax_year=2025)
        transfers = build_internal_transfer_frame(report)
        unmatched = build_unmatched_transfer_frame(report)
        warnings = build_warning_frame(report)

        self.assertEqual(len(modelo100), 1)
        self.assertEqual(len(staking), 1)
        self.assertEqual(len(airdrops), 0)
        self.assertEqual(len(transfers), 0)
        self.assertEqual(len(unmatched), 0)
        self.assertEqual(len(warnings), 0)

        with TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "report"
            written = export_report(report, output_path, tax_year=2025)

            self.assertEqual(len(written), 6)
            self.assertTrue((Path(tmp_dir) / "report_modelo100.csv").exists())
            self.assertTrue((Path(tmp_dir) / "report_staking_income.csv").exists())
            self.assertTrue((Path(tmp_dir) / "report_airdrop_income.csv").exists())
            self.assertTrue((Path(tmp_dir) / "report_internal_transfers.csv").exists())
            self.assertTrue((Path(tmp_dir) / "report_unmatched_transfers.csv").exists())
            self.assertTrue((Path(tmp_dir) / "report_warnings.csv").exists())


if __name__ == "__main__":
    main()
