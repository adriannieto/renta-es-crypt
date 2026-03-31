from __future__ import annotations

import csv
import importlib.util
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "kraken_export_anonymizer.py"
)
SCRIPT_SPEC = importlib.util.spec_from_file_location(
    "repo_kraken_export_anonymizer",
    SCRIPT_PATH,
)
assert SCRIPT_SPEC is not None
assert SCRIPT_SPEC.loader is not None
SCRIPT_MODULE = importlib.util.module_from_spec(SCRIPT_SPEC)
SCRIPT_SPEC.loader.exec_module(SCRIPT_MODULE)
kraken_export_anonymizer = SCRIPT_MODULE.kraken_export_anonymizer


class KrakenExportAnonymizerScriptTestCase(TestCase):
    def test_script_rewrites_ids_amounts_fees_and_recomputes_balances(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "kraken_ledger.csv"
            csv_path.write_text(
                (
                    '"txid","refid","time","type","subtype","aclass","subclass","asset","wallet","amount","fee","balance"\n'
                    '"real-tx-1","real-ref-1","2025-01-01 10:00:00","deposit","","currency","crypto","ETH","spot / main","1.0000","0.0000","1.0000"\n'
                    '"real-tx-2","real-ref-2","2025-01-02 10:00:00","trade","tradespot","currency","crypto","ETH","spot / main","-0.5000","0.0050","0.4950"\n'
                    '"real-tx-3","real-ref-2","2025-01-02 10:00:00","trade","tradespot","currency","crypto","BTC","spot / main","0.0200","0.0001","0.0199"\n'
                ),
                encoding="utf-8",
            )

            kraken_export_anonymizer(csv_path)

            with csv_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(rows[0]["txid"], "AAAAA-ZZZZZ-00001")
        self.assertEqual(rows[0]["refid"], "AAAAA-ZZZZZ-00002")
        self.assertEqual(rows[1]["refid"], "AAAAA-ZZZZZ-00004")
        self.assertEqual(rows[2]["refid"], "AAAAA-ZZZZZ-00004")

        self.assertNotEqual(rows[0]["amount"], "1.0000")
        self.assertNotEqual(rows[1]["fee"], "0.0050")

        eth_balance = Decimal(rows[0]["amount"]) - Decimal(rows[0]["fee"])
        self.assertEqual(Decimal(rows[0]["balance"]), eth_balance)

        eth_balance += Decimal(rows[1]["amount"]) - Decimal(rows[1]["fee"])
        self.assertEqual(Decimal(rows[1]["balance"]), eth_balance)

        btc_balance = Decimal(rows[2]["amount"]) - Decimal(rows[2]["fee"])
        self.assertEqual(Decimal(rows[2]["balance"]), btc_balance)


if __name__ == "__main__":
    main()
