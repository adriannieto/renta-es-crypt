from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

import yaml

from src.app.cli import apply_cli_overrides, build_parser
from src.app.settings import load_settings


TESTS_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = TESTS_ROOT / "data" / "parsers"


class CliTestCase(TestCase):
    def run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "src", *args],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
            env=dict(os.environ),
        )

    def prime_btceur_backfill_cache(self, cache_dir: str | Path) -> None:
        cache_root = Path(cache_dir) / "cryptodatadownload"
        cache_root.mkdir(parents=True, exist_ok=True)
        csv_text = (
            "date,symbol,open,high,low,close,Volume BTC,Volume EUR\n"
            "2025-01-10 10:00:00,BTCEUR,30000,30000,30000,30000,1,30000\n"
            "2025-02-01 08:00:00,BTCEUR,32000,32000,32000,32000,1,32000\n"
            "2025-03-10 09:00:00,BTCEUR,40000,40000,40000,40000,1,40000\n"
            "2025-03-10 14:00:00,BTCEUR,40000,40000,40000,40000,1,40000\n"
        )
        now_timestamp = datetime.now(timezone.utc).timestamp()
        for exchange in ("Binance", "Bitfinex", "Bitstamp", "Gemini"):
            csv_path = cache_root / f"{exchange}_BTCEUR_1h.csv"
            csv_path.write_text(csv_text, encoding="utf-8")
            os.utime(csv_path, (now_timestamp, now_timestamp))

    def test_cli_help_reflects_supported_surface(self) -> None:
        result = self.run_cli("--help")

        self.assertIn("--backfill-provider", result.stdout)
        self.assertIn("--external-provider", result.stdout)
        self.assertIn("--ignore-asset", result.stdout)
        self.assertIn("--allow-backfill-resolution-downgrade", result.stdout)
        self.assertIn("--coingecko-api-key", result.stdout)
        self.assertIn("platform is currently kraken", result.stdout)
        self.assertNotIn("coinbase", result.stdout.lower())
        self.assertNotIn("ledger", result.stdout.lower())

    def test_cli_generates_reports_for_kraken_input(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            self.prime_btceur_backfill_cache(tmp_dir)
            expected_resolution = load_settings().pricing.resolution
            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--cache-dir",
                tmp_dir,
            )

            self.assertIn("Loading settings", result.stdout)
            self.assertIn("Effective settings:", result.stdout)
            self.assertIn("Starting tax calculations", result.stdout)
            self.assertIn("Pre-heating price cache from CryptoDataDownload", result.stdout)
            self.assertIn(
                f"Processing Kraken CSV {DATA_DIR / 'kraken_cli_sample.csv'}",
                result.stdout,
            )
            self.assertIn(
                f"Imported year 2025 from Kraken CSV {DATA_DIR / 'kraken_cli_sample.csv'}",
                result.stdout,
            )
            self.assertIn(
                f"Kraken CSV {DATA_DIR / 'kraken_cli_sample.csv'} processed",
                result.stdout,
            )
            self.assertIn("Generating reports", result.stdout)
            self.assertIn("All work done", result.stdout)
            self.assertIn("Price backfill provider: cryptodatadownload", result.stdout)
            self.assertIn("Price external provider: coingecko", result.stdout)
            self.assertIn(f"Price resolution: {expected_resolution}", result.stdout)
            self.assertIn("Ignored assets: -", result.stdout)
            self.assertIn("Allow backfill resolution downgrade: False", result.stdout)
            generated = list(Path(tmp_dir).glob("renta-es-crypt-*_modelo100.csv"))
            self.assertEqual(len(generated), 1)

    def test_apply_cli_overrides_updates_cache_and_pricing(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--cache-dir",
                "/tmp/custom-cache",
                "--ignore-asset",
                "bsv",
                "--ignore-asset",
                "bch",
                "--backfill-provider",
                "cryptodatadownload",
                "--external-provider",
                "coingecko",
                "--allow-backfill-resolution-downgrade",
                "--coingecko-api-key",
                "cg-key",
            ]
        )

        settings = load_settings()
        overridden_settings = apply_cli_overrides(args, settings)

        self.assertEqual(overridden_settings.cache_dir, "/tmp/custom-cache")
        self.assertEqual(overridden_settings.ignored_assets, ["BSV", "BCH"])
        self.assertEqual(overridden_settings.pricing.backfill_provider, "cryptodatadownload")
        self.assertEqual(overridden_settings.pricing.external_provider, "coingecko")
        self.assertTrue(overridden_settings.pricing.allow_backfill_resolution_downgrade)
        self.assertEqual(overridden_settings.pricing.coingecko.api_key, "cg-key")

    def test_cli_cache_dir_override_updates_effective_summary_path(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            cache_dir = Path(tmp_dir) / "cache-dir"
            self.prime_btceur_backfill_cache(cache_dir)
            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--cache-dir",
                str(cache_dir),
            )

            self.assertIn(
                "Price cache: in-memory",
                result.stdout,
            )
            self.assertIn(
                f"Backfill CSV cache dir: {cache_dir / 'cryptodatadownload'}",
                result.stdout,
            )
            self.assertIn(
                f"External price cache dir: {cache_dir / 'external_prices'}",
                result.stdout,
            )
            self.assertIn(
                "CLI flags override configured settings from config files where applicable.",
                result.stdout,
            )

    def test_cli_can_disable_backfill_provider(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--backfill-provider",
                "none",
                "--coingecko-api-key",
                "cg-key",
            )

            self.assertNotIn("Pre-heating price cache from CryptoDataDownload", result.stdout)
            self.assertIn("Price backfill provider: none", result.stdout)

    def test_cli_can_enable_backfill_resolution_downgrade(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            self.prime_btceur_backfill_cache(tmp_dir)
            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--cache-dir",
                tmp_dir,
                "--allow-backfill-resolution-downgrade",
            )

            self.assertIn("Allow backfill resolution downgrade: True", result.stdout)
            self.assertIn(
                "CLI flags override configured settings from config files where applicable.",
                result.stdout,
            )

    def test_cli_can_override_ignored_assets(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            self.prime_btceur_backfill_cache(tmp_dir)
            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--cache-dir",
                tmp_dir,
                "--ignore-asset",
                "BSV",
                "--ignore-asset",
                "BCH",
            )

            self.assertIn("Ignored assets: BSV, BCH", result.stdout)
            self.assertIn(
                "CLI flags override configured settings from config files where applicable.",
                result.stdout,
            )

    def test_cli_flags_override_config_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            self.prime_btceur_backfill_cache(tmp_dir)
            config_path = Path(tmp_dir) / "override.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "pricing": {
                            "resolution": "day",
                            "coingecko": {"api_key": "from-config"},
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--cache-dir",
                tmp_dir,
                "--config",
                str(config_path),
                "--price-resolution",
                "hour",
                "--coingecko-api-key",
                "from-cli",
            )

            self.assertIn(f"Settings file: {config_path}", result.stdout)
            self.assertIn("Price resolution: hour", result.stdout)
            self.assertIn(
                "CLI flags override configured settings from config files where applicable.",
                result.stdout,
            )

    def test_cli_missing_config_override_fails_fast(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "missing.yaml"

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "src",
                    "--config",
                    str(missing_path),
                    "--input",
                    f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                env=dict(os.environ),
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Config override file not found", result.stderr)
            self.assertNotIn("usage:", result.stderr)

    def test_cli_runtime_error_does_not_print_argparse_usage(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "src",
                "--input",
                "broken-input-spec",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=dict(os.environ),
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Input must use the form", result.stderr)
        self.assertNotIn("usage:", result.stderr)
        self.assertIn("Loading settings", result.stdout)
        self.assertIn("Starting tax calculations", result.stdout)

    def test_cli_output_prefix_is_generated(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            self.prime_btceur_backfill_cache(tmp_dir)
            result = self.run_cli(
                "--input",
                f"kraken:{DATA_DIR / 'kraken_cli_sample.csv'}",
                "--output-dir",
                tmp_dir,
                "--cache-dir",
                tmp_dir,
            )

            self.assertIn("Output base:", result.stdout)
            generated = [
                path for path in Path(tmp_dir).glob("renta-es-crypt-*_modelo100.csv")
            ]
            self.assertEqual(len(generated), 1)
            prefix = generated[0].name.removesuffix("_modelo100.csv")
            self.assertRegex(prefix, r"^renta-es-crypt-\d{14}$")


if __name__ == "__main__":
    main()
