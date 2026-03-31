from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

import yaml

from src.app.settings import (
    BUNDLED_DEFAULT_SETTINGS_LABEL,
    PriceFeedProvider,
    UnmatchedTransferInMode,
    load_settings,
)


class SettingsTestCase(TestCase):
    def test_load_settings_without_override_uses_repo_defaults(self) -> None:
        settings = load_settings()

        self.assertEqual(settings.currency, "EUR")
        self.assertEqual(settings.tax_year, 2025)
        self.assertEqual(settings.cache_dir, ".cache")
        self.assertEqual(settings.ignored_assets, [])
        self.assertEqual(settings.pricing.backfill_provider, PriceFeedProvider.CRYPTODATADOWNLOAD)
        self.assertEqual(settings.pricing.external_provider, PriceFeedProvider.COINGECKO)
        self.assertIn(settings.pricing.resolution, {"minute", "hour", "day"})
        self.assertFalse(settings.pricing.allow_backfill_resolution_downgrade)
        self.assertEqual(settings.pricing.cryptodatadownload.base_url, "https://www.cryptodatadownload.com/cdd")
        self.assertEqual(settings.pricing.cryptodatadownload.symbols["BTC"], "BTC")
        self.assertEqual(settings.pricing.coingecko.coin_ids["BTC"], "bitcoin")
        self.assertEqual(
            settings.reporting.unmatched_transfer_in_mode,
            UnmatchedTransferInMode.FAIL,
        )

    def test_config_override_merges_repo_defaults(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            settings_path = Path(tmp_dir) / "override.yaml"

            settings_path.write_text(
                yaml.safe_dump(
                    {
                        "tax_year": 2026,
                        "ignored_assets": ["BSV"],
                        "reporting": {
                            "transfer_window_hours": 12,
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            settings = load_settings(settings_path)

            self.assertEqual(settings.tax_year, 2026)
            self.assertEqual(settings.ignored_assets, ["BSV"])
            self.assertEqual(settings.reporting.transfer_window_hours, 12)
            self.assertEqual(settings.currency, "EUR")

    def test_missing_config_override_raises_file_not_found(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            missing_path = Path(tmp_dir) / "missing.yaml"

            with self.assertRaises(FileNotFoundError):
                load_settings(missing_path)

    def test_load_settings_uses_bundled_defaults(self) -> None:
        settings = load_settings()

        self.assertEqual(settings.currency, "EUR")
        self.assertEqual(settings.cache_dir, ".cache")
        self.assertEqual(settings.pricing.cryptodatadownload.quote_priority, ["EUR", "USD", "USDT", "USDC", "BTC"])
        self.assertEqual(settings.pricing.cryptodatadownload.hour_suffix, "1h")
        self.assertEqual(settings.pricing.cryptodatadownload.minute_suffix, "minute")
        self.assertFalse(settings.pricing.allow_backfill_resolution_downgrade)
        self.assertEqual(
            settings.reporting.unmatched_transfer_in_mode,
            UnmatchedTransferInMode.FAIL,
        )

    def test_bundled_default_label_points_to_documented_defaults(self) -> None:
        self.assertEqual(BUNDLED_DEFAULT_SETTINGS_LABEL, "src/defaults.yaml")


if __name__ == "__main__":
    main()
