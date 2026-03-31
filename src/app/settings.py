from __future__ import annotations

from copy import deepcopy
from importlib import resources
from pathlib import Path
from typing import Any, Final

import yaml
from pydantic import BaseModel, Field

from src.shared import PriceResolution


class UnmatchedTransferInMode(str):
    FAIL = "fail"
    WARN = "warn"
    ZERO_COST_BASIS = "zero_cost_basis"


class TwoMonthRuleMode(str):
    ENABLED = "enabled"
    DISABLED = "disabled"


class PriceFeedProvider(str):
    NONE = "none"
    CRYPTODATADOWNLOAD = "cryptodatadownload"
    COINGECKO = "coingecko"


class CryptoDataDownloadSettings(BaseModel):
    base_url: str = "https://www.cryptodatadownload.com/cdd"
    exchanges: list[str] = Field(default_factory=lambda: ["Binance", "Bitfinex", "Bitstamp"])
    quote_priority: list[str] = Field(default_factory=lambda: ["EUR", "USD", "USDT", "BTC"])
    symbols: dict[str, str] = Field(default_factory=dict)
    hour_suffix: str = "1h"
    day_suffix: str = "d"
    minute_suffix: str = "minute"


class CoinGeckoSettings(BaseModel):
    base_url: str = "https://api.coingecko.com/api/v3"
    api_key: str | None = None
    coin_ids: dict[str, str] = Field(default_factory=dict)


class PricingSettings(BaseModel):
    backfill_provider: str = PriceFeedProvider.CRYPTODATADOWNLOAD
    external_provider: str = PriceFeedProvider.COINGECKO
    resolution: str = PriceResolution.HOUR
    allow_backfill_resolution_downgrade: bool = False
    warn_on_low_resolution: bool = True
    cryptodatadownload: CryptoDataDownloadSettings = Field(default_factory=CryptoDataDownloadSettings)
    coingecko: CoinGeckoSettings = Field(default_factory=CoinGeckoSettings)


class ReportingSettings(BaseModel):
    timezone: str = "UTC"
    transfer_window_hours: int = 4
    transfer_amount_tolerance_pct: float = 1.0
    unmatched_transfer_in_mode: str = UnmatchedTransferInMode.FAIL
    two_month_rule_mode: str = TwoMonthRuleMode.ENABLED


class AppSettings(BaseModel):
    currency: str = "EUR"
    tax_year: int = 2025
    cache_dir: str = ".cache"
    ignored_assets: list[str] = Field(default_factory=list)
    pricing: PricingSettings = Field(default_factory=PricingSettings)
    reporting: ReportingSettings = Field(default_factory=ReportingSettings)


BUNDLED_DEFAULT_SETTINGS_RESOURCE: Final[str] = "defaults.yaml"
BUNDLED_DEFAULT_SETTINGS_LABEL: Final[str] = "src/defaults.yaml"


def read_yaml_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def read_bundled_default_settings() -> dict[str, Any]:
    bundled_path = resources.files("src").joinpath(BUNDLED_DEFAULT_SETTINGS_RESOURCE)
    if not bundled_path.is_file():
        raise FileNotFoundError(f"Bundled defaults file not found: {bundled_path}")
    return yaml.safe_load(bundled_path.read_text(encoding="utf-8")) or {}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def default_settings_payload() -> dict[str, Any]:
    bundled_settings = read_bundled_default_settings()
    if bundled_settings:
        return bundled_settings
    return AppSettings().model_dump(mode="python")


def resolve_settings_path(path: str | Path | None = None) -> Path | None:
    if path is not None:
        return Path(path).expanduser()
    return None


def load_settings(path: str | Path | None = None) -> AppSettings:
    base_payload = default_settings_payload()
    settings_path = resolve_settings_path(path)

    if settings_path is None:
        return AppSettings.model_validate(base_payload)
    if not settings_path.exists():
        raise FileNotFoundError(f"Config override file not found: {settings_path}")

    override_payload = read_yaml_file(settings_path)
    return AppSettings.model_validate(deep_merge(base_payload, override_payload))
