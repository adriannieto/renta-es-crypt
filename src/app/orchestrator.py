"""Top-level orchestration from parsed files to engine output."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Callable

from src.app.settings import AppSettings, PriceFeedProvider
from src.engine import FifoEngine
from src.feeds.cached_price_feed import CachedPriceFeed
from src.feeds.coingecko_price_feed import CoinGeckoPriceFeed
from src.feeds.cryptodatadownload_backfill import CryptoDataDownloadBackfill
from src.feeds.price_cache import PriceCache
from src.feeds.price_feed_utils import build_low_resolution_warnings
from src.model import EngineReport, Transaction
from src.parser import PARSER_BY_PLATFORM, get_csv_parser
from src.shared import PriceFeed

BACKFILL_CSV_CACHE_DIRNAME = "cryptodatadownload"
EXTERNAL_PRICE_CACHE_DIRNAME = "external_prices"


def parse_input_specs(
    inputs: list[str],
    *,
    settings: AppSettings,
    price_feed: PriceFeed | None = None,
    price_resolution: str | None = None,
    on_input_start: Callable[[str, str], None] | None = None,
    on_input_warning: Callable[[str], None] | None = None,
) -> list[Transaction]:
    transactions: list[Transaction] = []
    resolution = price_resolution or settings.pricing.resolution
    feed = price_feed or build_price_feed(
        settings=settings,
        resolution=resolution,
    )

    for input_spec in inputs:
        platform, raw_path = split_input_spec(input_spec)
        if on_input_start is not None:
            on_input_start(platform, raw_path)
        parser = get_csv_parser(platform)
        parsed_transactions = parser.parse(
            Path(raw_path),
            price_feed=feed,
            coin_ids={},
            ignored_assets={asset.strip().upper() for asset in settings.ignored_assets},
        )
        parser_warnings = collect_parser_warnings(parser)
        if on_input_warning is not None:
            for warning in parser_warnings:
                on_input_warning(warning)
        transactions.extend(filter_ignored_assets(parsed_transactions, settings=settings))

    return sorted(transactions, key=lambda tx: tx.timestamp)


def preheat_price_cache(
    inputs: list[str],
    *,
    settings: AppSettings,
    price_feed: PriceFeed,
) -> list[dict[str, str]]:
    if not isinstance(price_feed, CachedPriceFeed):
        return []

    requests = collect_price_preheat_requests(inputs, settings=settings)
    if not requests:
        return []
    return price_feed.preheat_requests(requests)


def run_pipeline(
    transactions: list[Transaction],
    *,
    settings: AppSettings,
    unmatched_transfer_in_mode: str | None = None,
    two_month_rule_mode: str | None = None,
    price_resolution: str | None = None,
) -> EngineReport:
    resolution = price_resolution or settings.pricing.resolution
    engine = FifoEngine()
    report = engine.process_transactions(
        transactions,
        transfer_window_hours=settings.reporting.transfer_window_hours,
        transfer_amount_tolerance_pct=Decimal(str(settings.reporting.transfer_amount_tolerance_pct)),
        unmatched_transfer_in_mode=unmatched_transfer_in_mode or settings.reporting.unmatched_transfer_in_mode,
        two_month_rule_mode=two_month_rule_mode or settings.reporting.two_month_rule_mode,
    )
    report.price_backfill_provider = settings.pricing.backfill_provider
    report.price_external_provider = settings.pricing.external_provider
    report.price_resolution = resolution
    report.price_cache_path = "in-memory"
    report.backfill_csv_cache_dir = resolve_backfill_csv_cache_dir(settings)
    report.external_price_cache_dir = resolve_external_price_cache_dir(settings)
    return report


def build_price_feed(
    *,
    settings: AppSettings,
    resolution: str,
    event_logger: Callable[[str], None] | None = None,
) -> PriceFeed:
    if settings.pricing.backfill_provider not in {
        PriceFeedProvider.NONE,
        PriceFeedProvider.CRYPTODATADOWNLOAD,
    }:
        raise ValueError(
            f"Unsupported backfill provider '{settings.pricing.backfill_provider}'. "
            f"Expected one of: {[PriceFeedProvider.NONE, PriceFeedProvider.CRYPTODATADOWNLOAD]}"
        )
    if settings.pricing.external_provider != PriceFeedProvider.COINGECKO:
        raise ValueError(
            f"Unsupported external provider '{settings.pricing.external_provider}'. "
            f"Expected one of: {[PriceFeedProvider.COINGECKO]}"
        )

    csv_cache_dir = resolve_backfill_csv_cache_dir(settings)
    external_price_cache_dir = resolve_external_price_cache_dir(settings)
    price_cache = PriceCache(
        persist_dir=external_price_cache_dir,
        persist_providers={PriceFeedProvider.COINGECKO},
    )
    backfill_provider = None
    if settings.pricing.backfill_provider == PriceFeedProvider.CRYPTODATADOWNLOAD:
        backfill_provider = CryptoDataDownloadBackfill(
            price_cache=price_cache,
            csv_cache_dir=csv_cache_dir,
            base_url=settings.pricing.cryptodatadownload.base_url,
            exchanges=settings.pricing.cryptodatadownload.exchanges,
            quote_priority=settings.pricing.cryptodatadownload.quote_priority,
            symbols=settings.pricing.cryptodatadownload.symbols,
            hour_suffix=settings.pricing.cryptodatadownload.hour_suffix,
            day_suffix=settings.pricing.cryptodatadownload.day_suffix,
            minute_suffix=settings.pricing.cryptodatadownload.minute_suffix,
            allow_resolution_downgrade=settings.pricing.allow_backfill_resolution_downgrade,
            tax_year=settings.tax_year,
            resolution=resolution,
            event_logger=event_logger,
        )
    external_provider = CoinGeckoPriceFeed(
        price_cache=price_cache,
        base_url=settings.pricing.coingecko.base_url,
        api_key=settings.pricing.coingecko.api_key,
        coin_ids=settings.pricing.coingecko.coin_ids,
        resolution=resolution,
        event_logger=event_logger,
    )
    return CachedPriceFeed(
        price_cache=price_cache,
        resolution=resolution,
        backfill_provider=backfill_provider,
        external_provider=external_provider,
        event_logger=event_logger,
    )

def resolve_backfill_csv_cache_dir(settings: AppSettings) -> str:
    return str(Path(settings.cache_dir).expanduser() / BACKFILL_CSV_CACHE_DIRNAME)


def resolve_external_price_cache_dir(settings: AppSettings) -> str:
    return str(Path(settings.cache_dir).expanduser() / EXTERNAL_PRICE_CACHE_DIRNAME)


def build_pricing_warnings(
    *,
    settings: AppSettings,
    transactions: list[Transaction],
    price_resolution: str | None = None,
) -> list[str]:
    return build_low_resolution_warnings(
        transactions=transactions,
        resolution=price_resolution or settings.pricing.resolution,
        enabled=settings.pricing.warn_on_low_resolution,
    )


def collect_price_feed_warnings(price_feed: PriceFeed | None) -> list[str]:
    if price_feed is None:
        return []
    get_warnings = getattr(price_feed, "get_warnings", None)
    if callable(get_warnings):
        return list(get_warnings())
    return []


def collect_parser_warnings(parser: object) -> list[str]:
    get_warnings = getattr(parser, "get_warnings", None)
    if callable(get_warnings):
        return list(get_warnings())
    return []


def collect_price_preheat_requests(
    inputs: list[str],
    *,
    settings: AppSettings,
) -> list[tuple[str, datetime]]:
    requests: list[tuple[str, datetime]] = []
    ignored_assets = {asset.strip().upper() for asset in settings.ignored_assets}
    for input_spec in inputs:
        platform, raw_path = split_input_spec(input_spec)
        parser = get_csv_parser(platform)
        collect_requests = getattr(parser, "collect_price_preheat_requests", None)
        if not callable(collect_requests):
            continue
        requests.extend(
            collect_requests(
                Path(raw_path),
                ignored_assets=ignored_assets,
            )
        )
    return requests


def collect_input_year_markers(platform: str, raw_path: str) -> list[int]:
    parser = get_csv_parser(platform)
    collect_years = getattr(parser, "collect_input_year_markers", None)
    if not callable(collect_years):
        return []
    return list(collect_years(Path(raw_path)))


def format_preheat_summary_lines(records: list[dict[str, str]]) -> str | None:
    assets = sorted({f"{str(record['asset']).upper()}EUR" for record in records})
    if not assets:
        return None
    return ", ".join(assets)


def format_preheat_downgrade_summary_lines(records: list[dict[str, str]]) -> list[str]:
    downgraded_records = [
        record
        for record in records
        if str(record["requested_resolution"]) != str(record["used_resolution"])
    ]
    grouped: dict[tuple[str, str], list[datetime]] = {}
    for record in downgraded_records:
        asset = str(record["asset"]).upper()
        used_resolution = str(record["used_resolution"])
        grouped.setdefault((asset, used_resolution), []).append(record["requested_at"])

    lines: list[str] = []
    resolution_rank = {"day": 0, "hour": 1, "minute": 2}
    per_asset_segments: dict[str, list[tuple[str, str]]] = {}
    for (asset, used_resolution), timestamps in grouped.items():
        segments = []
        for start_at, end_at in _merge_by_calendar_step(sorted(set(timestamps)), used_resolution):
            segments.append(
                (
                    used_resolution,
                    f"({_format_summary_timestamp(start_at, used_resolution)} -> "
                    f"{_format_summary_timestamp(end_at, used_resolution)}) [{used_resolution}]",
                )
            )
        per_asset_segments.setdefault(asset, []).extend(segments)

    for asset in sorted(per_asset_segments):
        ordered_segments = [
            segment
            for _, segment in sorted(
                per_asset_segments[asset],
                key=lambda item: (resolution_rank[item[0]], item[1]),
            )
        ]
        lines.append(f"{asset}EUR: {', '.join(ordered_segments)}")
    return lines


def _extract_resolution(segment: str) -> str:
    if "[" not in segment or "]" not in segment:
        return "hour"
    return segment.rsplit("[", maxsplit=1)[-1].rstrip("]")


def _merge_contiguous_timestamps(
    timestamps: list[datetime],
    resolution: str,
) -> list[tuple[datetime, datetime]]:
    if not timestamps:
        return []

    step = _resolution_step(resolution)
    ranges: list[tuple[datetime, datetime]] = []
    range_start = timestamps[0]
    range_end = timestamps[0]

    for current in timestamps[1:]:
        if current - range_end == step:
            range_end = current
            continue
        ranges.append((range_start, range_end))
        range_start = current
        range_end = current

    ranges.append((range_start, range_end))
    return ranges


def _resolution_step(resolution: str) -> timedelta:
    if resolution == "minute":
        return timedelta(minutes=1)
    if resolution == "hour":
        return timedelta(hours=1)
    return timedelta(days=1)


def _merge_by_calendar_step(
    timestamps: list[datetime],
    resolution: str,
) -> list[tuple[datetime, datetime]]:
    if resolution != "day":
        return _merge_contiguous_timestamps(timestamps, resolution)
    if not timestamps:
        return []

    ranges: list[tuple[datetime, datetime]] = []
    range_start = timestamps[0]
    range_end = timestamps[0]
    previous_day = timestamps[0].date()

    for current in timestamps[1:]:
        current_day = current.date()
        if (current_day - previous_day).days <= 7:
            range_end = current
            previous_day = current_day
            continue
        ranges.append((range_start, range_end))
        range_start = current
        range_end = current
        previous_day = current_day

    ranges.append((range_start, range_end))
    return ranges


def _format_summary_timestamp(value: datetime, resolution: str) -> str:
    if resolution == "day":
        return value.date().isoformat()
    if resolution == "hour":
        return value.strftime("%Y-%m-%d %H:00")
    return value.strftime("%Y-%m-%d %H:%M")


def filter_ignored_assets(transactions: list[Transaction], *, settings: AppSettings) -> list[Transaction]:
    ignored_assets = {asset.strip().upper() for asset in settings.ignored_assets}
    if not ignored_assets:
        return list(transactions)

    filtered: list[Transaction] = []
    for transaction in transactions:
        if transaction.asset in ignored_assets:
            continue
        if transaction.counter_asset is not None and transaction.counter_asset.strip().upper() in ignored_assets:
            continue
        filtered.append(transaction)
    return filtered


def split_input_spec(value: str) -> tuple[str, str]:
    if ":" not in value:
        raise ValueError("Input must use the form '<platform>:<path>'.")
    platform, path = value.split(":", 1)
    normalized_platform = platform.strip().lower()
    if normalized_platform not in PARSER_BY_PLATFORM:
        raise ValueError(f"Unsupported platform '{platform}'. Expected one of: {sorted(PARSER_BY_PLATFORM)}")
    return normalized_platform, path.strip()
