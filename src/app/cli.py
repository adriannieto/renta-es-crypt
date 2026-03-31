from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from src.app.orchestrator import (
    build_price_feed,
    build_pricing_warnings,
    collect_price_feed_warnings,
    collect_input_year_markers,
    format_preheat_summary_lines,
    format_preheat_downgrade_summary_lines,
    parse_input_specs,
    preheat_price_cache,
    run_pipeline,
)
from src.app.settings import (
    BUNDLED_DEFAULT_SETTINGS_LABEL,
    PriceFeedProvider,
    TwoMonthRuleMode,
    UnmatchedTransferInMode,
    load_settings,
)
from src.engine import UnmatchedInboundTransfersError
from src.report import export_report


class SmartHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter
):
    """Formatter that keeps examples readable while also showing defaults."""


def build_examples() -> str:
    return (
        "examples:\n"
        "  renta-es-crypt --input kraken:kraken.csv\n"
        "  renta-es-crypt --config my-settings.yaml --input kraken:kraken.csv\n"
        "  renta-es-crypt --cache-dir .cache-local --input kraken:kraken.csv\n"
        "  renta-es-crypt --price-resolution hour --input kraken:kraken.csv\n"
        "  renta-es-crypt --allow-backfill-resolution-downgrade --input kraken:kraken.csv\n"
        "  renta-es-crypt --ignore-asset BSV --ignore-asset BCH --input kraken:kraken.csv\n"
        "  renta-es-crypt --backfill-provider cryptodatadownload --external-provider coingecko --input kraken:kraken.csv\n"
        "  renta-es-crypt --backfill-provider none --external-provider coingecko --input kraken:kraken.csv\n"
        "  renta-es-crypt --coingecko-api-key $CG_KEY --input kraken:kraken.csv\n"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="renta-es-crypt",
        description="Calculate Spanish crypto capital gains using FIFO for AEAT reporting.",
        formatter_class=SmartHelpFormatter,
        epilog=build_examples(),
    )
    parser.add_argument("--input", action="append", required=True, help="Input in the form '<platform>:<path>', where platform is currently kraken.")
    parser.add_argument("--output-dir", default="output", help="Directory for generated reports.")
    parser.add_argument(
        "--config",
        default=None,
        help="Optional YAML config override file. Its values are merged on top of the bundled defaults from src/defaults.yaml.",
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help="Optional directory override for shared on-disk cache artifacts, mainly the cached CryptoDataDownload CSV files.",
    )
    parser.add_argument(
        "--ignore-asset",
        action="append",
        default=None,
        help="Ignore an asset completely during parsing/orchestration. Repeat the flag to ignore multiple assets.",
    )
    parser.add_argument(
        "--backfill-provider",
        choices=[PriceFeedProvider.NONE, PriceFeedProvider.CRYPTODATADOWNLOAD],
        default=None,
        help="Backfill provider used to populate local pair cache before any external lookup is attempted.",
    )
    parser.add_argument(
        "--external-provider",
        choices=[PriceFeedProvider.COINGECKO],
        default=None,
        help="External historical provider used only when the local cache cannot resolve the required pair.",
    )
    parser.add_argument(
        "--price-resolution",
        choices=["minute", "hour", "day"],
        default=None,
        help="Temporal resolution used to bucket historical price lookups in the local cache.",
    )
    parser.add_argument(
        "--allow-backfill-resolution-downgrade",
        action="store_true",
        help="Allow CryptoDataDownload backfill to downgrade one step when the requested resolution file is missing: minute -> hour, hour -> day. Each downgraded lookup is emitted as a warning.",
    )
    parser.add_argument(
        "--coingecko-api-key",
        default=None,
        help="Optional CoinGecko API key override used by the external provider.",
    )
    parser.add_argument(
        "--unmatched-transfer-in-mode",
        choices=[UnmatchedTransferInMode.FAIL, UnmatchedTransferInMode.WARN, UnmatchedTransferInMode.ZERO_COST_BASIS],
        default=None,
        help="How to treat unmatched inbound transfers. 'fail' stops the run after listing all unmatched inbound transfers. 'warn' leaves them unmatched with a warning. 'zero_cost_basis' adds them to FIFO at zero cost, which may overstate gains.",
    )
    parser.add_argument(
        "--disable-two-month-rule",
        action="store_true",
        help="Disable 2-month-rule enforcement but still keep warnings when the situation is detected.",
    )
    parser.add_argument("--no-summary", action="store_true", help="Do not print the post-run summary block.")
    return parser


def resolve_output_path(args: argparse.Namespace) -> Path:
    return Path(args.output_dir) / build_default_report_name()


def apply_cli_overrides(args: argparse.Namespace, settings):
    overridden_settings = settings.model_copy(deep=True)
    if args.cache_dir is not None:
        overridden_settings.cache_dir = str(Path(args.cache_dir).expanduser())
    if args.ignore_asset is not None:
        overridden_settings.ignored_assets = [asset.strip().upper() for asset in args.ignore_asset]
    if args.backfill_provider is not None:
        overridden_settings.pricing.backfill_provider = args.backfill_provider
    if args.external_provider is not None:
        overridden_settings.pricing.external_provider = args.external_provider
    if args.allow_backfill_resolution_downgrade:
        overridden_settings.pricing.allow_backfill_resolution_downgrade = True
    if args.coingecko_api_key is not None:
        overridden_settings.pricing.coingecko.api_key = args.coingecko_api_key
    return overridden_settings


def build_default_report_name(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(timezone.utc)).strftime("%Y%m%d%H%M%S")
    return f"renta-es-crypt-{timestamp}"


def format_decimal(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01'))} EUR"


def format_transfer_issue(issue) -> str:
    tx_id = issue.tx_id or "-"
    source = issue.source or "-"
    location = issue.location or "-"
    return (
        f"  - {issue.asset} | {issue.timestamp.isoformat()} | amount={issue.amount} | "
        f"tx_type={issue.transaction_type} | tx_id={tx_id} | source={source} | location={location}"
    )


def print_error(message: str) -> None:
    print(f"renta-es-crypt: error: {message}", file=sys.stderr)


def print_step(message: str) -> None:
    print(message)


def build_input_start_notifier():
    def notify(platform: str, raw_path: str) -> None:
        print_step(f"Processing {platform.capitalize()} CSV {raw_path}")
        for year in collect_input_year_markers(platform, raw_path):
            print_step(f"Imported year {year} from {platform.capitalize()} CSV {raw_path}")
        print_step(f"{platform.capitalize()} CSV {raw_path} processed")

    return notify


def build_input_warning_collector(target: list[str]):
    def collect(message: str) -> None:
        target.append(message)

    return collect


def unique_in_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def print_effective_settings(settings) -> None:
    print("Effective settings:")
    print(f"  Currency: {settings.currency}")
    print(f"  Tax year: {settings.tax_year}")
    print(f"  Cache dir: {settings.cache_dir}")
    print(f"  Ignored assets: {', '.join(settings.ignored_assets) if settings.ignored_assets else '-'}")
    print(f"  Price resolution: {settings.pricing.resolution}")
    print(f"  Allow backfill resolution downgrade: {settings.pricing.allow_backfill_resolution_downgrade}")
    print(f"  Price backfill provider: {settings.pricing.backfill_provider}")
    print(f"  Price external provider: {settings.pricing.external_provider}")
    print(f"  Unmatched inbound mode: {settings.reporting.unmatched_transfer_in_mode}")
    print(f"  Two-month rule mode: {settings.reporting.two_month_rule_mode}")


def print_summary(args: argparse.Namespace, settings_path: str, report, output_path: Path, effective_settings) -> None:
    total_gain = sum((item.result_eur for item in report.realized_gains), Decimal("0"))
    total_staking_income = sum((item.income_eur for item in report.staking_income), Decimal("0"))
    total_airdrop_income = sum((item.income_eur for item in report.airdrop_income), Decimal("0"))

    print("Summary:")
    print(f"  Settings file: {settings_path}")
    print(f"  Output base: {output_path}")
    print(f"  Price backfill provider: {report.price_backfill_provider}")
    print(f"  Price external provider: {report.price_external_provider}")
    print(f"  Price resolution: {report.price_resolution}")
    print(
        "  Allow backfill resolution downgrade: "
        f"{effective_settings.pricing.allow_backfill_resolution_downgrade}"
    )
    print(f"  Price cache: {report.price_cache_path}")
    print(f"  Backfill CSV cache dir: {report.backfill_csv_cache_dir}")
    print(f"  External price cache dir: {report.external_price_cache_dir}")
    print(f"  Realized gain rows: {len(report.realized_gains)}")
    print(f"  Total gains/losses: {format_decimal(total_gain)}")
    print(f"  Staking income rows: {len(report.staking_income)}")
    print(f"  Total staking income: {format_decimal(total_staking_income)}")
    print(f"  Airdrop income rows: {len(report.airdrop_income)}")
    print(f"  Total airdrop income: {format_decimal(total_airdrop_income)}")
    print(f"  Internal transfers matched: {len(report.internal_transfers)}")
    print(f"  Unmatched transfers: {len(report.unmatched_transfers)}")
    print(f"  Processing warnings: {len(report.processing_warnings)}")
    print(f"  Unmatched inbound mode: {args.unmatched_transfer_in_mode or 'settings default'}")
    print("  Two-month rule mode: " f"{TwoMonthRuleMode.DISABLED if args.disable_two_month_rule else 'settings default'}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings_path = Path(args.config).expanduser() if args.config else BUNDLED_DEFAULT_SETTINGS_LABEL
    cli_warnings: list[str] = []

    print_step("Loading settings")
    try:
        settings = load_settings(args.config)
    except FileNotFoundError as exc:
        print_error(str(exc))
        return 1

    if (
        args.backfill_provider is not None
        or args.external_provider is not None
        or args.cache_dir is not None
        or args.ignore_asset is not None
        or args.price_resolution is not None
        or args.allow_backfill_resolution_downgrade
        or args.coingecko_api_key is not None
        or args.unmatched_transfer_in_mode is not None
        or args.disable_two_month_rule
    ):
        cli_warnings.append("CLI flags override configured settings from config files where applicable.")

    try:
        effective_settings = apply_cli_overrides(args, settings)
        print_effective_settings(effective_settings)
        resolution = args.price_resolution or effective_settings.pricing.resolution
        print_step("Starting tax calculations")
        price_feed = build_price_feed(
            settings=effective_settings,
            resolution=resolution,
            event_logger=print_step,
        )
        if effective_settings.pricing.backfill_provider != PriceFeedProvider.NONE:
            print_step("Pre-heating price cache from CryptoDataDownload")
            preheat_records = preheat_price_cache(
                args.input,
                settings=effective_settings,
                price_feed=price_feed,
            )
            populated_summary = format_preheat_summary_lines(preheat_records)
            if populated_summary:
                print(f"Tickers populated: {populated_summary}")
            downgrade_summary_lines = format_preheat_downgrade_summary_lines(preheat_records)
            if downgrade_summary_lines:
                print("Tickers downgraded:")
                for line in downgrade_summary_lines:
                    print(line)
            cli_warnings.extend(collect_price_feed_warnings(price_feed))
        transactions = parse_input_specs(
            args.input,
            settings=effective_settings,
            price_feed=price_feed,
            price_resolution=args.price_resolution,
            on_input_start=build_input_start_notifier(),
            on_input_warning=build_input_warning_collector(cli_warnings),
        )
    except ValueError as exc:
        print_error(str(exc))
        return 1
    output_path = resolve_output_path(args)
    try:
        print_step("Processing transactions")
        report = run_pipeline(
            transactions,
            settings=effective_settings,
            unmatched_transfer_in_mode=args.unmatched_transfer_in_mode,
            two_month_rule_mode=(TwoMonthRuleMode.DISABLED if args.disable_two_month_rule else None),
            price_resolution=args.price_resolution,
        )
    except UnmatchedInboundTransfersError as exc:
        print("Blocking unmatched inbound transfers:")
        for issue in exc.issues:
            print(format_transfer_issue(issue))
        print("Review those records, provide the missing transfer pair, or rerun with --unmatched-transfer-in-mode warn / zero_cost_basis if you accept that override.")
        return 1
    except ValueError as exc:
        print_error(str(exc))
        return 1

    cli_warnings.extend(
        build_pricing_warnings(
            settings=effective_settings,
            transactions=transactions,
            price_resolution=args.price_resolution,
        )
    )
    cli_warnings.extend(collect_price_feed_warnings(price_feed))

    print_step("Generating reports")
    written_paths = export_report(report, output_path, tax_year=settings.tax_year)
    print_step("All work done")

    if not args.no_summary:
        print_summary(args, str(settings_path), report, output_path, effective_settings)

    print("Generated:")
    for path in written_paths:
        print(path)

    all_warnings = unique_in_order([*cli_warnings, *report.processing_warnings])
    if all_warnings:
        print("Warnings:")
        for warning in all_warnings:
            print(warning)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
