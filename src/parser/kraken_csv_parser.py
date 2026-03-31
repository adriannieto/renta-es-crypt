from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import re
from typing import Any

import pandas as pd

from src.model import Transaction, TransactionType
from src.parser.common import (
    BRIDGE_ASSETS,
    build_transactions_from_frame,
    convert_asset_amount_to_eur,
    convert_fiat_amount_to_eur,
    is_fiat_asset,
    normalize_headers,
    parse_decimal,
    parse_timestamp,
    read_csv,
    resolve_fee_eur,
    resolve_price_eur,
    resolve_trade_total_eur,
)
from src.shared import PriceFeed


KRAKEN_COLUMN_ALIASES = {
    "timestamp": ["time", "timestamp", "date"],
    "asset": ["asset", "currency", "pair_base"],
    "type": ["type", "transaction_type", "side"],
    "amount": ["amount", "vol", "quantity"],
    "price_eur": ["price_eur"],
    "total_eur": ["cost_eur", "total_eur", "value_eur"],
    "received_asset": ["received_asset", "to_asset", "buy_asset", "pair_quote"],
    "received_amount": ["received_amount", "to_amount", "buy_amount"],
    "received_price_eur": ["received_price_eur"],
    "received_total_eur": ["received_total_eur", "to_total_eur"],
    "fee_eur": ["fee_eur"],
    "fee": ["fee"],
    "fee_currency": ["fee_currency"],
    "location": ["location", "wallet"],
    "tx_id": ["txid", "refid", "trade_id"],
    "notes": ["notes", "memo"],
}

KRAKEN_LEDGER_REQUIRED_COLUMNS = {
    "txid",
    "refid",
    "time",
    "type",
    "subtype",
    "asset",
    "wallet",
    "amount",
    "fee",
    "balance",
}

KRAKEN_ASSET_ALIASES = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XDG": "DOGE",
    "XXDG": "DOGE",
    "XETH": "ETH",
    "ETH2": "ETH",
    "ZEUR": "EUR",
    "ZUSD": "USD",
    "ZGBP": "GBP",
    "ZCAD": "CAD",
    "ZAUD": "AUD",
    "ZCHF": "CHF",
    "ZJPY": "JPY",
}

KRAKEN_INTERNAL_TRANSFER_SUBTYPES = {
    "allocation",
    "deallocation",
    "autoallocate",
    "migration",
    "spottostaking",
    "stakingfromspot",
    "stakingtospot",
    "spotfromstaking",
    "spottofutures",
    "spotfromfutures",
}
KRAKEN_GROUPED_TYPES = {"trade", "adjustment", "spend", "receive"}
KRAKEN_EARN_INTERNAL_SUBTYPES = {"migration", "allocation", "deallocation", "autoallocation"}
KRAKEN_BALANCE_SUFFIXES = {"S", "M", "B", "F", "P", "T"}


class KrakenCsvParser:
    source = "Kraken"

    def __init__(self) -> None:
        self._warnings: list[str] = []

    def parse(
        self,
        path: str | Path,
        *,
        price_feed: PriceFeed | None = None,
        coin_ids: dict[str, str] | None = None,
        ignored_assets: set[str] | None = None,
    ) -> list[Transaction]:
        self._warnings = []
        frame = read_csv(path)
        normalized = normalize_headers(frame)

        if is_kraken_ledger_frame(normalized):
            warning_counts: dict[str, int] = defaultdict(int)
            transactions = parse_kraken_ledger_frame(
                normalized,
                price_feed=price_feed,
                coin_ids=coin_ids or {},
                ignored_assets=ignored_assets or set(),
                warning_counts=warning_counts,
            )
            self._warnings = build_kraken_parser_warnings(warning_counts)
            return transactions

        return build_transactions_from_frame(
            frame,
            source=self.source,
            price_feed=price_feed,
            coin_ids=coin_ids or {},
            column_aliases=KRAKEN_COLUMN_ALIASES,
            ignored_assets=ignored_assets or set(),
        )

    def get_warnings(self) -> list[str]:
        return list(self._warnings)

    def collect_price_preheat_requests(
        self,
        path: str | Path,
        *,
        ignored_assets: set[str] | None = None,
    ) -> list[tuple[str, Any]]:
        frame = normalize_headers(read_csv(path))
        if not is_kraken_ledger_frame(frame):
            return []
        return collect_kraken_price_preheat_requests(
            frame,
            ignored_assets=ignored_assets or set(),
        )

    def collect_input_year_markers(self, path: str | Path) -> list[int]:
        frame = normalize_headers(read_csv(path))
        if "time" not in frame.columns:
            return []
        years: list[int] = []
        seen: set[int] = set()
        for value in frame["time"]:
            year = parse_timestamp(value).year
            if year in seen:
                continue
            seen.add(year)
            years.append(year)
        return years


def parse_kraken_csv(
    path: str | Path,
    *,
    price_service: PriceFeed | None = None,
    coin_ids: dict[str, str] | None = None,
    ignored_assets: set[str] | None = None,
) -> list[Transaction]:
    return KrakenCsvParser().parse(path, price_feed=price_service, coin_ids=coin_ids, ignored_assets=ignored_assets)


def is_kraken_ledger_frame(frame: pd.DataFrame) -> bool:
    return KRAKEN_LEDGER_REQUIRED_COLUMNS.issubset(set(frame.columns))


def parse_kraken_ledger_frame(
    frame: pd.DataFrame,
    *,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
    ignored_assets: set[str],
    warning_counts: dict[str, int],
) -> list[Transaction]:
    transactions: list[Transaction] = []
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for _, series in frame.iterrows():
        row = series.to_dict()
        entry_type = normalize_text(row.get("type"))
        refid = normalize_optional_text(row.get("refid"))

        if refid and is_kraken_grouped_internal_earn_row(row):
            grouped_rows[refid].append(row)
            continue

        if refid and entry_type in KRAKEN_GROUPED_TYPES:
            grouped_rows[refid].append(row)
            continue

        transactions.extend(
            build_transactions_from_kraken_row(
                row,
                price_feed=price_feed,
                coin_ids=coin_ids,
                ignored_assets=ignored_assets,
                warning_counts=warning_counts,
            )
        )

    for refid, rows in grouped_rows.items():
        transactions.extend(
            build_transactions_from_kraken_group(
                rows,
                refid=refid,
                price_feed=price_feed,
                coin_ids=coin_ids,
                ignored_assets=ignored_assets,
                warning_counts=warning_counts,
            )
        )

    return sorted(transactions, key=lambda tx: tx.timestamp)


def collect_kraken_price_preheat_requests(
    frame: pd.DataFrame,
    *,
    ignored_assets: set[str],
) -> list[tuple[str, Any]]:
    requests: list[tuple[str, Any]] = []
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for _, series in frame.iterrows():
        row = series.to_dict()
        entry_type = normalize_text(row.get("type"))
        refid = normalize_optional_text(row.get("refid"))

        if refid and (entry_type in KRAKEN_GROUPED_TYPES or is_kraken_grouped_internal_earn_row(row)):
            grouped_rows[refid].append(row)
            continue

        requests.extend(
            build_kraken_price_preheat_requests_from_row(
                row,
                ignored_assets=ignored_assets,
            )
        )

    for rows in grouped_rows.values():
        requests.extend(
            build_kraken_price_preheat_requests_from_group(
                rows,
                ignored_assets=ignored_assets,
            )
        )

    return requests


def build_kraken_price_preheat_requests_from_group(
    rows: list[dict[str, Any]],
    *,
    ignored_assets: set[str],
) -> list[tuple[str, Any]]:
    if is_kraken_internal_earn_group(rows):
        return []

    non_zero_rows = [row for row in rows if parse_decimal(row.get("amount")) != 0]
    negative_rows = [row for row in non_zero_rows if parse_decimal(row.get("amount")) < 0]
    positive_rows = [row for row in non_zero_rows if parse_decimal(row.get("amount")) > 0]
    if len(negative_rows) != 1 or len(positive_rows) != 1:
        return []

    debit_row = negative_rows[0]
    credit_row = positive_rows[0]
    sold_asset = normalize_kraken_asset_for_row(debit_row)
    bought_asset = normalize_kraken_asset_for_row(credit_row)
    if sold_asset in ignored_assets or bought_asset in ignored_assets:
        return []
    if is_fiat_asset(sold_asset) and is_fiat_asset(bought_asset):
        return []

    timestamp = parse_timestamp(debit_row["time"])
    if is_fiat_asset(sold_asset):
        return [] if sold_asset == "EUR" else [(sold_asset, timestamp)]
    if is_fiat_asset(bought_asset):
        return [] if bought_asset == "EUR" else [(bought_asset, timestamp)]
    if bought_asset in BRIDGE_ASSETS:
        return [(bought_asset, timestamp)]
    return [(sold_asset, timestamp)]


def build_kraken_price_preheat_requests_from_row(
    row: dict[str, Any],
    *,
    ignored_assets: set[str],
) -> list[tuple[str, Any]]:
    entry_type = normalize_text(row.get("type"))
    subtype = normalize_text(row.get("subtype"))
    subclass = normalize_text(row.get("subclass"))
    asset = normalize_kraken_asset_for_row(row)
    if asset in ignored_assets or is_fiat_asset(asset):
        return []
    if entry_type == "margin":
        return []
    if entry_type == "transfer" and subtype in KRAKEN_INTERNAL_TRANSFER_SUBTYPES:
        return []
    if entry_type == "earn" and subtype in KRAKEN_EARN_INTERNAL_SUBTYPES:
        return []

    amount = parse_decimal(row.get("amount"))
    if is_kraken_staking_release(
        entry_type=entry_type,
        subtype=subtype,
        subclass=subclass,
        amount=amount,
    ):
        return []

    needs_price = (
        is_kraken_staking_reward(entry_type=entry_type, subtype=subtype, amount=amount)
        or is_kraken_airdrop(entry_type=entry_type, subtype=subtype, amount=amount, asset=asset)
        or (entry_type == "transfer" and subtype == "" and amount > 0)
    )
    if not needs_price:
        return []

    return [(asset, parse_timestamp(row["time"]))]


def build_transactions_from_kraken_group(
    rows: list[dict[str, Any]],
    *,
    refid: str,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
    ignored_assets: set[str],
    warning_counts: dict[str, int],
) -> list[Transaction]:
    if is_kraken_internal_earn_group(rows):
        return build_kraken_internal_earn_group_transactions(rows, refid=refid, ignored_assets=ignored_assets)

    if is_kraken_dust_sweeping_group(rows):
        warning_counts["dust_sweeping_groups_skipped"] += 1
        return []

    non_zero_rows = collapse_kraken_group_rows(rows)
    negative_rows = [row for row in non_zero_rows if parse_decimal(row.get("amount")) < 0]
    positive_rows = [row for row in non_zero_rows if parse_decimal(row.get("amount")) > 0]

    if len(negative_rows) != 1 or len(positive_rows) != 1:
        warning_counts["incomplete_group_rows_skipped"] += 1
        return []

    debit_row = negative_rows[0]
    credit_row = positive_rows[0]
    sold_asset = normalize_kraken_asset_for_row(debit_row)
    bought_asset = normalize_kraken_asset_for_row(credit_row)
    if normalize_text(debit_row.get("type")) == "margin" or normalize_text(credit_row.get("type")) == "margin":
        warning_counts["margin_rows_skipped"] += len(rows)
        return []
    if sold_asset in ignored_assets or bought_asset in ignored_assets:
        return []
    sold_amount = abs(parse_decimal(debit_row["amount"]))
    bought_amount = abs(parse_decimal(credit_row["amount"]))
    timestamp = parse_timestamp(debit_row["time"])
    total_fee_eur = resolve_kraken_fee_eur(
        debit_row,
        asset=sold_asset,
        timestamp=timestamp,
        price_feed=price_feed,
        coin_ids=coin_ids,
    ) + resolve_kraken_fee_eur(
        credit_row,
        asset=bought_asset,
        timestamp=timestamp,
        price_feed=price_feed,
        coin_ids=coin_ids,
    )

    if is_fiat_asset(sold_asset) and is_fiat_asset(bought_asset):
        return []
    if is_fiat_asset(sold_asset):
        acquisition_total_eur = convert_kraken_fiat_amount_to_eur(
            asset=sold_asset,
            amount=sold_amount,
            timestamp=timestamp,
            price_feed=price_feed,
        )
        return [
            Transaction(
                timestamp=timestamp,
                asset=bought_asset,
                type=TransactionType.BUY,
                amount=bought_amount,
                price_eur=acquisition_total_eur / bought_amount,
                fee_eur=total_fee_eur,
                source="Kraken",
                location=normalize_optional_text(credit_row.get("wallet")) or "Kraken",
                tx_id=normalize_optional_text(credit_row.get("txid")) or refid,
                pair_id=refid,
                counter_asset=sold_asset,
                counter_amount=sold_amount,
                flags=["Kraken-Ledger"],
            )
        ]
    if is_fiat_asset(bought_asset):
        transmission_total_eur = convert_kraken_fiat_amount_to_eur(
            asset=bought_asset,
            amount=bought_amount,
            timestamp=timestamp,
            price_feed=price_feed,
        )
        return [
            Transaction(
                timestamp=timestamp,
                asset=sold_asset,
                type=TransactionType.SELL,
                amount=sold_amount,
                price_eur=transmission_total_eur / sold_amount,
                fee_eur=total_fee_eur,
                source="Kraken",
                location=normalize_optional_text(debit_row.get("wallet")) or "Kraken",
                tx_id=normalize_optional_text(debit_row.get("txid")) or refid,
                pair_id=refid,
                counter_asset=bought_asset,
                counter_amount=bought_amount,
                flags=["Kraken-Ledger"],
            )
        ]

    sold_total_eur = resolve_trade_total_eur(
        sold_asset=sold_asset,
        sold_amount=sold_amount,
        received_asset=bought_asset,
        received_amount=bought_amount,
        timestamp=timestamp,
        row=debit_row,
        price_feed=price_feed,
        coin_ids=coin_ids,
    )
    disposal_total_eur = sold_total_eur
    bought_price_eur = disposal_total_eur / bought_amount

    return [
        Transaction(
            timestamp=timestamp,
            asset=sold_asset,
            type=TransactionType.SELL,
            amount=sold_amount,
            price_eur=disposal_total_eur / sold_amount,
            fee_eur=total_fee_eur,
            source="Kraken",
            location=normalize_optional_text(debit_row.get("wallet")) or "Kraken",
            tx_id=normalize_optional_text(debit_row.get("txid")) or refid,
            pair_id=refid,
            counter_asset=bought_asset,
            counter_amount=bought_amount,
            counter_price_eur=bought_price_eur,
            flags=["Permuta", "Kraken-Ledger"],
        ),
        Transaction(
            timestamp=timestamp,
            asset=bought_asset,
            type=TransactionType.BUY,
            amount=bought_amount,
            price_eur=disposal_total_eur / bought_amount,
            fee_eur=Decimal("0"),
            source="Kraken",
            location=normalize_optional_text(credit_row.get("wallet")) or "Kraken",
            tx_id=normalize_optional_text(credit_row.get("txid")) or refid,
            pair_id=refid,
            counter_asset=sold_asset,
            counter_amount=sold_amount,
            counter_price_eur=disposal_total_eur / sold_amount,
            flags=["Permuta", "Kraken-Ledger"],
        ),
    ]


def build_transactions_from_kraken_row(
    row: dict[str, Any],
    *,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
    ignored_assets: set[str],
    warning_counts: dict[str, int],
) -> list[Transaction]:
    entry_type = normalize_text(row.get("type"))
    subtype = normalize_text(row.get("subtype"))
    amount = parse_decimal(row.get("amount"))

    if amount == 0:
        return []

    asset = normalize_kraken_asset_for_row(row)
    if asset in ignored_assets:
        return []
    timestamp = parse_timestamp(row["time"])
    tx_id = normalize_optional_text(row.get("txid")) or normalize_optional_text(row.get("refid"))
    location = normalize_optional_text(row.get("wallet")) or "Kraken"
    abs_amount = abs(amount)
    subclass = normalize_text(row.get("subclass"))

    if entry_type == "margin":
        warning_counts["margin_rows_skipped"] += 1
        return []

    if is_kraken_staking_release(entry_type=entry_type, subtype=subtype, subclass=subclass, amount=amount):
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.INTERNAL_TRANSFER,
                amount=abs_amount,
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=["Internal-Transfer", "Kraken-Ledger", "Kraken-Staking-Release"],
            )
        ]

    if is_kraken_staking_reward(entry_type=entry_type, subtype=subtype, amount=amount):
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.STAKE_REWARD,
                amount=abs_amount,
                price_eur=resolve_kraken_price_eur(
                    row,
                    asset=asset,
                    amount=abs_amount,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                fee_eur=resolve_kraken_fee_eur(
                    row,
                    asset=asset,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=["Kraken-Ledger"],
            )
        ]

    if is_kraken_airdrop(entry_type=entry_type, subtype=subtype, amount=amount, asset=asset):
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.AIRDROP,
                amount=abs_amount,
                price_eur=resolve_kraken_price_eur(
                    row,
                    asset=asset,
                    amount=abs_amount,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                fee_eur=resolve_kraken_fee_eur(
                    row,
                    asset=asset,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=build_kraken_airdrop_flags(entry_type=entry_type),
            )
        ]

    if entry_type == "transfer" and subtype in KRAKEN_INTERNAL_TRANSFER_SUBTYPES:
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.INTERNAL_TRANSFER,
                amount=abs_amount,
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=["Internal-Transfer", "Kraken-Ledger"],
            )
        ]

    if entry_type == "transfer" and subtype == "delistingconversion":
        warning_counts["delisting_conversion_rows_skipped"] += 1
        return []

    if entry_type == "deposit" and amount > 0 and not is_fiat_asset(asset):
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.TRANSFER_IN,
                amount=abs_amount,
                price_eur=resolve_kraken_price_eur(
                    row,
                    asset=asset,
                    amount=abs_amount,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                fee_eur=Decimal("0"),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=["Kraken-Ledger"],
            )
        ]

    if entry_type == "withdrawal" and amount < 0 and not is_fiat_asset(asset):
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.TRANSFER_OUT,
                amount=abs_amount,
                price_eur=resolve_kraken_price_eur(
                    row,
                    asset=asset,
                    amount=abs_amount,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                fee_eur=resolve_kraken_fee_eur(
                    row,
                    asset=asset,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=["Kraken-Ledger"],
            )
        ]

    if entry_type == "transfer" and amount > 0 and not is_fiat_asset(asset):
        return [
            Transaction(
                timestamp=timestamp,
                asset=asset,
                type=TransactionType.AIRDROP,
                amount=abs_amount,
                price_eur=resolve_kraken_price_eur(
                    row,
                    asset=asset,
                    amount=abs_amount,
                    timestamp=timestamp,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                ),
                fee_eur=Decimal("0"),
                source="Kraken",
                location=location,
                tx_id=tx_id,
                flags=["Kraken-Ledger", "Kraken-Transfer-Credit"],
            )
        ]

    if is_fiat_asset(asset):
        return []

    raise ValueError(
        f"Unsupported Kraken ledger row type/subtype combination: type={entry_type!r}, subtype={subtype!r}, asset={asset!r}"
    )


def collapse_kraken_group_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    collapsed: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        amount = parse_decimal(row.get("amount"))
        if amount == 0:
            continue
        asset = normalize_kraken_asset_for_row(row)
        sign = 1 if amount > 0 else -1
        key = (asset, sign)
        if key not in collapsed:
            collapsed[key] = dict(row)
            collapsed[key]["asset"] = asset
            collapsed[key]["amount"] = amount
            collapsed[key]["fee"] = parse_decimal(row.get("fee"))
            continue
        collapsed[key]["amount"] = parse_decimal(collapsed[key]["amount"]) + amount
        collapsed[key]["fee"] = parse_decimal(collapsed[key].get("fee")) + parse_decimal(row.get("fee"))

    return list(collapsed.values())


def normalize_kraken_asset(raw_value: Any) -> str:
    asset = str(raw_value).strip().upper()
    if "." in asset:
        asset = asset.split(".", maxsplit=1)[0]
    return KRAKEN_ASSET_ALIASES.get(asset, asset)


def normalize_kraken_asset_for_row(row: dict[str, Any]) -> str:
    raw_asset = row["asset"]
    if is_kraken_earn_staking_context(row):
        return normalize_kraken_earn_staking_asset(raw_asset)
    return normalize_kraken_asset(raw_asset)


def normalize_kraken_earn_staking_asset(raw_value: Any) -> str:
    asset = str(raw_value).strip().upper()
    if "." in asset:
        base_asset, suffix = asset.split(".", maxsplit=1)
        if suffix in KRAKEN_BALANCE_SUFFIXES:
            asset = base_asset
        else:
            asset = base_asset
    # Kraken Earn/Staking variants can embed internal numeric program codes
    # like SOL03.S or ATOM21.S. Collapse them to the base asset ticker only
    # in Earn/Staking contexts so spot assets like LUNA2 are preserved.
    asset = re.sub(r"\d+$", "", asset) or asset
    return KRAKEN_ASSET_ALIASES.get(asset, asset)


def is_kraken_earn_staking_context(row: dict[str, Any]) -> bool:
    entry_type = normalize_text(row.get("type"))
    subtype = normalize_text(row.get("subtype"))
    subclass = normalize_text(row.get("subclass"))
    if entry_type in {"staking", "earn"}:
        return True
    if entry_type == "transfer" and subtype in KRAKEN_INTERNAL_TRANSFER_SUBTYPES:
        return True
    return subclass in {"staking_on_chain", "staking_off_chain", "hold"}


def is_kraken_grouped_internal_earn_row(row: dict[str, Any]) -> bool:
    return normalize_text(row.get("type")) == "earn" and normalize_text(row.get("subtype")) in KRAKEN_EARN_INTERNAL_SUBTYPES


def is_kraken_internal_earn_group(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    return all(is_kraken_grouped_internal_earn_row(row) for row in rows)


def is_kraken_dust_sweeping_group(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    return all(normalize_text(row.get("subtype")) == "dustsweeping" for row in rows)


def build_kraken_internal_earn_group_transactions(
    rows: list[dict[str, Any]],
    *,
    refid: str,
    ignored_assets: set[str],
) -> list[Transaction]:
    rows_by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        rows_by_asset[normalize_kraken_asset_for_row(row)].append(row)

    transactions: list[Transaction] = []
    for asset, asset_rows in rows_by_asset.items():
        if asset in ignored_assets:
            continue
        primary_row = next((row for row in asset_rows if parse_decimal(row.get("amount")) > 0), asset_rows[0])
        subtypes = {normalize_text(row.get("subtype")) for row in asset_rows}
        flags = ["Internal-Transfer", "Kraken-Ledger"]
        if "migration" in subtypes:
            flags.append("Kraken-Earn-Migration")
        if "allocation" in subtypes:
            flags.append("Kraken-Earn-Allocation")
        if "deallocation" in subtypes:
            flags.append("Kraken-Earn-Deallocation")
        if "autoallocation" in subtypes:
            flags.append("Kraken-Earn-Autoallocation")
        transactions.append(
            Transaction(
                timestamp=parse_timestamp(primary_row["time"]),
                asset=asset,
                type=TransactionType.INTERNAL_TRANSFER,
                amount=max(abs(parse_decimal(row.get("amount"))) for row in asset_rows),
                price_eur=Decimal("0"),
                fee_eur=Decimal("0"),
                source="Kraken",
                location=normalize_optional_text(primary_row.get("wallet")) or "Kraken",
                tx_id=normalize_optional_text(primary_row.get("txid")) or refid,
                pair_id=refid,
                flags=flags,
            )
        )
    return transactions


def normalize_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value or "").strip().lower()


def is_kraken_staking_reward(*, entry_type: str, subtype: str, amount: Decimal) -> bool:
    if amount <= 0:
        return False
    if entry_type == "earn":
        return subtype == "reward"
    if entry_type == "staking":
        return subtype in {"", "reward"}
    return False


def is_kraken_airdrop(*, entry_type: str, subtype: str, amount: Decimal, asset: str) -> bool:
    if amount <= 0 or is_fiat_asset(asset):
        return False
    if entry_type == "invite bonus":
        return True
    if entry_type in {"dividend", "earn"}:
        return subtype == "airdrop"
    return False


def is_kraken_staking_release(*, entry_type: str, subtype: str, subclass: str, amount: Decimal) -> bool:
    return entry_type == "staking" and subtype == "" and subclass == "hold" and amount < 0


def build_kraken_airdrop_flags(*, entry_type: str) -> list[str]:
    flags = ["Kraken-Ledger"]
    if entry_type == "invite bonus":
        flags.append("Kraken-Invite-Bonus")
    if entry_type in {"dividend", "earn"}:
        flags.append("Kraken-Airdrop")
    return flags


def normalize_optional_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = str(value).strip()
    return normalized or None


def resolve_kraken_price_eur(
    row: dict[str, Any],
    *,
    asset: str,
    amount: Decimal,
    timestamp: Any,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
) -> Decimal:
    if is_fiat_asset(asset):
        return convert_kraken_fiat_amount_to_eur(
            asset=asset,
            amount=Decimal("1"),
            timestamp=timestamp,
            price_feed=price_feed,
        )

    return resolve_price_eur(
        asset=asset,
        amount=amount,
        timestamp=timestamp,
        row=row,
        price_feed=price_feed,
        coin_ids=coin_ids,
    )


def resolve_kraken_fee_eur(
    row: dict[str, Any],
    *,
    asset: str,
    timestamp: Any,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
) -> Decimal:
    if parse_decimal(row.get("fee")) == 0:
        return Decimal("0")

    fee_row = dict(row)
    fee_row["fee_currency"] = asset
    return resolve_fee_eur(
        asset=asset,
        timestamp=timestamp,
        row=fee_row,
        price_feed=price_feed,
        coin_ids=coin_ids,
    )


def convert_kraken_fiat_amount_to_eur(
    *,
    asset: str,
    amount: Decimal,
    timestamp: Any,
    price_feed: PriceFeed | None,
) -> Decimal:
    return convert_fiat_amount_to_eur(
        currency=asset,
        amount=amount,
        timestamp=parse_timestamp(timestamp) if not isinstance(timestamp, pd.Timestamp) else timestamp.to_pydatetime(),
        price_feed=price_feed,
    )


def build_kraken_parser_warnings(warning_counts: dict[str, int]) -> list[str]:
    warnings: list[str] = []
    margin_rows_skipped = warning_counts.get("margin_rows_skipped", 0)
    if margin_rows_skipped:
        warnings.append(
            f"Skipped {margin_rows_skipped} Kraken margin ledger row(s). Margin activity is not modeled by the spot FIFO engine and was excluded from the calculation."
        )
    incomplete_group_rows_skipped = warning_counts.get("incomplete_group_rows_skipped", 0)
    if incomplete_group_rows_skipped:
        warnings.append(
            f"Skipped {incomplete_group_rows_skipped} incomplete Kraken grouped ledger record(s). These grouped rows did not contain a complete debit/credit pair and were excluded from the calculation."
        )
    dust_sweeping_groups_skipped = warning_counts.get("dust_sweeping_groups_skipped", 0)
    if dust_sweeping_groups_skipped:
        warnings.append(
            f"Skipped {dust_sweeping_groups_skipped} Kraken dust-sweeping ledger group(s). These administrative consolidation records were excluded from the calculation."
        )
    delisting_conversion_rows_skipped = warning_counts.get("delisting_conversion_rows_skipped", 0)
    if delisting_conversion_rows_skipped:
        warnings.append(
            f"Skipped {delisting_conversion_rows_skipped} Kraken delisting-conversion ledger row(s). These administrative transfer records were excluded from the calculation."
        )
    return warnings
