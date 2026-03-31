"""Shared parsing helpers for exchange and wallet CSV adapters."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from src.model import Transaction, TransactionType
from src.shared import PriceFeed


TRANSACTION_TYPE_ALIASES = {
    "BUY": TransactionType.BUY,
    "PURCHASE": TransactionType.BUY,
    "TRADE_BUY": TransactionType.BUY,
    "SELL": TransactionType.SELL,
    "TRADE_SELL": TransactionType.SELL,
    "STAKE_REWARD": TransactionType.STAKE_REWARD,
    "STAKING": TransactionType.STAKE_REWARD,
    "REWARD": TransactionType.STAKE_REWARD,
    "AIRDROP": TransactionType.AIRDROP,
    "DROP": TransactionType.AIRDROP,
    "TRADE": TransactionType.TRADE,
    "EXCHANGE": TransactionType.TRADE,
    "SWAP": TransactionType.TRADE,
    "PERMUTA": TransactionType.TRADE,
    "TRANSFER_IN": TransactionType.TRANSFER_IN,
    "DEPOSIT": TransactionType.TRANSFER_IN,
    "RECEIVE": TransactionType.TRANSFER_IN,
    "TRANSFER_OUT": TransactionType.TRANSFER_OUT,
    "WITHDRAWAL": TransactionType.TRANSFER_OUT,
    "WITHDRAW": TransactionType.TRANSFER_OUT,
    "SEND": TransactionType.TRANSFER_OUT,
}

FIAT_ASSETS = {"EUR", "USD", "GBP", "CAD", "AUD", "CHF", "JPY"}
BRIDGE_ASSETS = {"EUR", "USD", "BTC"}


def parse_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    if pd.isna(value):
        return default
    cleaned = str(value).strip().replace(",", "")
    if not cleaned:
        return default
    return Decimal(cleaned)


def parse_timestamp(value: Any) -> datetime:
    parsed = pd.to_datetime(value, utc=True)
    if parsed.tzinfo is None:
        return parsed.to_pydatetime().replace(tzinfo=timezone.utc)
    return parsed.to_pydatetime().astimezone(timezone.utc)


def normalize_headers(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [
        str(column).strip().lower().replace(" ", "_").replace("-", "_")
        for column in normalized.columns
    ]
    return normalized


def find_column(columns: list[str], aliases: list[str], required: bool = False) -> str | None:
    for alias in aliases:
        if alias in columns:
            return alias
    if required:
        raise ValueError(f"Missing required column. Expected one of: {aliases}")
    return None


def parse_transaction_type(raw_value: Any) -> TransactionType:
    normalized = str(raw_value).strip().upper().replace(" ", "_")
    try:
        return TRANSACTION_TYPE_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported transaction type value: {raw_value}") from exc


def is_fiat_asset(asset: str) -> bool:
    return asset.strip().upper() in FIAT_ASSETS


def resolve_price_eur(
    *,
    asset: str,
    amount: Decimal,
    timestamp: datetime,
    row: dict[str, Any],
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
) -> Decimal:
    if is_fiat_asset(asset):
        return convert_fiat_amount_to_eur(
            currency=asset,
            amount=Decimal("1"),
            timestamp=timestamp,
            price_feed=price_feed,
        )
    if "price_eur" in row and not pd.isna(row["price_eur"]):
        return parse_decimal(row["price_eur"])
    if "total_eur" in row and not pd.isna(row["total_eur"]):
        total_eur = parse_decimal(row["total_eur"])
        if amount == 0:
            raise ValueError("Amount cannot be zero when deriving unit EUR price from total_eur.")
        return total_eur / amount
    if price_feed is None:
        raise ValueError(f"Missing EUR pricing for {asset} and no price feed was provided.")

    coin_id = coin_ids.get(asset.upper(), asset.upper())
    return price_feed.get_historical_price_eur(coin_id=coin_id, at=timestamp)


def convert_fiat_amount_to_eur(
    *,
    currency: str,
    amount: Decimal,
    timestamp: datetime,
    price_feed: PriceFeed | None,
) -> Decimal:
    normalized_currency = currency.strip().upper()
    if normalized_currency == "EUR":
        return amount
    if price_feed is None:
        raise ValueError(
            f"Missing price feed to convert fiat currency {normalized_currency} to EUR."
        )
    return price_feed.convert_fiat_amount_to_eur(
        amount=amount,
        currency=normalized_currency,
        at=timestamp,
    )


def resolve_fee_eur(
    *,
    asset: str,
    timestamp: datetime,
    row: dict[str, Any],
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
) -> Decimal:
    if "fee_eur" in row and not pd.isna(row["fee_eur"]):
        return parse_decimal(row["fee_eur"])
    if "fee" not in row or pd.isna(row["fee"]):
        return Decimal("0")

    fee_amount = parse_decimal(row["fee"])
    fee_currency = str(row.get("fee_currency") or "EUR").strip().upper()
    if fee_currency == "EUR":
        return fee_amount
    if fee_currency == asset.upper():
        unit_price_eur = resolve_price_eur(
            asset=asset,
            amount=Decimal("1"),
            timestamp=timestamp,
            row=row,
            price_feed=price_feed,
            coin_ids=coin_ids,
        )
        return fee_amount * unit_price_eur
    if price_feed is None:
        raise ValueError(f"Missing price feed to convert fee currency {fee_currency} to EUR.")

    fee_coin_id = coin_ids.get(fee_currency, fee_currency)
    return price_feed.convert_asset_amount_to_eur(amount=fee_amount, coin_id=fee_coin_id, at=timestamp)


def convert_asset_amount_to_eur(
    *,
    asset: str,
    amount: Decimal,
    timestamp: datetime,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
) -> Decimal:
    normalized_asset = asset.strip().upper()
    if is_fiat_asset(normalized_asset):
        return convert_fiat_amount_to_eur(
            currency=normalized_asset,
            amount=amount,
            timestamp=timestamp,
            price_feed=price_feed,
        )
    if price_feed is None:
        raise ValueError(f"Missing price feed to convert asset {normalized_asset} to EUR.")
    return price_feed.convert_asset_amount_to_eur(
        amount=amount,
        coin_id=coin_ids.get(normalized_asset, normalized_asset),
        at=timestamp,
    )


def resolve_trade_total_eur(
    *,
    sold_asset: str,
    sold_amount: Decimal,
    received_asset: str,
    received_amount: Decimal,
    timestamp: datetime,
    row: dict[str, Any],
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
) -> Decimal:
    if ("price_eur" in row and not pd.isna(row["price_eur"])) or (
        "total_eur" in row and not pd.isna(row["total_eur"])
    ):
        sold_price_eur = resolve_price_eur(
            asset=sold_asset,
            amount=sold_amount,
            timestamp=timestamp,
            row=row,
            price_feed=price_feed,
            coin_ids=coin_ids,
        )
        return sold_amount * sold_price_eur

    if is_fiat_asset(received_asset):
        return convert_fiat_amount_to_eur(
            currency=received_asset,
            amount=received_amount,
            timestamp=timestamp,
            price_feed=price_feed,
        )

    if received_asset in BRIDGE_ASSETS:
        return convert_asset_amount_to_eur(
            asset=received_asset,
            amount=received_amount,
            timestamp=timestamp,
            price_feed=price_feed,
            coin_ids=coin_ids,
        )

    if sold_asset in BRIDGE_ASSETS:
        return convert_asset_amount_to_eur(
            asset=sold_asset,
            amount=sold_amount,
            timestamp=timestamp,
            price_feed=price_feed,
            coin_ids=coin_ids,
        )

    sold_price_eur = resolve_price_eur(
        asset=sold_asset,
        amount=sold_amount,
        timestamp=timestamp,
        row=row,
        price_feed=price_feed,
        coin_ids=coin_ids,
    )
    return sold_amount * sold_price_eur


def build_transactions_from_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
    column_aliases: dict[str, list[str]],
    ignored_assets: set[str] | None = None,
) -> list[Transaction]:
    normalized = normalize_headers(frame)
    columns = list(normalized.columns)
    resolved_columns = {
        target: find_column(columns, aliases, required=target in {"timestamp", "asset", "type", "amount"})
        for target, aliases in column_aliases.items()
    }

    transactions: list[Transaction] = []
    ignored = {asset.strip().upper() for asset in (ignored_assets or set())}
    for _, series in normalized.iterrows():
        row = {
            target: series[column_name]
            for target, column_name in resolved_columns.items()
            if column_name is not None
        }

        timestamp = parse_timestamp(row["timestamp"])
        amount = parse_decimal(row["amount"])
        asset = str(row["asset"]).strip().upper()
        if asset in ignored:
            continue
        transaction_type = parse_transaction_type(row["type"])

        base_kwargs = {
            "timestamp": timestamp,
            "source": source,
            "location": str(row["location"]).strip() if "location" in row and not pd.isna(row["location"]) else source,
            "tx_id": str(row["tx_id"]).strip() if "tx_id" in row and not pd.isna(row["tx_id"]) else None,
            "notes": str(row["notes"]).strip() if "notes" in row and not pd.isna(row["notes"]) else None,
        }

        if transaction_type == TransactionType.TRADE:
            received_asset_raw = row.get("received_asset")
            if received_asset_raw is not None and not pd.isna(received_asset_raw):
                received_asset = str(received_asset_raw).strip().upper()
                if received_asset in ignored:
                    continue
            fee_eur = resolve_fee_eur(
                asset=asset,
                timestamp=timestamp,
                row=row,
                price_feed=price_feed,
                coin_ids=coin_ids,
            )
            transactions.extend(
                build_trade_transactions(
                    row=row,
                    source=source,
                    asset=asset,
                    amount=amount,
                    timestamp=timestamp,
                    fee_eur=fee_eur,
                    price_feed=price_feed,
                    coin_ids=coin_ids,
                    base_kwargs=base_kwargs,
                    ignored_assets=ignored,
                )
            )
            continue

        fee_eur = resolve_fee_eur(
            asset=asset,
            timestamp=timestamp,
            row=row,
            price_feed=price_feed,
            coin_ids=coin_ids,
        )
        price_eur = resolve_price_eur(
            asset=asset,
            amount=amount,
            timestamp=timestamp,
            row=row,
            price_feed=price_feed,
            coin_ids=coin_ids,
        )

        transactions.append(
            Transaction(
                **base_kwargs,
                asset=asset,
                type=transaction_type,
                amount=amount,
                price_eur=price_eur,
                fee_eur=fee_eur,
            )
        )

    return transactions


def build_trade_transactions(
    *,
    row: dict[str, Any],
    source: str,
    asset: str,
    amount: Decimal,
    timestamp: datetime,
    fee_eur: Decimal,
    price_feed: PriceFeed | None,
    coin_ids: dict[str, str],
    base_kwargs: dict[str, Any],
    ignored_assets: set[str] | None = None,
) -> list[Transaction]:
    received_asset_raw = row.get("received_asset")
    received_amount_raw = row.get("received_amount")

    if received_asset_raw is None or pd.isna(received_asset_raw):
        raise ValueError("TRADE rows must provide the received asset column.")
    if received_amount_raw is None or pd.isna(received_amount_raw):
        raise ValueError("TRADE rows must provide the received amount column.")

    received_asset = str(received_asset_raw).strip().upper()
    ignored = {asset.strip().upper() for asset in (ignored_assets or set())}
    if asset in ignored or received_asset in ignored:
        return []
    received_amount = parse_decimal(received_amount_raw)
    pair_id = base_kwargs.get("tx_id") or f"{source}:{timestamp.isoformat()}:{asset}:{received_asset}"

    # Fiat legs are ordinary buys/sells, not crypto-to-crypto permutas.
    if is_fiat_asset(asset) and is_fiat_asset(received_asset):
        return []

    if is_fiat_asset(asset):
        acquisition_total_eur = convert_fiat_amount_to_eur(
            currency=asset,
            amount=amount,
            timestamp=timestamp,
            price_feed=price_feed,
        )
        return [
            Transaction(
                **base_kwargs,
                asset=received_asset,
                type=TransactionType.BUY,
                amount=received_amount,
                price_eur=acquisition_total_eur / received_amount,
                fee_eur=fee_eur,
                pair_id=pair_id,
                counter_asset=asset,
                counter_amount=amount,
            )
        ]

    if is_fiat_asset(received_asset):
        transmission_total_eur = convert_fiat_amount_to_eur(
            currency=received_asset,
            amount=received_amount,
            timestamp=timestamp,
            price_feed=price_feed,
        )
        return [
            Transaction(
                **base_kwargs,
                asset=asset,
                type=TransactionType.SELL,
                amount=amount,
                price_eur=transmission_total_eur / amount,
                fee_eur=fee_eur,
                pair_id=pair_id,
                counter_asset=received_asset,
                counter_amount=received_amount,
            )
        ]

    # For crypto-to-crypto swaps, prefer the exchange-observed counter leg when
    # it uses a bridge asset like BTC. Only fall back to the external feed when
    # the trade itself does not give a usable conversion path.
    disposed_total_eur = resolve_trade_total_eur(
        sold_asset=asset,
        sold_amount=amount,
        received_asset=received_asset,
        received_amount=received_amount,
        timestamp=timestamp,
        row=row,
        price_feed=price_feed,
        coin_ids=coin_ids,
    )
    received_total_eur = disposed_total_eur
    received_price_eur = received_total_eur / received_amount

    disposal_total_eur = disposed_total_eur
    disposal_price_eur = disposal_total_eur / amount
    common_flags = ["Permuta"]

    return [
        Transaction(
            **base_kwargs,
            asset=asset,
            type=TransactionType.SELL,
            amount=amount,
            price_eur=disposal_price_eur,
            fee_eur=fee_eur,
            pair_id=pair_id,
            counter_asset=received_asset,
            counter_amount=received_amount,
            counter_price_eur=received_price_eur,
            flags=list(common_flags),
        ),
        Transaction(
            **base_kwargs,
            asset=received_asset,
            type=TransactionType.BUY,
            amount=received_amount,
            price_eur=received_price_eur,
            fee_eur=Decimal("0"),
            pair_id=pair_id,
            counter_asset=asset,
            counter_amount=amount,
            counter_price_eur=disposal_price_eur,
            flags=list(common_flags),
        ),
    ]


def read_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(Path(path))
