from __future__ import annotations

import argparse
import csv
import hashlib
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


ID_COLUMNS = ("txid", "refid")
ID_TEMPLATE = "AAAAA-ZZZZZ-{counter:05d}"
NUMERIC_COLUMNS = ("amount", "fee", "balance")
BALANCE_COLUMNS = ("asset", "amount", "fee", "balance")


def kraken_export_anonymizer(path: Path) -> None:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise ValueError(f"CSV file has no header: {path}")

        rows = list(reader)

    missing_columns = [column for column in ID_COLUMNS if column not in fieldnames]
    if missing_columns:
        raise ValueError(
            f"CSV file is missing required id columns {missing_columns}: {path}"
        )
    missing_balance_columns = [
        column for column in BALANCE_COLUMNS if column not in fieldnames
    ]
    if missing_balance_columns:
        raise ValueError(
            f"CSV file is missing required numeric columns {missing_balance_columns}: {path}"
        )

    id_map: dict[str, str] = {}
    next_counter = 1
    running_balances: dict[str, Decimal] = {}

    for row in rows:
        for column in ID_COLUMNS:
            value = (row.get(column) or "").strip()
            if not value:
                continue
            if value not in id_map:
                id_map[value] = ID_TEMPLATE.format(counter=next_counter)
                next_counter += 1
            row[column] = id_map[value]

        amount_precision = _decimal_precision(row["amount"])
        fee_precision = _decimal_precision(row["fee"])
        balance_precision = _decimal_precision(row["balance"])
        amount_quant = _quantizer_for(amount_precision)
        fee_quant = _quantizer_for(fee_precision)
        balance_quant = _quantizer_for(balance_precision)

        amount_value = _parse_decimal(row["amount"])
        fee_value = _parse_decimal(row["fee"])
        asset = row["asset"]
        amount_factor = _amount_factor_for_row(row)
        fee_factor = _fee_factor_for_row(row)

        randomized_amount = _scale_decimal(amount_value, amount_factor, amount_quant)
        randomized_fee = _scale_decimal(fee_value, fee_factor, fee_quant)
        previous_balance = running_balances.get(asset, Decimal("0"))
        randomized_balance = (
            previous_balance + randomized_amount - randomized_fee
        ).quantize(
            balance_quant,
            rounding=ROUND_HALF_UP,
        )
        running_balances[asset] = randomized_balance

        row["amount"] = _format_decimal(randomized_amount, amount_precision)
        row["fee"] = _format_decimal(randomized_fee, fee_precision)
        row["balance"] = _format_decimal(randomized_balance, balance_precision)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)


def _parse_decimal(value: str) -> Decimal:
    cleaned = str(value).strip()
    if not cleaned:
        return Decimal("0")
    return Decimal(cleaned)


def _decimal_precision(value: str) -> int:
    text = str(value).strip()
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def _quantizer_for(precision: int) -> Decimal:
    if precision <= 0:
        return Decimal("1")
    return Decimal(f"1e-{precision}")


def _format_decimal(value: Decimal, precision: int) -> str:
    quantized = value.quantize(_quantizer_for(precision), rounding=ROUND_HALF_UP)
    if precision == 0:
        return str(quantized.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return f"{quantized:.{precision}f}"


def _scale_decimal(value: Decimal, factor: Decimal, quantizer: Decimal) -> Decimal:
    if value == 0:
        return Decimal("0").quantize(quantizer)

    scaled = (value * factor).quantize(quantizer, rounding=ROUND_HALF_UP)
    if scaled == 0:
        minimum = quantizer.copy_sign(value)
        return minimum
    return scaled


def _amount_factor_for_row(row: dict[str, str]) -> Decimal:
    return _factor_from_key(
        f"amount::{row['asset']}::{row['type']}::{row['subtype']}::{row['time']}",
        minimum_basis_points=7000,
        spread_basis_points=5000,
    )


def _fee_factor_for_row(row: dict[str, str]) -> Decimal:
    return _factor_from_key(
        f"fee::{row['asset']}::{row['type']}::{row['subtype']}::{row['time']}",
        minimum_basis_points=6000,
        spread_basis_points=3500,
    )


def _factor_from_key(
    key: str,
    *,
    minimum_basis_points: int,
    spread_basis_points: int,
) -> Decimal:
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    basis_points = minimum_basis_points + (
        int.from_bytes(digest[:2], "big") % (spread_basis_points + 1)
    )
    return Decimal(basis_points) / Decimal("10000")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replace Kraken ledger txid/refid values with sequential anonymous placeholders."
    )
    parser.add_argument("path", help="Path to the Kraken ledger CSV file.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    kraken_export_anonymizer(Path(args.path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
