"""Tax-rule helpers layered on top of the FIFO engine results."""

from __future__ import annotations

import calendar
from datetime import datetime
from decimal import Decimal

from src.model import RealizedGain, Transaction, TransactionType


ACQUISITION_TYPES = {
    TransactionType.BUY,
    TransactionType.STAKE_REWARD,
    TransactionType.AIRDROP,
    TransactionType.TRANSFER_IN,
    TransactionType.IMPUTED_ZERO_COST_BASIS,
}


def shift_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def has_repurchase_within_two_months(
    gain: RealizedGain,
    transactions: list[Transaction],
) -> bool:
    lower_bound = shift_months(gain.sale_date, -2)
    upper_bound = shift_months(gain.sale_date, 2)

    for transaction in transactions:
        if transaction.asset != gain.asset:
            continue
        if transaction.type not in ACQUISITION_TYPES:
            continue
        if lower_bound <= transaction.timestamp <= upper_bound:
            if gain.acquisition_tx_id and transaction.tx_id == gain.acquisition_tx_id:
                continue
            if (
                gain.acquisition_tx_id is None
                and transaction.timestamp == gain.acquisition_date
                and transaction.type == gain.source_lot_type
            ):
                continue
            return True

    return False


def flag_two_month_rule(
    realized_gains: list[RealizedGain],
    transactions: list[Transaction],
) -> tuple[list[RealizedGain], list[RealizedGain]]:
    flagged: list[RealizedGain] = []
    affected: list[RealizedGain] = []

    for gain in realized_gains:
        updated = gain.model_copy(deep=True)
        if updated.result_eur < Decimal("0") and has_repurchase_within_two_months(updated, transactions):
            if "Wash-Sale-Warning" not in updated.flags:
                updated.flags.append("Wash-Sale-Warning")
            affected.append(updated.model_copy(deep=True))
        flagged.append(updated)

    return flagged, affected
