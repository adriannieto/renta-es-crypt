"""Reconcile likely internal transfers before FIFO processing."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from src.model import InternalTransferMatch, Transaction, TransactionType, TransferIssue


def reconcile_internal_transfers(
    transactions: list[Transaction],
    window_hours: int = 4,
    amount_tolerance_pct: Decimal = Decimal("1.0"),
) -> tuple[list[Transaction], list[InternalTransferMatch], list[TransferIssue]]:
    normalized = [transaction.model_copy(deep=True) for transaction in sorted(transactions, key=lambda tx: tx.timestamp)]
    transfer_ins: dict[str, list[int]] = {}
    matches: list[InternalTransferMatch] = []
    unmatched: list[TransferIssue] = []
    window = timedelta(hours=window_hours)

    for index, transaction in enumerate(normalized):
        if transaction.type == TransactionType.TRANSFER_IN:
            transfer_ins.setdefault(transaction.asset, []).append(index)

    used_in_indices: set[int] = set()

    for out_index, transaction in enumerate(normalized):
        if transaction.type != TransactionType.TRANSFER_OUT:
            continue

        candidate_indices = transfer_ins.get(transaction.asset, [])
        match_index = None

        for in_index in candidate_indices:
            if in_index in used_in_indices:
                continue

            candidate = normalized[in_index]
            time_delta = abs(candidate.timestamp - transaction.timestamp)

            if time_delta > window:
                continue

            lower_bound = transaction.amount * (Decimal("1") - (amount_tolerance_pct / Decimal("100")))
            upper_bound = transaction.amount * (Decimal("1") + (amount_tolerance_pct / Decimal("100")))

            if lower_bound <= candidate.amount <= upper_bound:
                match_index = in_index
                break

        if match_index is None:
            continue

        used_in_indices.add(match_index)
        incoming = normalized[match_index]
        transaction.type = TransactionType.INTERNAL_TRANSFER
        incoming.type = TransactionType.INTERNAL_TRANSFER

        if "Internal-Transfer" not in transaction.flags:
            transaction.flags.append("Internal-Transfer")
        if "Internal-Transfer" not in incoming.flags:
            incoming.flags.append("Internal-Transfer")

        pair_id = transaction.pair_id or transaction.tx_id or f"{transaction.asset}:{transaction.timestamp.isoformat()}"
        transaction.pair_id = pair_id
        incoming.pair_id = pair_id

        matches.append(
            InternalTransferMatch(
                asset=transaction.asset,
                transfer_out_at=transaction.timestamp,
                transfer_in_at=incoming.timestamp,
                amount_sent=transaction.amount,
                amount_received=incoming.amount,
                source=transaction.location or transaction.source,
                destination=incoming.location or incoming.source,
            )
        )

    matched_indices = {
        index
        for index, transaction in enumerate(normalized)
        if transaction.type == TransactionType.INTERNAL_TRANSFER
    }

    for index, transaction in enumerate(normalized):
        if index in matched_indices:
            continue
        if transaction.type not in {TransactionType.TRANSFER_IN, TransactionType.TRANSFER_OUT}:
            continue
        unmatched.append(
            TransferIssue(
                asset=transaction.asset,
                timestamp=transaction.timestamp,
                amount=transaction.amount,
                transaction_type=transaction.type,
                source=transaction.source,
                location=transaction.location,
                tx_id=transaction.tx_id,
            )
        )

    return normalized, matches, unmatched
