"""Core FIFO engine for Spanish crypto gain/loss calculations."""

from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal

from src.app.settings import TwoMonthRuleMode, UnmatchedTransferInMode
from src.engine.tax_rules import flag_two_month_rule
from src.engine.transfer_matcher import reconcile_internal_transfers
from src.model import (
    AirdropIncome,
    EngineReport,
    InternalTransferMatch,
    RealizedGain,
    StakingIncome,
    TaxLot,
    Transaction,
    TransactionType,
    TransferIssue,
)


class UnmatchedInboundTransfersError(ValueError):
    """Raised when unmatched inbound transfers block reliable FIFO processing."""

    def __init__(self, issues: list[TransferIssue]) -> None:
        self.issues = list(issues)
        super().__init__(
            f"Found {len(self.issues)} unmatched inbound transfer(s). "
            "Review and classify them before calculating taxes, or override the handling mode explicitly."
        )


class InsufficientInventoryError(ValueError):
    """Raised when a sale exceeds the FIFO inventory available for an asset."""

    def __init__(
        self,
        *,
        transaction: Transaction,
        available_amount: Decimal,
        open_lots: list[TaxLot],
    ) -> None:
        self.transaction = transaction
        self.available_amount = available_amount
        self.open_lots = [lot.model_copy(deep=True) for lot in open_lots]
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        sale = self.transaction
        lines = [
            f"Insufficient inventory for {sale.asset}: trying to sell {sale.amount}, available {self.available_amount}.",
            "Sale transaction:",
            (
                f"  timestamp={sale.timestamp.isoformat()} tx_id={sale.tx_id or '-'} "
                f"source={sale.source or '-'} location={sale.location or '-'} "
                f"amount={sale.amount} price_eur={sale.price_eur} fee_eur={sale.fee_eur}"
            ),
        ]
        if not self.open_lots:
            lines.append("Open lots before sale: none")
            return "\n".join(lines)

        lines.append("Open lots before sale:")
        for lot in self.open_lots:
            lines.append(
                "  "
                f"acquired_at={lot.acquired_at.isoformat()} tx_id={lot.tx_id or '-'} "
                f"source_type={lot.source_type.value} amount_remaining={lot.amount_remaining} "
                f"unit_cost_eur={lot.unit_cost_eur} total_cost_eur={lot.total_cost_eur} "
                f"source={lot.source or '-'} location={lot.location or '-'}"
            )
        return "\n".join(lines)


class FifoEngine:
    """FIFO-based capital gains engine for Spanish crypto tax reporting."""

    def __init__(self) -> None:
        self._queues: dict[str, deque[TaxLot]] = defaultdict(deque)
        self._realized_gains: list[RealizedGain] = []
        self._staking_income: list[StakingIncome] = []
        self._airdrop_income: list[AirdropIncome] = []
        self._internal_transfers: list[InternalTransferMatch] = []
        self._unmatched_transfers: list[TransferIssue] = []
        self._processing_warnings: list[str] = []
        self._processed_transactions: list[Transaction] = []

    def process_transactions(
        self,
        transactions: list[Transaction],
        transfer_window_hours: int = 4,
        transfer_amount_tolerance_pct: Decimal = Decimal("1.0"),
        unmatched_transfer_in_mode: str = UnmatchedTransferInMode.FAIL,
        two_month_rule_mode: str = TwoMonthRuleMode.ENABLED,
    ) -> EngineReport:
        self._queues = defaultdict(deque)
        self._realized_gains = []
        self._staking_income = []
        self._airdrop_income = []
        self._internal_transfers = []
        self._unmatched_transfers = []
        self._processing_warnings = []
        (
            self._processed_transactions,
            self._internal_transfers,
            self._unmatched_transfers,
        ) = reconcile_internal_transfers(
            transactions=transactions,
            window_hours=transfer_window_hours,
            amount_tolerance_pct=transfer_amount_tolerance_pct,
        )
        self._apply_unmatched_transfer_in_mode(unmatched_transfer_in_mode)

        for transaction in self._processed_transactions:
            self.process_transaction(transaction)

        flagged_gains, affected_two_month = flag_two_month_rule(
            realized_gains=self._realized_gains,
            transactions=self._processed_transactions,
        )
        self._apply_two_month_rule_mode(flagged_gains, affected_two_month, two_month_rule_mode)

        return EngineReport(
            realized_gains=list(self._realized_gains),
            staking_income=list(self._staking_income),
            airdrop_income=list(self._airdrop_income),
            internal_transfers=list(self._internal_transfers),
            unmatched_transfers=list(self._unmatched_transfers),
            processing_warnings=list(self._processing_warnings),
            open_lots=self.get_open_lots(),
        )

    def process_transaction(self, transaction: Transaction) -> None:
        if transaction.type in {TransactionType.BUY, TransactionType.IMPUTED_ZERO_COST_BASIS}:
            self._handle_acquisition(transaction)
            return
        if transaction.type == TransactionType.STAKE_REWARD:
            self._handle_stake_reward(transaction)
            return
        if transaction.type == TransactionType.AIRDROP:
            self._handle_airdrop(transaction)
            return
        if transaction.type == TransactionType.SELL:
            self._handle_sale(transaction)
            return
        if transaction.type in {
            TransactionType.TRANSFER_IN,
            TransactionType.TRANSFER_OUT,
            TransactionType.INTERNAL_TRANSFER,
        }:
            return

        raise ValueError(f"Unsupported transaction type: {transaction.type}")

    def _apply_unmatched_transfer_in_mode(self, unmatched_transfer_in_mode: str) -> None:
        unmatched_inbounds = [
            issue
            for issue in self._unmatched_transfers
            if issue.transaction_type == TransactionType.TRANSFER_IN
        ]

        if not unmatched_inbounds:
            return
        if unmatched_transfer_in_mode == UnmatchedTransferInMode.FAIL:
            raise UnmatchedInboundTransfersError(unmatched_inbounds)
        if unmatched_transfer_in_mode != UnmatchedTransferInMode.ZERO_COST_BASIS:
            return

        updated_inbounds = 0

        for issue in self._unmatched_transfers:
            if issue.transaction_type != TransactionType.TRANSFER_IN:
                continue
            for transaction in self._processed_transactions:
                if transaction.tx_id and issue.tx_id and transaction.tx_id != issue.tx_id:
                    continue
                if (
                    transaction.asset == issue.asset
                    and transaction.timestamp == issue.timestamp
                    and transaction.type == TransactionType.TRANSFER_IN
                ):
                    transaction.type = TransactionType.IMPUTED_ZERO_COST_BASIS
                    transaction.price_eur = Decimal("0")
                    transaction.fee_eur = Decimal("0")
                    if "Unmatched-Transfer" not in transaction.flags:
                        transaction.flags.append("Unmatched-Transfer")
                    if "Zero-Cost-Basis-Assumption" not in transaction.flags:
                        transaction.flags.append("Zero-Cost-Basis-Assumption")
                    updated_inbounds += 1
                    break

        if updated_inbounds > 0:
            self._processing_warnings.append(
                "Conservative mode 'zero_cost_basis' was applied to unmatched inbound transfers. "
                "Those assets were added to FIFO with zero acquisition value, which can overstate gains "
                "and may not be fully compliant with Spanish tax law."
            )

    def _apply_two_month_rule_mode(
        self,
        flagged_gains: list[RealizedGain],
        affected_two_month: list[RealizedGain],
        two_month_rule_mode: str,
    ) -> None:
        if two_month_rule_mode == TwoMonthRuleMode.ENABLED:
            self._realized_gains = flagged_gains
            return

        affected_keys = {
            (gain.asset, gain.acquisition_date, gain.sale_date, gain.amount, gain.acquisition_tx_id)
            for gain in affected_two_month
        }
        downgraded: list[RealizedGain] = []

        for gain in flagged_gains:
            updated = gain.model_copy(deep=True)
            gain_key = (updated.asset, updated.acquisition_date, updated.sale_date, updated.amount, updated.acquisition_tx_id)
            if gain_key in affected_keys:
                updated.flags = [flag for flag in updated.flags if flag != "Wash-Sale-Warning"]
                if "Two-Month-Rule-Disabled-Warning" not in updated.flags:
                    updated.flags.append("Two-Month-Rule-Disabled-Warning")
            downgraded.append(updated)

        if affected_two_month:
            self._processing_warnings.append(
                "Two-month rule detection was triggered for one or more loss transactions, "
                "but rule enforcement is disabled by configuration. Results may therefore be more favorable "
                "than a conservative Spanish tax treatment."
            )

        self._realized_gains = downgraded

    def get_open_lots(self) -> dict[str, list[TaxLot]]:
        return {
            asset: [lot.model_copy(deep=True) for lot in lots if lot.amount_remaining > 0]
            for asset, lots in self._queues.items()
            if any(lot.amount_remaining > 0 for lot in lots)
        }

    def _handle_acquisition(self, transaction: Transaction) -> None:
        total_cost = transaction.gross_value_eur + transaction.fee_eur
        lot = TaxLot(
            asset=transaction.asset,
            acquired_at=transaction.timestamp,
            amount_total=transaction.amount,
            amount_remaining=transaction.amount,
            total_cost_eur=total_cost,
            source_type=transaction.type,
            source=transaction.source,
            location=transaction.location,
            tx_id=transaction.tx_id,
            flags=list(transaction.flags),
        )
        self._queues[transaction.asset].append(lot)

    def _handle_stake_reward(self, transaction: Transaction) -> None:
        self._staking_income.append(
            StakingIncome(
                asset=transaction.asset,
                received_at=transaction.timestamp,
                amount=transaction.amount,
                income_eur=transaction.gross_value_eur,
            )
        )
        self._handle_acquisition(transaction)

    def _handle_airdrop(self, transaction: Transaction) -> None:
        self._airdrop_income.append(
            AirdropIncome(
                asset=transaction.asset,
                received_at=transaction.timestamp,
                amount=transaction.amount,
                income_eur=transaction.gross_value_eur,
            )
        )
        flagged = transaction.model_copy(deep=True)
        if "Airdrop" not in flagged.flags:
            flagged.flags.append("Airdrop")
        self._handle_acquisition(flagged)

    def _handle_sale(self, transaction: Transaction) -> None:
        remaining_to_sell = transaction.amount
        sale_queue = self._queues[transaction.asset]
        available_amount = sum(lot.amount_remaining for lot in sale_queue)

        if available_amount < transaction.amount:
            raise InsufficientInventoryError(
                transaction=transaction,
                available_amount=available_amount,
                open_lots=[lot.model_copy(deep=True) for lot in sale_queue if lot.amount_remaining > 0],
            )

        while remaining_to_sell > 0:
            current_lot = sale_queue[0]
            if current_lot.amount_remaining == 0:
                sale_queue.popleft()
                continue

            matched_amount = min(current_lot.amount_remaining, remaining_to_sell)
            acquisition_value = matched_amount * current_lot.unit_cost_eur
            fee_share = transaction.fee_eur * (matched_amount / transaction.amount)
            transmission_value = (matched_amount * transaction.price_eur) - fee_share
            flags = list(dict.fromkeys([*current_lot.flags, *transaction.flags]))

            if current_lot.source_type == TransactionType.STAKE_REWARD and "Staking" not in flags:
                flags.append("Staking")
            if current_lot.source_type == TransactionType.AIRDROP and "Airdrop" not in flags:
                flags.append("Airdrop")

            self._realized_gains.append(
                RealizedGain(
                    asset=transaction.asset,
                    acquisition_date=current_lot.acquired_at,
                    sale_date=transaction.timestamp,
                    amount=matched_amount,
                    acquisition_value_eur=acquisition_value,
                    transmission_value_eur=transmission_value,
                    result_eur=transmission_value - acquisition_value,
                    flags=flags,
                    source_lot_type=current_lot.source_type,
                    acquisition_tx_id=current_lot.tx_id,
                )
            )

            current_lot.amount_remaining -= matched_amount
            remaining_to_sell -= matched_amount

            if current_lot.amount_remaining == 0:
                sale_queue.popleft()
