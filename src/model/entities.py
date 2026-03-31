"""Normalized domain models used throughout the tax pipeline.

The models intentionally keep financial values as ``Decimal`` to avoid the
rounding drift that would accumulate with ``float`` in FIFO calculations.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class TransactionType(StrEnum):
    BUY = "BUY"
    SELL = "SELL"
    STAKE_REWARD = "STAKE_REWARD"
    AIRDROP = "AIRDROP"
    TRADE = "TRADE"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    INTERNAL_TRANSFER = "INTERNAL_TRANSFER"
    IMPUTED_ZERO_COST_BASIS = "IMPUTED_ZERO_COST_BASIS"


class Transaction(BaseModel):
    """Canonical transaction shape after platform-specific normalization."""

    timestamp: datetime
    asset: str
    type: TransactionType
    amount: Decimal = Field(gt=Decimal("0"))
    price_eur: Decimal = Field(ge=Decimal("0"))
    fee_eur: Decimal = Field(default=Decimal("0"), ge=Decimal("0"))
    source: Optional[str] = None
    location: Optional[str] = None
    tx_id: Optional[str] = None
    pair_id: Optional[str] = None
    counter_asset: Optional[str] = None
    counter_amount: Optional[Decimal] = None
    counter_price_eur: Optional[Decimal] = None
    notes: Optional[str] = None
    flags: list[str] = Field(default_factory=list)

    @field_validator("timestamp")
    @classmethod
    def ensure_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        return value.strip().upper()

    @property
    def gross_value_eur(self) -> Decimal:
        return self.amount * self.price_eur


class TaxLot(BaseModel):
    """Open inventory lot tracked by the FIFO engine."""

    asset: str
    acquired_at: datetime
    amount_total: Decimal = Field(gt=Decimal("0"))
    amount_remaining: Decimal = Field(ge=Decimal("0"))
    total_cost_eur: Decimal = Field(ge=Decimal("0"))
    source_type: TransactionType
    source: Optional[str] = None
    location: Optional[str] = None
    tx_id: Optional[str] = None
    flags: list[str] = Field(default_factory=list)

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        return value.strip().upper()

    @property
    def unit_cost_eur(self) -> Decimal:
        if self.amount_total == 0:
            return Decimal("0")
        return self.total_cost_eur / self.amount_total


class StakingIncome(BaseModel):
    """Income entry for rewards taxed at receipt."""

    asset: str
    received_at: datetime
    amount: Decimal
    income_eur: Decimal


class AirdropIncome(BaseModel):
    """Airdrop entry tracked separately from staking rewards."""

    asset: str
    received_at: datetime
    amount: Decimal
    income_eur: Decimal


class InternalTransferMatch(BaseModel):
    """Matched exchange-to-wallet or wallet-to-exchange movement."""

    asset: str
    transfer_out_at: datetime
    transfer_in_at: datetime
    amount_sent: Decimal
    amount_received: Decimal
    source: Optional[str] = None
    destination: Optional[str] = None
    flags: list[str] = Field(default_factory=lambda: ["Internal-Transfer"])


class TransferIssue(BaseModel):
    """Transfer record that could not be safely paired as internal."""

    asset: str
    timestamp: datetime
    amount: Decimal
    transaction_type: TransactionType
    source: Optional[str] = None
    location: Optional[str] = None
    tx_id: Optional[str] = None
    flags: list[str] = Field(default_factory=lambda: ["Unmatched-Transfer"])
    reason: str = "No matching transfer leg was found within the configured tolerance."


class RealizedGain(BaseModel):
    """FIFO realization created when a sale consumes part of a lot."""

    asset: str
    acquisition_date: datetime
    sale_date: datetime
    amount: Decimal
    acquisition_value_eur: Decimal
    transmission_value_eur: Decimal
    result_eur: Decimal
    flags: list[str] = Field(default_factory=list)
    source_lot_type: TransactionType
    acquisition_tx_id: Optional[str] = None


class EngineReport(BaseModel):
    """Aggregated output consumed by reporting and downstream tests."""

    price_backfill_provider: str | None = None
    price_external_provider: str | None = None
    price_resolution: str | None = None
    price_cache_path: str | None = None
    backfill_csv_cache_dir: str | None = None
    external_price_cache_dir: str | None = None
    realized_gains: list[RealizedGain] = Field(default_factory=list)
    staking_income: list[StakingIncome] = Field(default_factory=list)
    airdrop_income: list[AirdropIncome] = Field(default_factory=list)
    internal_transfers: list[InternalTransferMatch] = Field(default_factory=list)
    unmatched_transfers: list[TransferIssue] = Field(default_factory=list)
    processing_warnings: list[str] = Field(default_factory=list)
    open_lots: dict[str, list[TaxLot]] = Field(default_factory=dict)
