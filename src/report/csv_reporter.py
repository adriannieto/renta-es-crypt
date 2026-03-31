"""Report exporters for Modelo 100 and supporting schedules."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.model import EngineReport

MODELO100_COLUMNS = [
    "Asset Name",
    "Acquisition Date",
    "Sale Date",
    "Acquisition Value (including fees)",
    "Transmission Value (minus fees)",
    "Result (Gain/Loss)",
    "Flags",
]

STAKING_COLUMNS = ["Asset Name", "Receipt Date", "Amount", "Income EUR"]
AIRDROP_COLUMNS = ["Asset Name", "Receipt Date", "Amount", "Income EUR"]
INTERNAL_TRANSFER_COLUMNS = [
    "Asset Name",
    "Transfer Out",
    "Transfer In",
    "Amount Sent",
    "Amount Received",
    "Source",
    "Destination",
    "Flags",
]
UNMATCHED_TRANSFER_COLUMNS = [
    "Asset Name",
    "Timestamp",
    "Amount",
    "Transaction Type",
    "Source",
    "Location",
    "Transaction Id",
    "Flags",
    "Reason",
]
WARNING_COLUMNS = ["Level", "Message"]


def build_modelo100_frame(report: EngineReport, tax_year: int | None = None) -> pd.DataFrame:
    rows = [
        {
            "Asset Name": gain.asset,
            "Acquisition Date": gain.acquisition_date.date().isoformat(),
            "Sale Date": gain.sale_date.date().isoformat(),
            "Acquisition Value (including fees)": float(gain.acquisition_value_eur),
            "Transmission Value (minus fees)": float(gain.transmission_value_eur),
            "Result (Gain/Loss)": float(gain.result_eur),
            "Flags": ", ".join(gain.flags),
        }
        for gain in report.realized_gains
        if tax_year is None or gain.sale_date.year == tax_year
    ]
    return pd.DataFrame(rows, columns=MODELO100_COLUMNS)


def build_staking_income_frame(report: EngineReport, tax_year: int | None = None) -> pd.DataFrame:
    rows = [
        {
            "Asset Name": item.asset,
            "Receipt Date": item.received_at.date().isoformat(),
            "Amount": float(item.amount),
            "Income EUR": float(item.income_eur),
        }
        for item in report.staking_income
        if tax_year is None or item.received_at.year == tax_year
    ]
    return pd.DataFrame(rows, columns=STAKING_COLUMNS)


def build_airdrop_income_frame(report: EngineReport, tax_year: int | None = None) -> pd.DataFrame:
    rows = [
        {
            "Asset Name": item.asset,
            "Receipt Date": item.received_at.date().isoformat(),
            "Amount": float(item.amount),
            "Income EUR": float(item.income_eur),
        }
        for item in report.airdrop_income
        if tax_year is None or item.received_at.year == tax_year
    ]
    return pd.DataFrame(rows, columns=AIRDROP_COLUMNS)


def build_internal_transfer_frame(report: EngineReport) -> pd.DataFrame:
    rows = [
        {
            "Asset Name": item.asset,
            "Transfer Out": item.transfer_out_at.isoformat(),
            "Transfer In": item.transfer_in_at.isoformat(),
            "Amount Sent": float(item.amount_sent),
            "Amount Received": float(item.amount_received),
            "Source": item.source,
            "Destination": item.destination,
            "Flags": ", ".join(item.flags),
        }
        for item in report.internal_transfers
    ]
    return pd.DataFrame(rows, columns=INTERNAL_TRANSFER_COLUMNS)


def build_unmatched_transfer_frame(report: EngineReport) -> pd.DataFrame:
    rows = [
        {
            "Asset Name": item.asset,
            "Timestamp": item.timestamp.isoformat(),
            "Amount": float(item.amount),
            "Transaction Type": item.transaction_type,
            "Source": item.source,
            "Location": item.location,
            "Transaction Id": item.tx_id,
            "Flags": ", ".join(item.flags),
            "Reason": item.reason,
        }
        for item in report.unmatched_transfers
    ]
    return pd.DataFrame(rows, columns=UNMATCHED_TRANSFER_COLUMNS)


def build_warning_frame(report: EngineReport) -> pd.DataFrame:
    rows = [{"Level": "WARNING", "Message": message} for message in report.processing_warnings]
    return pd.DataFrame(rows, columns=WARNING_COLUMNS)


def export_report(
    report: EngineReport,
    output_path: str | Path,
    tax_year: int | None = None,
) -> list[Path]:
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    modelo100_frame = build_modelo100_frame(report, tax_year=tax_year)
    staking_frame = build_staking_income_frame(report, tax_year=tax_year)
    airdrop_frame = build_airdrop_income_frame(report, tax_year=tax_year)
    transfer_frame = build_internal_transfer_frame(report)
    unmatched_frame = build_unmatched_transfer_frame(report)
    warning_frame = build_warning_frame(report)

    base = destination.with_suffix("") if destination.suffix else destination
    modelo100_path = base.with_name(f"{base.name}_modelo100.csv")
    staking_path = base.with_name(f"{base.name}_staking_income.csv")
    airdrop_path = base.with_name(f"{base.name}_airdrop_income.csv")
    transfer_path = base.with_name(f"{base.name}_internal_transfers.csv")
    unmatched_path = base.with_name(f"{base.name}_unmatched_transfers.csv")
    warning_path = base.with_name(f"{base.name}_warnings.csv")
    modelo100_frame.to_csv(modelo100_path, index=False)
    staking_frame.to_csv(staking_path, index=False)
    airdrop_frame.to_csv(airdrop_path, index=False)
    transfer_frame.to_csv(transfer_path, index=False)
    unmatched_frame.to_csv(unmatched_path, index=False)
    warning_frame.to_csv(warning_path, index=False)
    return [modelo100_path, staking_path, airdrop_path, transfer_path, unmatched_path, warning_path]
