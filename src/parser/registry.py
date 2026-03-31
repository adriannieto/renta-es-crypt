"""Parser registry for supported CSV input platforms."""

from __future__ import annotations

from src.parser.kraken_csv_parser import KrakenCsvParser
from src.shared import CsvParser


PARSER_BY_PLATFORM: dict[str, CsvParser] = {
    "kraken": KrakenCsvParser(),
}


def get_csv_parser(platform: str) -> CsvParser:
    normalized_platform = platform.strip().lower()
    try:
        return PARSER_BY_PLATFORM[normalized_platform]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported platform '{platform}'. Expected one of: {sorted(PARSER_BY_PLATFORM)}"
        ) from exc
