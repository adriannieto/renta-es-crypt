from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
from typing import Callable

import pandas as pd
import requests

from src.feeds.price_cache import PriceCache
from src.shared import PriceResolution, normalize_price_timestamp


PAIR_PROVIDER = "cryptodatadownload"


class CryptoDataDownloadBackfill:
    """Best-effort local pair resolver backed by CryptoDataDownload CSV snapshots."""

    def __init__(
        self,
        *,
        cache_path: str | Path = ".cache/price_cache.json",
        resolution: str = PriceResolution.HOUR,
        base_url: str = "https://www.cryptodatadownload.com/cdd",
        exchanges: list[str] | None = None,
        quote_priority: list[str] | None = None,
        symbols: dict[str, str] | None = None,
        hour_suffix: str = "1h",
        day_suffix: str = "d",
        minute_suffix: str = "minute",
        allow_resolution_downgrade: bool = False,
        tax_year: int | None = None,
        session: requests.Session | None = None,
        timeout_seconds: int = 30,
        price_cache: PriceCache | None = None,
        csv_cache_dir: str | Path | None = None,
        now_provider: Callable[[], datetime] | None = None,
        event_logger: Callable[[str], None] | None = None,
    ) -> None:
        self.price_cache = price_cache or PriceCache(cache_path)
        cache_root = Path(cache_path).expanduser().parent
        self.resolution = resolution
        self.base_url = base_url.rstrip("/")
        self.exchanges = exchanges or ["Binance", "Bitfinex", "Bitstamp"]
        self.quote_priority = [quote.upper() for quote in (quote_priority or ["EUR", "USD", "USDT", "BTC"])]
        self.symbols = {asset.upper(): symbol.upper() for asset, symbol in (symbols or {}).items()}
        self.hour_suffix = hour_suffix
        self.day_suffix = day_suffix
        self.minute_suffix = minute_suffix
        self.allow_resolution_downgrade = allow_resolution_downgrade
        self.tax_year = tax_year
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self.csv_cache_dir = Path(csv_cache_dir).expanduser() if csv_cache_dir is not None else cache_root / "cryptodatadownload"
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self.event_logger = event_logger
        self._warnings: list[str] = []
        self._warning_keys: set[str] = set()
        self._attempted_backfills: set[tuple[str, str, str, int | None]] = set()
        self._last_failed_resolution: dict[str, str] | None = None
        self._downgrade_warning_emitted = False

    def get_warnings(self) -> list[str]:
        return list(self._warnings)

    def resolve_historical_price_eur(self, symbol: str, at: datetime) -> Decimal | None:
        price, _ = self.resolve_historical_price_eur_with_metadata(symbol, at)
        return price

    def resolve_historical_price_eur_with_metadata(self, symbol: str, at: datetime) -> tuple[Decimal | None, str | None]:
        normalized_symbol = self._normalize_symbol(symbol)
        normalized_at = at.astimezone(timezone.utc)
        attempted_pairs: set[str] = set()
        price, resolution = self._resolve_unit_eur_value(
            symbol=normalized_symbol,
            at=normalized_at,
            visited=frozenset(),
            attempted_pairs=attempted_pairs,
        )
        if price is None:
            self._last_failed_resolution = {
                "symbol": normalized_symbol,
                "timestamp": normalized_at.isoformat(),
                "pairs": ", ".join(sorted(attempted_pairs)),
            }
        else:
            self._last_failed_resolution = None
        return price, resolution

    def convert_fiat_amount_to_eur(self, amount: Decimal, currency: str, at: datetime) -> Decimal:
        normalized_currency = currency.strip().upper()
        if normalized_currency == "EUR":
            return amount

        eur_rate = self.resolve_historical_price_eur(normalized_currency, at)
        if eur_rate is None:
            raise ValueError(
                f"CryptoDataDownload backfill could not resolve historical EUR FX value for {normalized_currency} at {at}."
            )
        return amount * eur_rate

    def _resolve_unit_eur_value(
        self,
        *,
        symbol: str,
        at: datetime,
        visited: frozenset[str],
        attempted_pairs: set[str],
    ) -> tuple[Decimal | None, str | None]:
        normalized_symbol = symbol.upper()
        if normalized_symbol == "EUR":
            return Decimal("1"), PriceResolution.MINUTE
        if normalized_symbol in visited:
            return None, None

        next_visited = visited | {normalized_symbol}
        direct_pair, direct_resolution = self._get_pair_rate(
            base=normalized_symbol,
            quote="EUR",
            at=at,
            attempted_pairs=attempted_pairs,
        )
        if direct_pair is not None:
            return direct_pair, direct_resolution

        anchored_pair, anchored_resolution = self._resolve_via_anchor_asset(
            symbol=normalized_symbol,
            at=at,
            visited=next_visited,
            attempted_pairs=attempted_pairs,
        )
        if anchored_pair is not None:
            return anchored_pair, anchored_resolution

        for bridge_quote in self.quote_priority:
            if bridge_quote in {"EUR", normalized_symbol}:
                continue
            pair_rate, pair_resolution = self._get_pair_rate(
                base=normalized_symbol,
                quote=bridge_quote,
                at=at,
                attempted_pairs=attempted_pairs,
            )
            if pair_rate is None:
                continue

            bridge_eur, bridge_resolution = self._resolve_unit_eur_value(
                symbol=bridge_quote,
                at=at,
                visited=next_visited,
                attempted_pairs=attempted_pairs,
            )
            if bridge_eur is not None:
                return pair_rate * bridge_eur, self._coarsest_resolution(pair_resolution, bridge_resolution)

        return None, None

    def _resolve_via_anchor_asset(
        self,
        *,
        symbol: str,
        at: datetime,
        visited: frozenset[str],
        attempted_pairs: set[str],
    ) -> tuple[Decimal | None, str | None]:
        for anchor_asset in self._iter_anchor_assets(symbol):
            if anchor_asset in visited:
                continue

            anchor_to_symbol, anchor_symbol_resolution = self._get_pair_rate(
                base=anchor_asset,
                quote=symbol,
                at=at,
                attempted_pairs=attempted_pairs,
            )
            if anchor_to_symbol in {None, Decimal("0")}:
                continue

            anchor_to_eur, anchor_eur_resolution = self._resolve_unit_eur_value(
                symbol=anchor_asset,
                at=at,
                visited=visited,
                attempted_pairs=attempted_pairs,
            )
            if anchor_to_eur is None:
                continue

            return (
                anchor_to_eur / anchor_to_symbol,
                self._coarsest_resolution(anchor_symbol_resolution, anchor_eur_resolution),
            )
        return None, None

    def _iter_anchor_assets(self, symbol: str) -> list[str]:
        normalized_symbol = symbol.upper()
        candidates: list[str] = []
        for candidate in self.quote_priority:
            normalized_candidate = candidate.upper()
            if normalized_candidate in {normalized_symbol, "EUR"}:
                continue
            if normalized_candidate in candidates:
                continue
            candidates.append(normalized_candidate)
        return candidates

    def _get_pair_rate(
        self,
        *,
        base: str,
        quote: str,
        at: datetime,
        attempted_pairs: set[str],
    ) -> tuple[Decimal | None, str | None]:
        direct_pair = f"{base}{quote}"
        direct_rate, direct_resolution = self._get_cached_or_backfilled_pair_rate(
            pair=direct_pair,
            at=at,
            attempted_pairs=attempted_pairs,
        )
        if direct_rate is not None:
            return direct_rate, direct_resolution

        inverse_pair = f"{quote}{base}"
        inverse_rate, inverse_resolution = self._get_cached_or_backfilled_pair_rate(
            pair=inverse_pair,
            at=at,
            attempted_pairs=attempted_pairs,
        )
        if inverse_rate in {None, Decimal("0")}:
            return None, None
        return Decimal("1") / inverse_rate, inverse_resolution

    def _get_cached_or_backfilled_pair_rate(
        self,
        *,
        pair: str,
        at: datetime,
        attempted_pairs: set[str],
    ) -> tuple[Decimal | None, str | None]:
        attempted_pairs.add(pair)
        cached_rate = self._lookup_pair_cache(pair=pair, at=at, resolution=self.resolution)
        if cached_rate is not None:
            return cached_rate, self.resolution

        if self._backfill_pair(pair=pair, at=at, resolution=self.resolution):
            resolved_rate = self._lookup_pair_cache(pair=pair, at=at, resolution=self.resolution)
            if resolved_rate is not None:
                return resolved_rate, self.resolution

        downgraded_resolution = self._downgraded_resolution(self.resolution)
        if not self.allow_resolution_downgrade or downgraded_resolution is None:
            return None, None

        downgraded_cached_rate = self._lookup_pair_cache(
            pair=pair,
            at=at,
            resolution=downgraded_resolution,
        )
        if downgraded_cached_rate is None and self._backfill_pair(pair=pair, at=at, resolution=downgraded_resolution):
            downgraded_cached_rate = self._lookup_pair_cache(
                pair=pair,
                at=at,
                resolution=downgraded_resolution,
            )
        if downgraded_cached_rate is None:
            return None, None

        self._warn_resolution_downgrade(
            pair=pair,
            requested_resolution=self.resolution,
            used_resolution=downgraded_resolution,
            at=at,
        )
        return downgraded_cached_rate, downgraded_resolution

    def _lookup_pair_cache(self, *, pair: str, at: datetime, resolution: str) -> Decimal | None:
        normalized_at = at.astimezone(timezone.utc)
        close_values: list[Decimal] = []
        for exchange in self.exchanges:
            cached = self.price_cache.get(
                provider=PAIR_PROVIDER,
                asset_id=f"{exchange}:{pair}",
                requested_at=normalized_at,
                resolution=resolution,
            )
            if cached is None:
                continue
            close_value = cached.get("close")
            if close_value is not None:
                close_values.append(close_value)
        if not close_values:
            return None
        return sum(close_values, Decimal("0")) / Decimal(len(close_values))

    def _backfill_pair(self, *, pair: str, at: datetime, resolution: str) -> bool:
        year = at.year if resolution == PriceResolution.MINUTE else None
        cache_key = (pair, resolution, at.strftime("%Y") if year is not None else "all", self.tax_year)
        if cache_key in self._attempted_backfills:
            return False
        self._attempted_backfills.add(cache_key)
        any_loaded = False

        for exchange in self.exchanges:
            try:
                frame = self._download_pair_frame(exchange=exchange, pair=pair, year=year, resolution=resolution)
            except FileNotFoundError:
                continue

            if frame.empty:
                continue

            price_points = self._frame_to_price_points(frame, resolution=resolution)
            if self.tax_year is not None:
                price_points = {
                    point_at: prices
                    for point_at, prices in price_points.items()
                    if point_at.year <= self.tax_year
                }
            if not price_points:
                continue

            self.price_cache.set_many(
                provider=PAIR_PROVIDER,
                asset_id=f"{exchange}:{pair}",
                resolution=resolution,
                price_points=price_points,
            )
            inverse_pair = self._invert_pair(pair)
            inverse_price_points = self._invert_price_points(price_points)
            if inverse_pair is not None and inverse_price_points:
                self.price_cache.set_many(
                    provider=PAIR_PROVIDER,
                    asset_id=f"{exchange}:{inverse_pair}",
                    resolution=resolution,
                    price_points=inverse_price_points,
                )
            any_loaded = True

        if any_loaded:
            return True

        return False

    def _download_pair_frame(self, *, exchange: str, pair: str, year: int | None, resolution: str) -> pd.DataFrame:
        suffix = self._build_suffix(year=year, resolution=resolution)
        url = f"{self.base_url}/{exchange}_{pair}_{suffix}.csv"
        cached_text = self._read_cached_csv_text(exchange=exchange, pair=pair, suffix=suffix)
        if cached_text is not None:
            return self._parse_csv_text(cached_text)
        response = self.session.get(url, timeout=self.timeout_seconds)
        if response.status_code == 404:
            raise FileNotFoundError(url)
        response.raise_for_status()
        self._write_cached_csv_text(exchange=exchange, pair=pair, suffix=suffix, text=response.text)
        return self._parse_csv_text(response.text)

    def _build_suffix(self, *, year: int | None, resolution: str) -> str:
        if resolution == PriceResolution.MINUTE:
            if year is None:
                raise ValueError("Minute-resolution backfills require a target year.")
            return f"{year}_{self.minute_suffix}"
        if resolution == PriceResolution.HOUR:
            return self.hour_suffix
        return self.day_suffix

    def _parse_csv_text(self, text: str) -> pd.DataFrame:
        lines = text.splitlines()
        header_index = None
        for index, line in enumerate(lines):
            normalized_line = line.strip().lower()
            if normalized_line.startswith("unix,") or normalized_line.startswith("date,"):
                header_index = index
                break
        if header_index is None:
            return pd.DataFrame()

        frame = pd.read_csv(StringIO("\n".join(lines[header_index:])))
        frame.columns = [str(column).strip().lower() for column in frame.columns]
        return frame

    def _frame_to_price_points(self, frame: pd.DataFrame, *, resolution: str) -> dict[datetime, dict[str, Decimal]]:
        if "close" not in frame.columns:
            raise ValueError("CryptoDataDownload CSV is missing the close column.")

        price_points: dict[datetime, dict[str, Decimal]] = {}
        for _, series in frame.iterrows():
            timestamp = self._parse_row_timestamp(series)
            if timestamp is None:
                continue
            bucketed_at = normalize_price_timestamp(timestamp, resolution)
            price_points[bucketed_at] = {"close": Decimal(str(series["close"]))}
        return price_points

    def _parse_row_timestamp(self, series: pd.Series) -> datetime | None:
        if "unix" in series and not pd.isna(series["unix"]):
            unix_value = int(str(series["unix"]).split(".")[0])
            return self._normalize_unix_timestamp(unix_value)
        if "date" in series and not pd.isna(series["date"]):
            return pd.to_datetime(series["date"], utc=True).to_pydatetime().astimezone(timezone.utc)
        return None

    def _normalize_symbol(self, symbol: str) -> str:
        normalized = symbol.strip().upper()
        return self.symbols.get(normalized, normalized)

    def _warn_once(self, *, key: str, message: str) -> None:
        if key in self._warning_keys:
            return
        self._warning_keys.add(key)
        self._warnings.append(message)
        self._log(message)

    def _normalize_unix_timestamp(self, unix_value: int) -> datetime:
        normalized_value = unix_value
        while abs(normalized_value) > 10_000_000_000:
            normalized_value = normalized_value // 1000

        try:
            return datetime.fromtimestamp(normalized_value, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(f"Unsupported unix timestamp in CryptoDataDownload data: {unix_value}") from exc

    def _log(self, message: str) -> None:
        if self.event_logger is not None:
            self.event_logger(message)

    def _csv_cache_path(self, *, exchange: str, pair: str, suffix: str) -> Path:
        safe_exchange = exchange.replace("/", "_")
        safe_pair = pair.replace("/", "_")
        return self.csv_cache_dir / f"{safe_exchange}_{safe_pair}_{suffix}.csv"

    def _read_cached_csv_text(self, *, exchange: str, pair: str, suffix: str) -> str | None:
        cache_path = self._csv_cache_path(exchange=exchange, pair=pair, suffix=suffix)
        if not cache_path.exists():
            return None
        modified_at = datetime.fromtimestamp(cache_path.stat().st_mtime, tz=timezone.utc).date()
        today = self.now_provider().astimezone(timezone.utc).date()
        if modified_at != today:
            return None
        return cache_path.read_text(encoding="utf-8")

    def _write_cached_csv_text(self, *, exchange: str, pair: str, suffix: str, text: str) -> None:
        cache_path = self._csv_cache_path(exchange=exchange, pair=pair, suffix=suffix)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(text, encoding="utf-8")
        now_timestamp = self.now_provider().astimezone(timezone.utc).timestamp()
        os.utime(cache_path, (now_timestamp, now_timestamp))

    def _invert_pair(self, pair: str) -> str | None:
        pair_upper = pair.upper()
        known_quotes = sorted(
            {"EUR", "USD", "USDT", "BTC", *self.quote_priority},
            key=len,
            reverse=True,
        )
        for quote in known_quotes:
            if not pair_upper.endswith(quote):
                continue
            base = pair_upper[: -len(quote)]
            if not base:
                return None
            return f"{quote}{base}"
        return None

    def _invert_price_points(
        self,
        price_points: dict[datetime, dict[str, Decimal]],
    ) -> dict[datetime, dict[str, Decimal]]:
        inverted: dict[datetime, dict[str, Decimal]] = {}
        for point_at, prices in price_points.items():
            close_value = prices.get("close")
            if close_value in {None, Decimal("0")}:
                continue
            inverted[point_at] = {"close": Decimal("1") / close_value}
        return inverted

    def _downgraded_resolution(self, resolution: str) -> str | None:
        if resolution == PriceResolution.MINUTE:
            return PriceResolution.HOUR
        if resolution == PriceResolution.HOUR:
            return PriceResolution.DAY
        return None

    def _warn_resolution_downgrade(
        self,
        *,
        pair: str,
        requested_resolution: str,
        used_resolution: str,
        at: datetime,
    ) -> None:
        if self._downgrade_warning_emitted:
            return
        self._downgrade_warning_emitted = True
        self._warn_once(
            key="downgrade:summary",
            message=(
                "Some historical prices were resolved using lower-resolution backfill data because "
                "allow_backfill_resolution_downgrade is enabled. This can reduce accuracy in the tax results."
            ),
        )

    def describe_last_resolution_failure(self, symbol: str, at: datetime) -> str | None:
        if self._last_failed_resolution is None:
            return None
        normalized_symbol = self._normalize_symbol(symbol)
        normalized_at = at.astimezone(timezone.utc).isoformat()
        if (
            self._last_failed_resolution["symbol"] != normalized_symbol
            or self._last_failed_resolution["timestamp"] != normalized_at
        ):
            return None
        attempted_pairs = self._last_failed_resolution.get("pairs") or "-"
        return (
            f"Local CryptoDataDownload backfill could not resolve {normalized_symbol} at {normalized_at}. "
            f"Tried local pairs: {attempted_pairs}."
        )

    def _coarsest_resolution(self, left: str | None, right: str | None) -> str | None:
        resolution_rank = {
            PriceResolution.MINUTE: 0,
            PriceResolution.HOUR: 1,
            PriceResolution.DAY: 2,
        }
        candidates = [value for value in (left, right) if value is not None]
        if not candidates:
            return None
        return max(candidates, key=lambda value: resolution_rank[value])
