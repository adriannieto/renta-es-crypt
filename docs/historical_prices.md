# Historical Prices

Historical price configuration lives under `pricing` in [`src/defaults.yaml`](./src/defaults.yaml).

## Resolution Order

Historical valuation follows this order:

1. explicit EUR values already present in the CSV
2. trade pairs already implied by the exchange input
3. locally backfilled pair data from CryptoDataDownload
4. CoinGecko as external fallback

## Pre-Heating

Before transactions are parsed into tax events, the CLI pre-heats the price cache from the raw inputs:

- it scans the input files for the assets and timestamps that will likely need historical prices
- it resolves those assets with the configured backfill strategy first
- it does not use the external provider during this pre-heating phase
- once that pass is done, the actual parsing and engine run reuse the in-memory cache built during that stage

Typical output:

```text
Pre-heating price cache from CryptoDataDownload
Tickers populated: ADAEUR, ATOMEUR, XTZEUR
Tickers downgraded:
...
```

Normal cache hits and misses are intentionally not logged because they are too noisy in large historical imports.

## CryptoDataDownload Backfill

The `cryptodatadownload` backfill provider downloads CSVs on demand and stores them in the shared local cache directory.

It tries:

- direct EUR pairs first
- inverse-pair resolution when only the opposite quote exists
- then other bridge and anchor routes using the configured `quote_priority`

This can resolve paths such as:

- `ETHUSD * USDEUR`
- `ETHBTC * BTCEUR`
- other best-effort combinations available from cached or backfilled pairs

The route is not hardcoded to one strict order after the direct EUR attempt. The resolver tries the locally available pairs, inverse pairs, and bridge routes derived from `quote_priority` until it finds a valid EUR path.

When several exchanges provide the same pair for the same time bucket, the backfill uses the average of the available close prices instead of picking only the first exchange. This helps build a richer historical dataset and also fills gaps when one exchange is missing a bucket but another exchange has it.

The local cache also stores inverse pairs when a CSV is backfilled. That means:

- if `EURUSD` is available, the cache can later resolve `USDEUR` without downloading a second file
- if `ETHBTC` is available, the cache can later resolve `BTCETH` from the same backfilled data

`symbols` in `pricing.cryptodatadownload` is only a ticker normalization map. It exists for cases where your local asset symbol and the CryptoDataDownload pair symbol differ. It does not define bridge assets and does not expand the triangulation search space.

## Cache Behavior

The final resolved EUR cache used during one execution is in-memory only and is rebuilt on each run.

To avoid repeated network downloads:

- successful CryptoDataDownload CSV fetches are cached locally on disk
- if the same CSV was already downloaded today, the backfill reuses the local CSV copy instead of downloading it again

External provider lookups are also persisted on disk:

- they are stored as multiple small JSON files
- they are split by provider, asset, and resolution
- this avoids one huge JSON file and lets the tool reuse historical external results across executions

## Backfill Provider

You can disable local pre-heating and backfill entirely with:

- setting: `pricing.backfill_provider: none`
- CLI: `--backfill-provider none`

In that mode, the tool skips CryptoDataDownload pre-heating and goes directly to the external provider whenever the input itself does not already provide the needed EUR valuation.

## Resolution

Lower historical price resolutions reduce local cache misses but also reduce temporal precision.

Default:

- `hour`

Why:

- it is a practical compromise for tax work
- it avoids silently collapsing everything to daily candles
- it keeps the local cache useful without requiring massive minute datasets

Using `day` can materially alter gain/loss calculations if the same asset is bought and sold several times during one day.

Using `minute` can improve precision, but yearly minute files are often incomplete and not every pair and year exists.

## Resolution Downgrade

Config:

```yaml
pricing:
  allow_backfill_resolution_downgrade: false
```

CLI:

```bash
--allow-backfill-resolution-downgrade
```

When disabled, the tool behaves strictly:

- if the requested pair still cannot be resolved at the configured resolution, that pair is not filled locally with a lower resolution
- the resolver then tries the external provider

When enabled, the backfill layer may downgrade one step only:

- `minute -> hour`
- `hour -> day`

This mode can materially smooth price action and alter gain/loss calculations. The CLI therefore reports which tickers were resolved with downgraded resolution.

## CoinGecko Fallback

The `coingecko` external provider is only used when the local cache still cannot resolve the requested value after trying direct pairs and triangulation.

It requires:

- `pricing.coingecko.api_key`
- explicit mappings in `pricing.coingecko.coin_ids` for non-trivial tickers

Example:

```yaml
pricing:
  coingecko:
    api_key: your_key
    coin_ids:
      BTC: bitcoin
      ETH: ethereum
      TRUMP: official-trump
```

If the mapping is missing, the tool fails explicitly instead of attempting the raw ticker against CoinGecko.

If CoinGecko is reached, remember that it is a fallback path, not the primary valuation source for the tool.
