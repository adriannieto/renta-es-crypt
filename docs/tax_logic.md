# Tax Logic

This document describes the tax assumptions currently implemented by the tool.

## FIFO

FIFO is tracked independently per asset.

For an acquisition lot:

```text
total_cost_eur = (amount * price_eur) + fee_eur
unit_cost_eur = total_cost_eur / amount
```

For a disposal:

```text
transmission_value_eur = (amount_sold * sale_price_eur) - prorated_sale_fee_eur
gain_or_loss_eur = transmission_value_eur - acquisition_value_eur
```

Spanish fee treatment implemented by default:

- acquisition fees increase acquisition value
- sale fees reduce transmission value

## Internal Transfers

Internal transfers are matched when:

- the asset is the same
- timestamps fall within the configured time window
- the received amount falls within the configured percentage tolerance

Matched pairs are marked as internal transfers and do not generate gains.

Unmatched transfers are always warned about.

Default behavior:

- unmatched inbound transfers block the run after all such records are listed
- unmatched `TRANSFER_OUT` records remain warnings only

Optional relaxed fallbacks:

- `reporting.unmatched_transfer_in_mode: warn`
- unmatched inbound transfers remain warnings only
- this avoids a hard stop, but leaves inventory incomplete and must be reviewed manually

- `reporting.unmatched_transfer_in_mode: zero_cost_basis`
- unmatched inbound transfers are inserted into FIFO with `0 EUR` acquisition value
- later sales of those assets will therefore treat the full net proceeds as gain
- this can overstate gains, may cause the taxpayer to pay more tax than reality, and may not be fully compliant with Spanish tax law

## Two-Month Rule

The engine detects loss-making disposals that have a same-asset repurchase within the configured plus/minus two-month window, excluding the original lot that was sold.

Default mode:

- affected rows are flagged with `Wash-Sale-Warning`

Optional override:

- `reporting.two_month_rule_mode: disabled`
- CLI equivalent: `--disable-two-month-rule`
- the engine still detects the situation and keeps warnings
- affected rows are marked with `Two-Month-Rule-Disabled-Warning`
- this can produce a result more favorable than a conservative treatment

## Permuta

Crypto-to-crypto swaps are treated as `permuta`.

Current implementation:

- a swap row is expanded into two simultaneous actions
- sale of the asset delivered
- purchase of the asset received

The disposal leg is included in capital gains, and the acquired asset enters FIFO as a new lot.

Trade valuation rules:

- if one side of the trade is fiat, the parser does not treat it as permuta
- fiat to crypto becomes a `BUY`
- crypto to fiat becomes a `SELL`
- the unit EUR price is derived from the fiat leg divided by the crypto amount
- if the fiat leg is not EUR, the fiat amount is converted to EUR first
- only crypto-to-crypto trades use historical price feeds
- for crypto-to-crypto, the EUR valuation is taken from the asset being sold, and the acquired leg inherits that EUR total for FIFO consistency

## Staking

`STAKE_REWARD` is treated with dual logic:

- income at market value on receipt
- a new FIFO lot at that same EUR value

This income is exported in the staking schedule.

## Airdrops

`AIRDROP` is tracked separately from staking.

Current implementation:

- record a separate airdrop income item
- add the received asset to FIFO at market value on receipt
- tag later disposals of that lot with `Airdrop`

The tool treats the receipt as a gain not derived from a prior transmission, values it at market value on receipt, and keeps it separate from staking income and from the savings-base transmission schedule.

## Ignored Assets

You can exclude problematic assets globally with:

```yaml
ignored_assets:
  - BSV
```

CLI equivalent:

```bash
python3 -m src \
  --ignore-asset BSV \
  --ignore-asset BCH \
  --input kraken:work/kraken.csv
```

This is a hard exclusion. The tool drops:

- transactions whose `asset` is ignored
- transactions whose `counter_asset` is ignored

That means a crypto-to-crypto trade involving an ignored asset is excluded completely rather than leaving one side of the trade behind.

Important warning:

- ignored assets can create inventory mismatches and eventually block the calculation
- this is especially relevant for `permuta` and other crypto-to-crypto trades
- example: if you ignore a `TRUMP/USDT` trade, the associated `USDT` acquisition or disposal also disappears from FIFO
- if you later try to sell or spend that `USDT`, the engine can fail with `Insufficient inventory` because that lot never entered inventory

Use `ignored_assets` only when you explicitly accept excluding the full tax and inventory impact of those assets and of the related transactions.
