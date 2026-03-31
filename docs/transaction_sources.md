# Transaction Sources

## Kraken

The tool currently expects the Kraken `Ledger` export as the main input source.

To generate the CSV:

- go to Kraken website
- open `Documents`
- create a new export
- choose `Ledger`
- choose `CSV`

Recommended export range:

- from the first historical transaction in the account
- to at least December 31 of the tax year you want to calculate

## Normalization

Kraken-specific rows are normalized into canonical transactions with:

- `timestamp`
- `asset`
- `type`
- `amount`
- `price_eur`
- `fee_eur`

All timestamps are normalized to UTC.

## Kraken-Specific Handling

The parser contains explicit handling for Kraken ledger behaviors such as:

- grouped `trade` rows paired by `refid`
- fiat/crypto trades
- crypto/crypto trades treated as permuta
- staking rewards
- airdrops
- internal staking and earn migrations
- allocation and deallocation movements
- transfer matching candidates

It also normalizes Kraken asset variants used in earn and staking contexts, such as:

- `.S`, `.M`, `.B`, `.F`, `.P`, `.T` balance suffixes
- internal numeric program codes like `ATOM21.S` or `SOL03.S`

Those variants are reduced to the economic base asset only in the relevant earn/staking contexts, so unrelated assets such as `LUNA2` are preserved.

## Parser Warnings

The parser can skip Kraken rows or groups that do not represent a modeled spot-tax event, for example:

- margin rows
- delisting conversion transfer rows
- dust sweeping consolidation groups
- incomplete grouped trade records without a complete debit/credit pair

In those cases, the process continues but emits summarized warnings so the user can review them.
