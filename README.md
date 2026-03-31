# renta-es-crypt

CLI tool to calculate Spanish crypto capital gains and losses for AEAT-style reporting.

## Disclaimer

This tool is a public helper for taxpayers working from home. It is still under active development and it is not legal or tax advice.

By using it, you remain solely responsible for reviewing the inputs, validating the calculations, and deciding what to file. The authors and contributors assume no responsibility or liability for losses, penalties, claims, filing mistakes, incorrect tax logic, incomplete data, software defects, or any other damages caused by using the tool.

## Docs

- Tax logic: [docs/tax_logic.md](./docs/tax_logic.md)
- Transaction sources: [docs/transaction_sources.md](./docs/transaction_sources.md)
- Historical prices: [docs/historical_prices.md](./docs/historical_prices.md)

## Usage

Default configuration lives in [src/defaults.yaml](src/defaults.yaml), use `--config=file.yaml` to specify another config file. Note that CLI overrides always take precedence.

> The tool can be run without Docker with `python3 -m src`

```bash
docker build -t renta-es-crypt .
docker run --rm \
  -v "$PWD/work:/work" \
  renta-es-crypt \
  --input kraken:/work/kraken.csv \
  --output-dir /work/ \
  --cache-dir /work/
```

## Development

In case you want to use a real Kraken Ledger export in unit tests, run `python3 scripts/anonymize_kraken_ledger_ids.py <CSV_FILE_TO_OVERRIDE>` before pushing it to any public source

