# Irish Spring V2

SQL validation and ongoing data engineering over the existing Irish Spring price
snapshot. No new prices have been fetched. The old v0/v1 work and historical
`rebuild_v2` remain in the neighboring `Irish spring v0, v1` directory.

## Start

From this directory:

```sh
uv sync
uv run python python/init_db.py
uv run python python/check_data.py --verify-files
```

Open `database/_database.db` with a DuckDB SQL client, then use `sql/queries.sql`.

```sql
SELECT security_id, date, ticker, adjusted_close
FROM price_history
WHERE model_usable
ORDER BY security_id, date
LIMIT 20;
```

`python/init_db.py` derives the project root from its own location. It registers
the stacked Parquet in DuckDB without copying the price data into a database
table. Run it again after relocating the project. The SQL view definition is a
template rendered by this script, not a standalone query to execute unchanged.

## Layout

- `data/price_history/price_history.parquet`: stacked baseline snapshot.
- `data/price_history/by_security/`: all 911 individual Parquet files, retained.
- `data/price_history/audits/`: original conversion/deletion receipts, copied price notes, and V2 migration receipt.
- `data/reference/final_security_manifest/`: frozen identity manifest with membership and ticker representation intervals.
- `data/reference/security_catalog/`: identities and corporate-action notes.
- `data/reference/membership/`: reconciled source intervals and documentation.
- `data/reference/source_resolution/`: existing vendor/source selection evidence for future refresh work.
- `database/_database.db`: local DuckDB database exposing `price_history`.
- `sql/`: view template and evaluation queries.
- `python/`: initialization and baseline validation commands.
- `python/archive/import_prices.py`: historical CSV conversion recipe; requires explicit source and destination paths. Original finalized CSVs were deleted after validation.
- `docs/archive/data_workspace_README.md`: documentation from the previous data workspace.

## Baseline and scope

The imported snapshot contains 4,017,983 rows for 911 securities, ending on
2026-08-26. Of these, 4,017,283 rows are model-usable and 700 explicitly document
invalid observations. All 28 columns and their types are retained.

Use `model_usable` when observed prices are required. This flag does not indicate
S&P membership; membership tests must use the reference intervals. The security
manifest has 912 IDs, including the unresolved Marshall & Ilsley security and a
synthetic price-lineage entry; it is not interchangeable with a current index list.

The check command verifies baseline hashes, counts, dates, duplicate keys, and the
frozen manifest. `--verify-files` additionally compares all individual Parquet
values with the corresponding stacked row groups. These are preservation checks,
not a claim that vendor history or corporate-action handling is fully validated.

Do not scan both individual and stacked Parquet files in the same query: that
would double count the data. Keep both versions until a separate cleanup decision.

Historical provenance records retain their original paths and hashes. Those
paths describe where evidence originated; they are not active V2 dependencies.
Source-resolution ledgers reference raw files retained in the historical rebuild,
which can be consulted when investigating a specific issue. The migration receipt
records the new location and hashes of copied references.

## Development and version control

The project has its own local Git repository. No remote, commit, or push is required
for initialization. `.gitignore` excludes data, database files, credentials, and
environments; preserve separate data backups before any future cleanup.

Start by adding SQL checks and derived datasets while keeping this baseline
unchanged. A refresh pipeline has not been implemented or run. Future updates
should produce candidates, preserve reviewed source mappings, and pass comparisons
before promotion.

Dependencies are pinned in `pyproject.toml` and resolved in `uv.lock`. The existing
Python requirements file is retained for compatibility with the conversion recipe.
