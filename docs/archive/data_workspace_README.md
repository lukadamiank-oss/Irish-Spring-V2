# Irish Spring Data

Finalized historical price CSVs copied to Parquet for DuckDB queries. The source
is `rebuild_v2/data/processed/eodhd/final_security_prices/v1` in the original
Irish Spring project. Raw download attempts and superseded candidate files are
not part of this export.

## Files

- `data/price_history/by_security/`: one Parquet file for each finalized source CSV.
- `data/price_history/price_history.parquet`: all individual files stacked in security-ID order, preserving their original row order.
- `database/_database.db`: DuckDB database containing the `price_history` view, which reads the stacked file without storing another copy of the prices.
- `sql/queries.sql`: basic inspection and evaluation queries.
- `sql/create_price_history_view.sql`: view definition with the absolute Parquet path.
- `data/price_history/audits/conversion_validation.json`: conversion checks and aggregate counts.
- `data/price_history/audits/conversion_manifest.csv`: per-file source/output hashes, counts, and stacked row-group positions.
- `data/price_history/audits/source_price_notes/`: original per-security provenance notes, copied without editing.

## Query

Open `database/_database.db` in a DuckDB-compatible SQL client. For example:

```sql
SELECT security_id, date, ticker, adjusted_close
FROM price_history
WHERE model_usable
ORDER BY security_id, date
LIMIT 20;
```

The included Python environment can also query the database:

```python
import duckdb
connection = duckdb.connect(
    '/Users/lukakirigin/dev/Irish_Spring_Data/database/_database.db',
    read_only=True,
)
print(connection.sql('SELECT count(*) FROM price_history').fetchall())
```

Run Python with `.venv/bin/python`. The view uses an absolute path so the working
directory does not matter. If the project moves, update the path in the view
definition and recreate the view at the new location.

## Preservation and types

All 28 source columns and every source row are retained. No deduplication,
price adjustment, date trimming, membership filtering, or price imputation was
performed. Dates may precede S&P membership because source files include verified
lookback history. `model_usable` indicates price usability, not index membership.

- `date`: SQL DATE.
- `model_usable`: BOOLEAN.
- OHLC, adjusted close, and volume: DOUBLE, consistent with floating-point source processing. Numeric text is parsed to IEEE-754 doubles without additional rounding.
- `vendor_segment_ordinal`: nullable INTEGER.
- Remaining columns: VARCHAR; original empty text remains empty text.
- Empty numeric fields remain NULL, never zero. Metadata date columns remain text to preserve their source representation.

Existing invalid rows are retained with blank prices and their exclusion reasons.
Use `WHERE model_usable` for analyses that require observed prices.

Each source CSV hash and row count was checked against the existing finalized
manifest. Every typed value was compared after individual Parquet round-trip,
then every stacked row group was compared with its individual Parquet file.
DuckDB checks cover counts, duplicate security/date keys, and existing final-price
row-status/null-price rules. These are conversion checks, not a fresh validation
of vendor accuracy or corporate-action treatment.

Do not scan both the individual copies and the stacked file together: that would
count every row twice. After evaluation, the individual Parquet copies can be
removed without changing the `price_history` view. The 911 finalized source price CSVs were deleted after a second complete comparison against both Parquet copies. Raw vendor CSVs, manifests, and provenance files were retained. See `audits/csv_deletion_receipt.json` under `data/price_history/` for the validation and deletion record.

`python/import_prices.py` records the reproducible conversion and refuses to
overwrite an existing export. Dependencies are pinned in `python/requirements.txt`.

The importer is retained as the historical conversion recipe. Its original finalized CSV inputs have now been removed at your request; rerunning it requires restoring those inputs. Both individual and stacked Parquet files remain available.
