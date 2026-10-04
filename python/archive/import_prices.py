"""Copy finalized CSVs to individual and stacked Parquet, with lossless round-trip checks.

Run once in the destination's .venv. Refuses to overwrite an existing export.
"""
import argparse
import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

PRICE_COLUMNS = ['open', 'high', 'low', 'close', 'adjusted_close', 'volume']
COLUMNS = ['security_id', 'security_name', 'date', 'ticker', 'eodhd_symbol', *PRICE_COLUMNS,
           'source_download_status', 'source_download_reason', 'source_output_path', 'source_sha256',
           'representation_start', 'representation_end', 'requested_from', 'requested_to',
           'actual_first_price_date', 'actual_last_price_date', 'vendor_segment_ordinal',
           'vendor_segment_basis', 'final_row_status', 'model_usable', 'model_exclusion_reason',
           'invalid_reason', 'invalid_detail']
TYPES = {name: pa.string() for name in COLUMNS}
TYPES.update({name: pa.float64() for name in PRICE_COLUMNS})
TYPES.update(date=pa.date32(), model_usable=pa.bool_(), vendor_segment_ordinal=pa.int32())
SCHEMA = pa.schema([(name, TYPES[name]) for name in COLUMNS])

def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def quote(value):
    return "'" + str(value).replace("'", "''") + "'"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    source, dest = args.source.resolve(), args.destination.resolve()
    output = dest / 'data' / 'price_history'
    stage = dest / 'data' / '.price_history_importing'
    if output.exists() or stage.exists():
        raise RuntimeError('Export or staging directory already exists; refusing to overwrite.')
    with (source / 'audits/final_security_price_manifest.csv').open(newline='') as f:
        manifest = list(csv.DictReader(f))
    expected = {r['security_id']: r for r in manifest}
    files = sorted((source / 'by_security').glob('*/*__final_prices.csv'))
    assert len(expected) == len(manifest) == len(files), 'Source file/manifest count mismatch'
    assert {p.parent.name for p in files} == set(expected), 'Source IDs mismatch'
    stage.mkdir(parents=True)
    individual = stage / 'by_security'
    individual.mkdir()
    audit = stage / 'audits'
    audit.mkdir()
    notes = audit / 'source_price_notes'
    notes.mkdir()
    stacked = stage / 'price_history.parquet'
    records = []
    options = pacsv.ConvertOptions(column_types=TYPES, null_values=[''], strings_can_be_null=False,
                                  true_values=['True', 'true'], false_values=['False', 'false'])
    with pq.ParquetWriter(stacked, SCHEMA, compression='zstd', version='2.6') as writer:
        for index, path in enumerate(files):
            sid = path.parent.name
            entry = expected[sid]
            source_hash = sha(path)
            assert source_hash == entry['final_price_sha256'], f'Stale source manifest: {sid}'
            table = pacsv.read_csv(path, convert_options=options)
            assert table.schema == SCHEMA, f'Schema mismatch: {sid}'
            assert table.num_rows == int(entry['row_count']), f'Row count mismatch: {sid}'
            assert pc.unique(table['security_id']).to_pylist() == [sid], f'Identity mismatch: {sid}'
            assert table['date'].null_count == table['model_usable'].null_count == 0
            usable = pc.sum(pc.cast(table['model_usable'], pa.int64())).as_py()
            assert usable == int(entry['model_usable_row_count'])
            assert table.num_rows - usable == int(entry['invalid_row_count'])
            target = individual / (path.stem + '.parquet')
            pq.write_table(table, target, compression='zstd', version='2.6')
            assert table.equals(pq.read_table(target)), f'Parquet round-trip mismatch: {sid}'
            writer.write_table(table, row_group_size=table.num_rows)
            assert sha(path) == source_hash, f'Source changed during conversion: {sid}'
            note = path.with_name(sid + '__price_notes.json')
            if note.exists():
                shutil.copy2(note, notes / note.name)
            records.append(dict(security_id=sid, source_csv=str(path), source_sha256=source_hash,
                                parquet_file=str(Path('by_security') / target.name),
                                parquet_sha256=sha(target), stacked_row_group=index,
                                row_count=table.num_rows, model_usable_row_count=usable,
                                invalid_row_count=table.num_rows-usable))
            if (index+1) % 100 == 0 or index+1 == len(files):
                print(f'Converted and verified {index+1}/{len(files)} files', flush=True)
    parquet = pq.ParquetFile(stacked)
    assert parquet.num_row_groups == len(records)
    for record in records:
        assert parquet.read_row_group(record['stacked_row_group']).equals(
            pq.read_table(stage / record['parquet_file'])), 'Stacked value mismatch'
    c = duckdb.connect()
    c.execute(f'CREATE VIEW prices AS SELECT * FROM read_parquet({quote(stacked)})')
    stats = c.execute('''SELECT count(*) AS row_count, count(DISTINCT security_id) AS securities,
        count(*) FILTER (WHERE model_usable) usable_rows,
        count(*) FILTER (WHERE NOT model_usable) invalid_rows,
        min(date) first_date, max(date) last_date FROM prices''').fetchone()
    duplicates = c.execute('''SELECT count(*) FROM
        (SELECT security_id, date FROM prices GROUP BY ALL HAVING count(*) > 1)''').fetchone()[0]
    invalid_prices = ' OR '.join(f'{n} IS NOT NULL' for n in PRICE_COLUMNS)
    missing_prices = ' OR '.join(f'{n} IS NULL OR NOT isfinite({n})' for n in PRICE_COLUMNS)
    bad = c.execute(f'''SELECT count(*) FROM prices WHERE
       (model_usable AND (final_row_status <> 'valid_price_observation' OR {missing_prices}))
       OR (NOT model_usable AND (final_row_status <> 'invalid_no_price_observation'
           OR {invalid_prices} OR coalesce(model_exclusion_reason, '') = ''
           OR coalesce(invalid_reason, '') = ''))''').fetchone()[0]
    assert stats[0] == sum(r['row_count'] for r in records)
    assert stats[1] == len(files)
    assert bad == 0, f'{bad} rows fail existing final-price validation rules'
    c.close()
    report = dict(created_utc=datetime.now(timezone.utc).isoformat(), source_directory=str(source),
                  source_file_count=len(files), row_count=stats[0], security_count=stats[1],
                  model_usable_row_count=stats[2], invalid_row_count=stats[3],
                  first_date=str(stats[4]), last_date=str(stats[5]), duplicate_security_date_groups=duplicates,
                  final_price_validation_failures=bad, all_source_manifest_hashes_match=True,
                  all_individual_values_match_typed_csv=True, all_stacked_values_match_individual=True,
                  deduplication_performed=False, stacked_sha256=sha(stacked),
                  individual_bytes=sum(p.stat().st_size for p in individual.iterdir()),
                  stacked_bytes=stacked.stat().st_size, schema={f.name:str(f.type) for f in SCHEMA},
                  duckdb_version=duckdb.__version__, pyarrow_version=pa.__version__)
    (audit / 'conversion_validation.json').write_text(json.dumps(report, indent=2)+'\n')
    with (audit / 'conversion_manifest.csv').open('w', newline='') as f:
        w=csv.DictWriter(f, fieldnames=list(records[0]));w.writeheader();w.writerows(records)
    for name in ['final_security_price_manifest.csv','final_security_price_summary.json']:
        shutil.copy2(source / 'audits' / name, audit / ('source_' + name))
    stage.rename(output)
    final_stack = output / 'price_history.parquet'
    view_sql = f'CREATE VIEW price_history AS SELECT * FROM read_parquet({quote(final_stack)});\n'
    (dest / 'sql' / 'create_price_history_view.sql').write_text(view_sql)
    database = dest / 'database' / '_database.db'
    c = duckdb.connect(str(database))
    try:
        c.execute('BEGIN')
        c.execute(view_sql)
        assert c.execute('SELECT count(*) FROM price_history').fetchone()[0] == stats[0]
        c.execute('COMMIT')
    except Exception:
        c.execute('ROLLBACK')
        raise
    finally:
        c.close()
    print(json.dumps(report, indent=2), flush=True)

if __name__ == '__main__':
    main()
