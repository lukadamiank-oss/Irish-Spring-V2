"""Check the frozen price snapshot and optionally compare every Parquet value."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()

def check(verify_files=False):
    data = ROOT / 'data/price_history'
    baseline = json.loads((data / 'audits/conversion_validation.json').read_text())
    stack = data / 'price_history.parquet'
    assert digest(stack) == baseline['stacked_sha256'], 'Stacked snapshot checksum changed'
    with (data / 'audits/conversion_manifest.csv').open(newline='') as f:
        manifest = list(csv.DictReader(f))
    individuals = set((data / 'by_security').glob('*.parquet'))
    assert individuals == {data / r['parquet_file'] for r in manifest}
    assert len(individuals) == baseline['source_file_count']
    with duckdb.connect(str(ROOT / 'database/_database.db'), read_only=True) as con:
        stats = con.execute('''SELECT count(*), count(DISTINCT security_id),
          count(*) FILTER (WHERE model_usable), count(*) FILTER (WHERE NOT model_usable),
          min(date), max(date) FROM price_history''').fetchone()
        assert stats[:4] == tuple(baseline[k] for k in ['row_count','security_count','model_usable_row_count','invalid_row_count'])
        assert str(stats[4]) == baseline['first_date'] and str(stats[5]) == baseline['last_date']
        duplicates = con.execute('''SELECT count(*) FROM (SELECT security_id, date
          FROM price_history GROUP BY ALL HAVING count(*) > 1)''').fetchone()[0]
        assert duplicates == baseline['duplicate_security_date_groups']
    if verify_files:
        parquet = pq.ParquetFile(stack)
        assert parquet.num_row_groups == len(manifest)
        for row in manifest:
            path = data / row['parquet_file']
            assert digest(path) == row['parquet_sha256'], path
            table = pq.read_table(path)
            assert table.num_rows == int(row['row_count'])
            assert table.equals(parquet.read_row_group(int(row['stacked_row_group']))), path
    reference = ROOT / 'data/reference/final_security_manifest'
    freeze = json.loads((reference / 'final_security_manifest_v1_FREEZE.json').read_text())
    assert digest(reference / 'final_security_manifest_v1.csv') == freeze['sha256']
    print(json.dumps({'rows':stats[0], 'securities':stats[1], 'usable_rows':stats[2],
        'invalid_rows':stats[3], 'first_date':str(stats[4]), 'last_date':str(stats[5]),
        'duplicate_security_date_groups':duplicates, 'individual_parquet_files':len(individuals),
        'all_individual_values_compared':verify_files, 'checks_passed':True},indent=2))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-files', action='store_true')
    check(parser.parse_args().verify_files)
