"""Register the local Parquet file in DuckDB; rerun after moving the project."""
import argparse
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

def initialize(root: Path = ROOT) -> int:
    root = root.resolve()
    parquet = root / 'data/price_history/price_history.parquet'
    if not parquet.is_file():
        raise FileNotFoundError(parquet)
    database = root / 'database/_database.db'
    database.parent.mkdir(parents=True, exist_ok=True)
    template = (root / 'sql/create_price_history_view.sql').read_text()
    sql = template.replace('{{PRICE_HISTORY_PATH}}', str(parquet).replace("'", "''"))
    with duckdb.connect(str(database)) as con:
        con.execute('BEGIN')
        try:
            con.execute(sql)
            rows = con.execute('SELECT count(*) FROM price_history').fetchone()[0]
            con.execute('COMMIT')
        except Exception:
            con.execute('ROLLBACK')
            raise
    return rows

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    args = parser.parse_args()
    print(f'Registered price_history: {initialize(args.root):,} rows')
