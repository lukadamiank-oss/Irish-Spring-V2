-- Rendered by python/init_db.py with the current project location.
CREATE OR REPLACE VIEW price_history AS
SELECT * FROM read_parquet('{{PRICE_HISTORY_PATH}}');
