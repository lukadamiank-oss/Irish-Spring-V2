-- Open database/_database.db with DuckDB, then run these queries.
-- The view reads only the stacked file, avoiding double counting the individual copies.
SELECT count(*) AS row_count,
       count(DISTINCT security_id) AS securities,
       min(date) AS first_date,
       max(date) AS last_date,
       count(*) FILTER (WHERE model_usable) AS usable_rows,
       count(*) FILTER (WHERE NOT model_usable) AS invalid_rows
FROM price_history;

SELECT security_id, security_name, count(*) AS row_count,
       min(date) AS first_date, max(date) AS last_date,
       count(*) FILTER (WHERE NOT model_usable) AS invalid_rows
FROM price_history
GROUP BY security_id, security_name
ORDER BY security_id;

SELECT date, ticker, open, high, low, close, adjusted_close, volume
FROM price_history
WHERE security_id = 'GS_SECURITY_000001' AND model_usable
ORDER BY date DESC
LIMIT 20;

-- Review duplicate security/date keys; no rows were deduplicated during import.
SELECT security_id, date, count(*) AS copies
FROM price_history
GROUP BY security_id, date
HAVING count(*) > 1
ORDER BY security_id, date;

SELECT security_id, invalid_reason, count(*) AS row_count
FROM price_history
WHERE NOT model_usable
GROUP BY security_id, invalid_reason
ORDER BY security_id, invalid_reason;
