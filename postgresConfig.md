Open SQL Shell (psql):

CREATE TABLE IF NOT EXISTS json_example (id TEXT PRIMARY KEY, json_col JSON);

SELECT * FROM public.json_example ORDER BY id ASC LIMIT 100