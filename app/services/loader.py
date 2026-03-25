from typing import Iterable, Mapping, Any, Sequence
import json

from app.utils.db import get_db_connection


STAGING_SCHEMA = "temp_data"


def ensure_table_exists(table_name: str, columns: Sequence[str]) -> None:
	"""Create a simple table if it does not exist yet.

	All columns are created as TEXT for simplicity. If the table already
	exists, any missing columns from `columns` are added as TEXT so that
	new fields in the data (and CSV) are not silently dropped.
	"""
	cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
	qualified_table = f'{STAGING_SCHEMA}."{table_name}"'
	create_sql = f'CREATE TABLE IF NOT EXISTS {qualified_table} (id SERIAL PRIMARY KEY, {cols_sql});'

	with get_db_connection() as conn:
		with conn.cursor() as cur:
			# Ensure the staging schema exists first
			cur.execute(f'CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};')
			# Ensure the table exists
			cur.execute(create_sql)

			# Inspect existing columns and add any that are missing.
			cur.execute(
				"""
				SELECT column_name
				FROM information_schema.columns
				WHERE table_schema = %s AND table_name = %s
				""",
				(STAGING_SCHEMA, table_name),
			)
			existing_cols = {row[0] for row in cur.fetchall()}

			for col in columns:
				if col not in existing_cols:
					alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT;'
					cur.execute(alter_sql)


def insert_rows(table_name: str, rows: Iterable[Mapping[str, Any]]) -> int:
	"""Insert a collection of dict-like rows into the given table.

	- Uses the keys of the first row as the column list
	- Returns the number of rows successfully inserted
	"""
	rows = list(rows)
	if not rows:
		return 0

	# Collect the union of all keys across all rows so that
	# no field present in the data is silently dropped.
	seen = set()
	columns = []
	for row in rows:
		for key in row.keys():
			if key not in seen:
				seen.add(key)
				columns.append(key)
	ensure_table_exists(table_name, columns)

	qualified_table = f'{STAGING_SCHEMA}."{table_name}"'
	cols_sql = ", ".join(f'"{c}"' for c in columns)
	placeholders = ", ".join(["%s"] * len(columns))
	insert_sql = f'INSERT INTO {qualified_table} ({cols_sql}) VALUES ({placeholders})'

	def _normalize_value(value: Any) -> Any:
		"""Convert complex Python values into DB-friendly types.

		- dict / list / tuple / set -> JSON string
		- other non-primitive types -> string fallback
		"""
		if isinstance(value, (dict, list, tuple, set)):
			return json.dumps(value)
		# psycopg2 can handle None, str, int, float, bool directly
		if isinstance(value, (type(None), str, int, float, bool)):
			return value
		return str(value)

	values = [
		tuple(_normalize_value(row.get(col)) for col in columns)
		for row in rows
	]

	with get_db_connection() as conn:
		with conn.cursor() as cur:
			# Treat these tables as staging/temp tables in the temp_data schema:
			# clear existing data on each load so the DB procedure can move
			# data into the final schema without duplicates.
			cur.execute(f'TRUNCATE TABLE {qualified_table};')
			cur.executemany(insert_sql, values)

	return len(values)

