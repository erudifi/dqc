# dqc

Simple data quality checker for PostgreSQL databases. Finds NaN values in table columns.

## Usage

Check a single table:
```bash
python main.py check-table "postgresql://user:pass@host:port/dbname" "table_name"
```

Check all tables in database:
```bash
python main.py check-database "postgresql://user:pass@host:port/dbname"
```

## Flags

- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns  
- `--text-types` - Only check text/string columns

Flags can be combined: `python main.py check-table "db_url" "table" --numeric-types --text-types`

Default behavior checks all columns.
