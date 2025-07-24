# dqc

Simple data quality checker for PostgreSQL databases. Finds NaN values in table columns.

## Usage

```bash
python main.py "postgresql://user:pass@host:port/dbname" "table_name"
```

## Flags

- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns  
- `--text-types` - Only check text/string columns

Flags can be combined: `--numeric-types --text-types`

Default behavior checks all columns.
