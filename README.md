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

**Column type filters (default: check all columns):**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns  
- `--text-types` - Only check text/string columns

**Table filters (check-database only):**
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

## Examples

```bash
# Check all columns in a table (default behavior)
python main.py check-table "db_url" "table"

# Check all columns in all tables (default behavior)
python main.py check-database "db_url"

# Check only specific column types
python main.py check-table "db_url" "table" --numeric-types --text-types

# Check database with filters
python main.py check-database "db_url" --skip-large-tables --skip-table django_session --skip-table auth_log

# Check only numeric columns across all tables
python main.py check-database "db_url" --numeric-types
```
