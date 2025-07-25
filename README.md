# dqc

Simple data quality checker for PostgreSQL databases.

## Quick Usage

```bash
# Check a single table for NaN values
python main.py check-table "postgresql://user:pass@host:port/dbname" "table_name"

# Check all tables for NaN values
python main.py check-database "postgresql://user:pass@host:port/dbname"

# Check if a column exists across all tables
python main.py check-column "postgresql://user:pass@host:port/dbname" "column_name"
```

## Commands

### `check-table`

Check for NaN values in columns of a single table.

**Usage:** `python main.py check-table DATABASE_URL TABLE_NAME [FLAGS]`

**Behavior:**
- By default, checks all columns in the table
- Shows NaN count, percentage, and sample faulty records
- Sample records include primary key and context columns

**Flags:**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns
- `--text-types` - Only check text/string columns

**Examples:**
```bash
# Check all columns
python main.py check-table "db_url" "users"

# Check only numeric columns
python main.py check-table "db_url" "loans" --numeric-types

# Check multiple column types
python main.py check-table "db_url" "payments" --numeric-types --date-types
```

### `check-database`

Check for NaN values across all tables in a database.

**Usage:** `python main.py check-database DATABASE_URL [FLAGS]`

**Behavior:**
- By default, checks all columns in all tables
- Shows progress as tables are processed
- Displays summary of issues found across the database
- Sample records include primary key and context columns

**Flags:**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns
- `--text-types` - Only check text/string columns
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Examples:**
```bash
# Check all tables and columns
python main.py check-database "db_url"

# Skip large tables and specific tables
python main.py check-database "db_url" --skip-large-tables --skip-table django_session --skip-table auth_log

# Check only numeric columns across all tables
python main.py check-database "db_url" --numeric-types
```

### `check-column`

Check if a specific column exists across all tables and show its type information.

**Usage:** `python main.py check-column DATABASE_URL COLUMN_NAME`

**Behavior:**
- Case-insensitive column name matching
- Shows column type, nullable status, and default values
- Displays coverage summary (how many tables have the column)
- Lists tables with and without the column

**Examples:**
```bash
# Check if 'id' column exists in all tables
python main.py check-column "db_url" "id"

# Check for timestamp columns
python main.py check-column "db_url" "created_at"
```