# dqc

Comprehensive data quality checker for PostgreSQL databases.

## Quick Usage

```bash
# Run all data quality checks on a single table (DEFAULT: NaN, references, encoding, primary keys)
python main.py check-table "postgresql://user:pass@host:port/dbname" "table_name"

# Run all data quality checks on all tables (DEFAULT: NaN, references, encoding, primary keys)
python main.py check-database "postgresql://user:pass@host:port/dbname"

# Check only NaN values across all tables
python main.py check-nan "postgresql://user:pass@host:port/dbname"

# Check only foreign key references across all tables
python main.py check-references "postgresql://user:pass@host:port/dbname"

# Check only character encoding issues across all tables
python main.py check-encoding "postgresql://user:pass@host:port/dbname"

# Check for large tables (over 500K rows by default)
python main.py check-large-tables "postgresql://user:pass@host:port/dbname"

# Check if a column exists across all tables
python main.py check-column "postgresql://user:pass@host:port/dbname" "column_name"

# Check which tables have or lack primary keys
python main.py check-pk "postgresql://user:pass@host:port/dbname"

# Describe a table's structure and sample data
python main.py describe-table "postgresql://user:pass@host:port/dbname" "table_name"
```

## Commands

### `check-table`

Run comprehensive data quality checks on a single table.

**Usage:** `python main.py check-table DATABASE_URL TABLE_NAME [FLAGS]`

**Behavior:**
- **By default, runs ALL checks**: NaN values, orphaned references, encoding issues, and primary key validation
- Shows organized summary of all issues found in the table
- Sample records include primary key and context columns

**Column Type Flags:**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns
- `--text-types` - Only check text/string columns

**Check Control Flags:**
- `--skip-nan-check` - Skip NaN value detection
- `--skip-references-check` - Skip foreign key validation
- `--skip-encoding-check` - Skip character encoding checks
- `--skip-pk-check` - Skip primary key validation

**Examples:**
```bash
# Run all data quality checks (default)
python main.py check-table "db_url" "users"

# Skip expensive checks
python main.py check-table "db_url" "loans" --skip-references-check --skip-encoding-check

# Focus only on NaN issues in numeric columns
python main.py check-table "db_url" "payments" --skip-references-check --skip-encoding-check --numeric-types
```

### `check-database`

Run comprehensive data quality checks across all tables in a database.

**Usage:** `python main.py check-database DATABASE_URL [FLAGS]`

**Behavior:**
- **By default, runs ALL checks**: NaN values, orphaned references, encoding issues, and primary key validation
- Shows progress as tables are processed
- Displays organized summary of all issues found
- Sample records include primary key and context columns

**Column Type Flags:**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns
- `--text-types` - Only check text/string columns

**Table Filtering Flags:**
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Check Control Flags:**
- `--skip-nan-check` - Skip NaN value detection
- `--skip-references-check` - Skip foreign key validation
- `--skip-encoding-check` - Skip character encoding checks
- `--skip-pk-check` - Skip primary key validation

**Examples:**
```bash
# Run all data quality checks (default)
python main.py check-database "db_url"

# Skip expensive checks for large databases
python main.py check-database "db_url" --skip-references-check --skip-encoding-check

# Focus only on NaN issues with table filtering
python main.py check-database "db_url" --skip-references-check --skip-encoding-check --skip-large-tables

# Check only numeric columns across all tables
python main.py check-database "db_url" --numeric-types
```

### `check-nan`

Check for NaN values across all tables in a database.

**Usage:** `python main.py check-nan DATABASE_URL [FLAGS]`

**Behavior:**
- By default, checks all columns in all tables for NaN values
- Shows progress as tables are processed
- Displays summary of NaN issues found across the database
- Sample records include primary key and context columns

**Column Type Flags:**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns
- `--text-types` - Only check text/string columns

**Table Filtering Flags:**
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Examples:**
```bash
# Check all tables for NaN values
python main.py check-nan "db_url"

# Check only numeric columns across all tables
python main.py check-nan "db_url" --numeric-types

# Skip large tables and specific problematic tables
python main.py check-nan "db_url" --skip-large-tables --skip-table logs --skip-table raw_data
```

### `check-references`

Check for orphaned foreign key references across all tables in a database.

**Usage:** `python main.py check-references DATABASE_URL [FLAGS]`

**Behavior:**
- Finds records with foreign keys pointing to non-existent parent records
- Handles composite foreign keys properly
- Shows sample orphaned records with context
- Displays foreign key constraint details

**Flags:**
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Examples:**
```bash
# Check all tables for orphaned references
python main.py check-references "db_url"

# Skip large tables to speed up analysis
python main.py check-references "db_url" --skip-large-tables
```

### `check-encoding`

Check for character encoding issues across all tables in a database.

**Usage:** `python main.py check-encoding DATABASE_URL [FLAGS]`

**Behavior:**
- By default, checks all text/string columns
- Detects null bytes, control characters, and invalid UTF-8 sequences
- Shows sample problematic records with context
- Focuses on columns that commonly cause JDBC/application failures

**Column Type Flags:**
- `--numeric-types` - Only check numeric columns
- `--date-types` - Only check date/datetime columns
- `--text-types` - Only check text/string columns

**Table Filtering Flags:**
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Examples:**
```bash
# Check all text columns for encoding issues
python main.py check-encoding "db_url"

# Check only specific column types
python main.py check-encoding "db_url" --text-types

# Skip problematic tables
python main.py check-encoding "db_url" --skip-table logs --skip-table raw_data
```

### `check-large-tables`

Check for tables that exceed a row count threshold.

**Usage:** `python main.py check-large-tables DATABASE_URL [FLAGS]`

**Behavior:**
- Scans all tables and counts rows in each
- By default, shows tables with more than 500,000 rows
- Displays row counts in human-readable format with commas
- Shows percentage over threshold for large tables
- Provides summary statistics

**Flags:**
- `--threshold INTEGER` - Override default threshold (default: 500,000)
- `--show-all` - Show all tables with row counts, not just large ones
- `--top N` - Show top N largest tables regardless of threshold
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Examples:**
```bash
# Check for tables over 500K rows (default)
python main.py check-large-tables "db_url"

# Check for tables over 1M rows
python main.py check-large-tables "db_url" --threshold 1000000

# Show all table sizes sorted by row count
python main.py check-large-tables "db_url" --show-all

# Show top 10 largest tables regardless of threshold
python main.py check-large-tables "db_url" --top 10

# Skip system/log tables
python main.py check-large-tables "db_url" --skip-table django_session --skip-table access_logs
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

### `check-pk`

Check for tables that have or lack primary keys.

**Usage:** `python main.py check-pk DATABASE_URL [FLAGS]`

**Behavior:**
- Shows real-time progress as it processes each table
- Lists tables WITH primary keys (showing constraint names and column names)
- Lists tables WITHOUT primary keys (showing row counts for priority assessment)
- Provides coverage summary and identifies tables at risk
- Supports table filtering to focus on specific subsets

**Table Filtering Flags:**
- `--skip-large-tables` - Skip tables with more than 500K rows
- `--skip-table TABLE_NAME` - Skip specific tables (can be used multiple times)

**Examples:**
```bash
# Check all tables for primary keys
python main.py check-pk "db_url"

# Skip large tables to speed up analysis
python main.py check-pk "db_url" --skip-large-tables

# Skip system/metadata tables
python main.py check-pk "db_url" --skip-table django_migrations --skip-table auth_permissions
```

### `describe-table`

Describe a table's structure, constraints, and basic statistics.

**Usage:** `python main.py describe-table DATABASE_URL TABLE_NAME`

**Behavior:**
- Shows comprehensive table information including row count
- Lists all columns with data types, nullable status, and defaults
- Displays primary key, foreign keys, and indexes
- Shows check constraints (if supported)
- Provides sample data (first 3 rows) in pandas-like format
- Validates table existence and suggests alternatives if not found

**Examples:**
```bash
# Describe a user table
python main.py describe-table "db_url" "users"

# Describe a transaction table
python main.py describe-table "db_url" "payments"
```