#!/usr/bin/env python3

import click
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import Integer, Float, Numeric, BigInteger, SmallInteger, Date, DateTime, Time, String, Text, VARCHAR, CHAR


@click.group()
def dqc():
    """Data Quality Checker for PostgreSQL databases."""
    pass


def _display_dataframe(columns, rows, indent="    "):
    """Display data in pandas-like tabular format."""
    if not rows or not columns:
        return
    
    # Calculate column widths
    col_widths = []
    for i, col in enumerate(columns):
        max_width = len(str(col))
        for row in rows:
            if i < len(row):
                max_width = max(max_width, len(str(row[i])))
        col_widths.append(min(max_width, 20))  # Cap at 20 chars
    
    # Print header
    header = " | ".join(f"{col[:20]:<{col_widths[i]}}" for i, col in enumerate(columns))
    click.echo(f"{indent}{header}")
    
    # Print separator
    separator = "-+-".join("-" * width for width in col_widths)
    click.echo(f"{indent}{separator}")
    
    # Print rows
    for row in rows:
        row_str = " | ".join(
            f"{str(row[i])[:20]:<{col_widths[i]}}" if i < len(row) else f"{'':< {col_widths[i]}}"
            for i in range(len(columns))
        )
        click.echo(f"{indent}{row_str}")


def _get_primary_key_column(inspector, table_name):
    """Get the primary key column name for a table."""
    try:
        pk_constraint = inspector.get_pk_constraint(table_name)
        if pk_constraint and pk_constraint.get('constrained_columns'):
            return pk_constraint['constrained_columns'][0]  # Return first PK column
        return 'id'  # Default fallback
    except:
        return 'id'  # Default fallback


def _get_sample_faulty_records(engine, inspector, table_name, column_name, pk_column):
    """Get 5 sample records where the specified column contains 'NaN'."""
    try:
        quoted_table = f'"{table_name}"'
        quoted_column = f'"{column_name}"'
        quoted_pk = f'"{pk_column}"'
        
        # Get columns for context
        columns = inspector.get_columns(table_name)
        context_columns = []
        
        # Find best timestamp columns (prefer creation, then update)
        creation_keywords = ['creat', 'add', 'insert', 'start']
        update_keywords = ['updat', 'modif', 'chang', 'edit']
        timestamp_keywords = ['time', 'date', 'stamp']
        
        creation_col = None
        update_col = None
        
        for col in columns:
            col_name_lower = col["name"].lower()
            
            # Check if it's a timestamp-like column
            if any(ts_word in col_name_lower for ts_word in timestamp_keywords):
                # Prefer creation columns
                if not creation_col and any(create_word in col_name_lower for create_word in creation_keywords):
                    creation_col = f'"{col["name"]}"'
                # Then update columns
                elif not update_col and any(update_word in col_name_lower for update_word in update_keywords):
                    update_col = f'"{col["name"]}"'
        
        # Add first few regular columns
        for col in columns:
            if len(context_columns) >= 6:  # Leave room for PK, faulty column, and timestamps
                break
            context_columns.append(f'"{col["name"]}"')
        
        # Make sure PK and faulty column are included
        if quoted_pk not in context_columns:
            context_columns.insert(0, quoted_pk)
        if quoted_column not in context_columns:
            context_columns.append(quoted_column)
            
        # Add timestamp columns (creation first, then update)
        if creation_col and creation_col not in context_columns:
            context_columns.append(creation_col)
        if update_col and update_col not in context_columns and len(context_columns) < 9:
            context_columns.append(update_col)
                
        # Limit total columns to avoid overly wide tables
        context_columns = context_columns[:9]
        
        query = f"""
        SELECT {', '.join(context_columns)}
        FROM {quoted_table} 
        WHERE {quoted_column}::text = 'NaN' 
        LIMIT 5
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            column_names = [col.replace('"', '') for col in context_columns]
            rows = result.fetchall()
            return column_names, rows
    except Exception:
        return [], []


def _check_table_nan_values(engine, inspector, table_name, numeric_types, date_types, text_types):
    """Helper function to check NaN values in a single table."""
    columns = inspector.get_columns(table_name)
    target_columns = []
    
    # Check if any type flags are specified
    type_flags_used = numeric_types or date_types or text_types
    
    if type_flags_used:
        # Filter columns based on specified flags
        for column in columns:
            column_type = column['type']
            include_column = False
            
            if numeric_types and isinstance(column_type, (Integer, Float, Numeric, BigInteger, SmallInteger)):
                include_column = True
            elif date_types and isinstance(column_type, (Date, DateTime, Time)):
                include_column = True
            elif text_types and isinstance(column_type, (String, Text, VARCHAR, CHAR)):
                include_column = True
            
            if include_column:
                target_columns.append(column['name'])
        
        if not target_columns:
            return None, None, type_flags_used
    else:
        # Check all columns by default
        target_columns = [column['name'] for column in columns]
    
    # Build dynamic SQL query to check for 'NaN' string values
    select_clauses = []
    for col in target_columns:
        quoted_col = f'"{col}"'
        safe_alias = col.replace('"', '')  # Remove quotes for alias
        select_clauses.append(f"SUM(CASE WHEN {quoted_col}::text = 'NaN' THEN 1 ELSE 0 END) as {safe_alias}_nan")
    
    quoted_table = f'"{table_name}"'
    query = f"SELECT {', '.join(select_clauses)} FROM {quoted_table}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        
        # Get total row count
        total_query = f"SELECT COUNT(*) FROM {table_name}"
        total_rows = conn.execute(text(total_query)).scalar()
    
    # Get primary key column for sample records
    pk_column = _get_primary_key_column(inspector, table_name)
    
    # Collect results
    table_issues = []
    result_dict = result._asdict()
    
    for col in target_columns:
        safe_alias = col.replace('"', '')  # Remove quotes for alias lookup
        nan_count = result_dict.get(f"{safe_alias}_nan", 0)
        
        # Handle None values
        if nan_count is None:
            nan_count = 0
            
        if nan_count > 0:
            percentage = (nan_count / total_rows) * 100 if total_rows > 0 else 0
            
            # Get sample faulty records
            sample_columns, sample_records = _get_sample_faulty_records(engine, inspector, table_name, col, pk_column)
            
            table_issues.append({
                'column': col,
                'nan_count': nan_count,
                'total_rows': total_rows,
                'percentage': percentage,
                'pk_column': pk_column,
                'sample_columns': sample_columns,
                'sample_records': sample_records
            })
    
    return table_issues, total_rows, type_flags_used


@dqc.command()
@click.argument('database_url')
@click.argument('table_name')
@click.option('--numeric-types', is_flag=True, help='Only check numeric columns')
@click.option('--date-types', is_flag=True, help='Only check date/datetime columns')
@click.option('--text-types', is_flag=True, help='Only check text/string columns')
def check_table(database_url, table_name, numeric_types, date_types, text_types):
    """Check for NaN values in columns of a PostgreSQL table."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        if not inspector.has_table(table_name):
            click.echo(f"Error: Table '{table_name}' not found in database.")
            return
        
        table_issues, total_rows, type_flags_used = _check_table_nan_values(
            engine, inspector, table_name, numeric_types, date_types, text_types
        )
        
        if table_issues is None:
            flag_descriptions = []
            if numeric_types:
                flag_descriptions.append("numeric")
            if date_types:
                flag_descriptions.append("date/datetime")
            if text_types:
                flag_descriptions.append("text/string")
            
            click.echo(f"No {' or '.join(flag_descriptions)} columns found in table '{table_name}'.")
            return
        
        click.echo(f"\nNaN values in table '{table_name}' (Total rows: {total_rows}):")
        click.echo("-" * 50)
        
        if table_issues:
            for issue in table_issues:
                click.echo(f"{issue['column']}: {issue['nan_count']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                
                # Show sample records in pandas-like format
                if issue['sample_records']:
                    click.echo(f"  Sample faulty records:")
                    _display_dataframe(issue['sample_columns'], issue['sample_records'])
                    click.echo()
        else:
            if type_flags_used:
                flag_descriptions = []
                if numeric_types:
                    flag_descriptions.append("numeric")
                if date_types:
                    flag_descriptions.append("date/datetime")
                if text_types:
                    flag_descriptions.append("text/string")
                column_description = f"{' and '.join(flag_descriptions)} columns"
            else:
                column_description = "columns"
            
            click.echo(f"No NaN values found in {column_description}.")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}")


@dqc.command()
@click.argument('database_url')
@click.option('--numeric-types', is_flag=True, help='Only check numeric columns')
@click.option('--date-types', is_flag=True, help='Only check date/datetime columns')
@click.option('--text-types', is_flag=True, help='Only check text/string columns')
@click.option('--skip-large-tables', is_flag=True, help='Skip tables with more than 500K rows')
@click.option('--skip-table', multiple=True, help='Skip specific tables by name (can be used multiple times)')
def check_database(database_url, numeric_types, date_types, text_types, skip_large_tables, skip_table):
    """Check for NaN values in all tables of a PostgreSQL database."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        table_names = inspector.get_table_names()
        
        if not table_names:
            click.echo("No tables found in database.")
            return
        
        click.echo(f"Checking {len(table_names)} tables for NaN values...")
        
        database_issues = {}
        
        for i, table_name in enumerate(table_names, 1):
            click.echo(f"[{i}/{len(table_names)}] Checking table: {table_name}")
            
            try:
                # Check if table should be skipped by name
                if table_name in skip_table:
                    click.echo(f"  -> Skipped (explicitly excluded)")
                    continue
                
                # Check table size if skip-large-tables flag is used
                if skip_large_tables:
                    with engine.connect() as conn:
                        count_query = f'SELECT COUNT(*) FROM "{table_name}"'
                        row_count = conn.execute(text(count_query)).scalar()
                        
                        if row_count > 500000:
                            click.echo(f"  -> Skipped (large table: {row_count:,} rows)")
                            continue
                
                table_issues, total_rows, type_flags_used = _check_table_nan_values(
                    engine, inspector, table_name, numeric_types, date_types, text_types
                )
                
                if table_issues is not None and table_issues:
                    database_issues[table_name] = {
                        'issues': table_issues,
                        'total_rows': total_rows
                    }
                    click.echo(f"  -> Found {len(table_issues)} columns with NaN values")
                else:
                    click.echo(f"  -> No NaN values found")
                    
            except Exception as e:
                click.echo(f"  -> Error: {str(e)}")
                continue
        
        # Display results
        if database_issues:
            click.echo(f"\nNaN values found in {len(database_issues)} tables:")
            click.echo("=" * 60)
            
            for table_name, data in database_issues.items():
                click.echo(f"\nTable: {table_name} (Total rows: {data['total_rows']})")
                click.echo("-" * 50)
                
                for issue in data['issues']:
                    click.echo(f"  {issue['column']}: {issue['nan_count']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                    
                    # Show sample records in pandas-like format
                    if issue['sample_records']:
                        click.echo(f"    Sample faulty records:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                        click.echo()
        else:
            if type_flags_used:
                flag_descriptions = []
                if numeric_types:
                    flag_descriptions.append("numeric")
                if date_types:
                    flag_descriptions.append("date/datetime")
                if text_types:
                    flag_descriptions.append("text/string")
                column_description = f"{' and '.join(flag_descriptions)} columns"
            else:
                column_description = "columns"
            
            click.echo(f"\nNo NaN values found in {column_description} across all tables.")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}")


@dqc.command()
@click.argument('database_url')
@click.argument('column_name')
def check_column(database_url, column_name):
    """Check if a column exists across all tables in a PostgreSQL database."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        table_names = inspector.get_table_names()
        
        if not table_names:
            click.echo("No tables found in database.")
            return
        
        click.echo(f"Checking column '{column_name}' across {len(table_names)} tables...")
        click.echo("=" * 60)
        
        tables_with_column = []
        tables_without_column = []
        
        for table_name in table_names:
            try:
                columns = inspector.get_columns(table_name)
                column_info = None
                
                # Look for the column (case-insensitive)
                for col in columns:
                    if col['name'].lower() == column_name.lower():
                        column_info = {
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col.get('nullable', True),
                            'default': col.get('default', None)
                        }
                        break
                
                if column_info:
                    tables_with_column.append((table_name, column_info))
                else:
                    tables_without_column.append(table_name)
                    
            except Exception as e:
                click.echo(f"Error checking table '{table_name}': {str(e)}")
                continue
        
        # Display results
        if tables_with_column:
            click.echo(f"\nTables WITH column '{column_name}' ({len(tables_with_column)}):")
            click.echo("-" * 50)
            
            for table_name, col_info in tables_with_column:
                nullable_str = "NULL" if col_info['nullable'] else "NOT NULL"
                default_str = f" DEFAULT {col_info['default']}" if col_info['default'] else ""
                click.echo(f"  {table_name}: {col_info['name']} ({col_info['type']}) {nullable_str}{default_str}")
        
        if tables_without_column:
            click.echo(f"\nTables WITHOUT column '{column_name}' ({len(tables_without_column)}):")
            click.echo("-" * 50)
            
            for table_name in tables_without_column:
                click.echo(f"  {table_name}")
        
        # Summary
        total_tables = len(tables_with_column) + len(tables_without_column)
        coverage_percent = (len(tables_with_column) / total_tables * 100) if total_tables > 0 else 0
        
        click.echo(f"\nSummary:")
        click.echo(f"  Column coverage: {len(tables_with_column)}/{total_tables} tables ({coverage_percent:.1f}%)")
        
        if not tables_with_column:
            click.echo(f"  Column '{column_name}' does not exist in any table.")
        elif not tables_without_column:
            click.echo(f"  Column '{column_name}' exists in all tables.")
        else:
            click.echo(f"  Column '{column_name}' is missing from {len(tables_without_column)} tables.")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}")


if __name__ == '__main__':
    dqc()