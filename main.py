#!/usr/bin/env python3

import click
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import Integer, Float, Numeric, BigInteger, SmallInteger, Date, DateTime, Time, String, Text, VARCHAR, CHAR


@click.group()
def dqc():
    """Data Quality Checker for PostgreSQL databases."""
    pass


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
            table_issues.append({
                'column': col,
                'nan_count': nan_count,
                'total_rows': total_rows,
                'percentage': percentage
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


if __name__ == '__main__':
    dqc()