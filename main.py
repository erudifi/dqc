#!/usr/bin/env python3

import click
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.types import Integer, Float, Numeric, BigInteger, SmallInteger, Date, DateTime, Time, String, Text, VARCHAR, CHAR


@click.group()
def dqc():
    """Data Quality Checker for PostgreSQL databases."""
    pass


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
                flag_descriptions = []
                if numeric_types:
                    flag_descriptions.append("numeric")
                if date_types:
                    flag_descriptions.append("date/datetime")
                if text_types:
                    flag_descriptions.append("text/string")
                
                click.echo(f"No {' or '.join(flag_descriptions)} columns found in table '{table_name}'.")
                return
            
            flag_descriptions = []
            if numeric_types:
                flag_descriptions.append("numeric")
            if date_types:
                flag_descriptions.append("date/datetime")
            if text_types:
                flag_descriptions.append("text/string")
            
            click.echo(f"Checking {' and '.join(flag_descriptions)} columns: {', '.join(target_columns)}")
        else:
            # Check all columns by default
            target_columns = [column['name'] for column in columns]
            click.echo(f"Checking all columns: {', '.join(target_columns)}")
        
        # Build dynamic SQL query to check for 'NaN' string values
        select_clauses = []
        for col in target_columns:
            select_clauses.append(f"SUM(CASE WHEN {col}::text = 'NaN' THEN 1 ELSE 0 END) as {col}_nan")
        
        query = f"SELECT {', '.join(select_clauses)} FROM {table_name}"
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            
            # Get total row count
            total_query = f"SELECT COUNT(*) FROM {table_name}"
            total_rows = conn.execute(text(total_query)).scalar()
        
        issues_found = False
        click.echo(f"\nNaN values in table '{table_name}' (Total rows: {total_rows}):")
        click.echo("-" * 50)
        
        for col in target_columns:
            nan_count = result._asdict()[f"{col}_nan"]
            
            if nan_count > 0:
                issues_found = True
                percentage = (nan_count / total_rows) * 100 if total_rows > 0 else 0
                click.echo(f"{col}: {nan_count}/{total_rows} ({percentage:.2f}%)")
        
        if not issues_found:
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


if __name__ == '__main__':
    dqc()