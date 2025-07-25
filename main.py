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


def _get_foreign_keys(inspector, table_name):
    """Get foreign key constraints for a table."""
    try:
        return inspector.get_foreign_keys(table_name)
    except Exception:
        return []


def _find_orphaned_records(engine, table_name, fk_info):
    """Find records with foreign keys that reference non-existent parent records."""
    try:
        child_table = f'"{table_name}"'
        parent_table = f'"{fk_info["referred_table"]}"'
        
        # Handle composite foreign keys
        child_cols = [f'"{col}"' for col in fk_info['constrained_columns']]
        parent_cols = [f'"{col}"' for col in fk_info['referred_columns']]
        
        # Build JOIN conditions
        join_conditions = []
        for child_col, parent_col in zip(child_cols, parent_cols):
            join_conditions.append(f"{child_table}.{child_col} = {parent_table}.{parent_col}")
        
        # Count orphaned records
        count_query = f"""
        SELECT COUNT(*) 
        FROM {child_table}
        LEFT JOIN {parent_table} ON {' AND '.join(join_conditions)}
        WHERE {parent_cols[0]} IS NULL 
        AND {child_cols[0]} IS NOT NULL
        """
        
        # Get total records in child table
        total_query = f"SELECT COUNT(*) FROM {child_table}"
        
        with engine.connect() as conn:
            orphaned_count = conn.execute(text(count_query)).scalar()
            total_count = conn.execute(text(total_query)).scalar()
            
            return orphaned_count, total_count
            
    except Exception:
        return 0, 0


def _get_sample_orphaned_records(engine, inspector, table_name, fk_info, pk_column):
    """Get sample orphaned records with context columns."""
    try:
        child_table = f'"{table_name}"'
        parent_table = f'"{fk_info["referred_table"]}"'
        quoted_pk = f'"{pk_column}"'
        
        # Handle composite foreign keys  
        child_cols = [f'"{col}"' for col in fk_info['constrained_columns']]
        parent_cols = [f'"{col}"' for col in fk_info['referred_columns']]
        
        # Get context columns (first few columns + PK + FK columns)
        columns = inspector.get_columns(table_name)
        context_columns = []
        
        # Add first few columns
        for col in columns[:4]:
            context_columns.append(f'"{col["name"]}"')
            
        # Ensure PK is included
        if quoted_pk not in context_columns:
            context_columns.insert(0, quoted_pk)
            
        # Ensure FK columns are included
        for child_col in child_cols:
            if child_col not in context_columns:
                context_columns.append(child_col)
        
        # Build JOIN conditions
        join_conditions = []
        for child_col, parent_col in zip(child_cols, parent_cols):
            join_conditions.append(f"{child_table}.{child_col} = {parent_table}.{parent_col}")
        
        # Query for sample orphaned records
        query = f"""
        SELECT {', '.join([f"{child_table}.{col}" for col in context_columns])}
        FROM {child_table}
        LEFT JOIN {parent_table} ON {' AND '.join(join_conditions)}
        WHERE {parent_table}.{parent_cols[0]} IS NULL 
        AND {child_table}.{child_cols[0]} IS NOT NULL
        LIMIT 5
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            column_names = [col.replace('"', '') for col in context_columns]
            rows = result.fetchall()
            return column_names, rows
            
    except Exception:
        return [], []


def _check_table_encoding_issues(engine, inspector, table_name, numeric_types, date_types, text_types):
    """Helper function to check encoding issues in a single table."""
    columns = inspector.get_columns(table_name)
    target_columns = []
    
    # Check if any type flags are specified
    type_flags_used = numeric_types or date_types or text_types
    
    if type_flags_used:
        # Filter columns based on specified flags - focus on text types for encoding
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
        # Check all text/string columns by default for encoding issues
        for column in columns:
            column_type = column['type']
            if isinstance(column_type, (String, Text, VARCHAR, CHAR)):
                target_columns.append(column['name'])
    
    if not target_columns:
        return [], 0, type_flags_used
    
    # Get primary key column for sample records
    pk_column = _get_primary_key_column(inspector, table_name)
    
    # Build dynamic SQL query to check for encoding issues
    select_clauses = []
    for col in target_columns:
        quoted_col = f'"{col}"'
        safe_alias = col.replace('"', '')
        
        # Check for multiple encoding issues:
        # 1. Null bytes (ASCII 0)
        # 2. Other problematic control characters (ASCII 1-31 except tab, newline, carriage return)
        # 3. Invalid UTF-8 sequences (this is harder to detect in pure SQL)
        select_clauses.extend([
            f"SUM(CASE WHEN {quoted_col} ~ '\\x00' THEN 1 ELSE 0 END) as {safe_alias}_null_bytes",
            f"SUM(CASE WHEN {quoted_col} ~ '[\\x01-\\x08\\x0B\\x0C\\x0E-\\x1F]' THEN 1 ELSE 0 END) as {safe_alias}_control_chars"
        ])
    
    quoted_table = f'"{table_name}"'
    query = f"SELECT {', '.join(select_clauses)} FROM {quoted_table}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        
        # Get total row count
        total_query = f"SELECT COUNT(*) FROM {quoted_table}"
        total_rows = conn.execute(text(total_query)).scalar()
    
    # Collect results
    table_issues = []
    result_dict = result._asdict()
    
    for col in target_columns:
        safe_alias = col.replace('"', '')
        null_byte_count = result_dict.get(f"{safe_alias}_null_bytes", 0) or 0
        control_char_count = result_dict.get(f"{safe_alias}_control_chars", 0) or 0
        
        total_encoding_issues = null_byte_count + control_char_count
        
        if total_encoding_issues > 0:
            percentage = (total_encoding_issues / total_rows) * 100 if total_rows > 0 else 0
            
            # Get sample records with encoding issues
            sample_columns, sample_records = _get_sample_encoding_issues(engine, inspector, table_name, col, pk_column)
            
            issue_types = []
            if null_byte_count > 0:
                issue_types.append(f"null bytes: {null_byte_count}")
            if control_char_count > 0:
                issue_types.append(f"control chars: {control_char_count}")
            
            table_issues.append({
                'column': col,
                'issue_types': issue_types,
                'total_issues': total_encoding_issues,
                'total_rows': total_rows,
                'percentage': percentage,
                'pk_column': pk_column,
                'sample_columns': sample_columns,
                'sample_records': sample_records
            })
    
    return table_issues, total_rows, type_flags_used


def _get_sample_encoding_issues(engine, inspector, table_name, column_name, pk_column):
    """Get sample records with encoding issues."""
    try:
        quoted_table = f'"{table_name}"'
        quoted_column = f'"{column_name}"'
        quoted_pk = f'"{pk_column}"'
        
        # Get context columns (first few columns + PK + problematic column)
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
            if len(context_columns) >= 6:
                break
            context_columns.append(f'"{col["name"]}"')
        
        # Make sure PK and problematic column are included
        if quoted_pk not in context_columns:
            context_columns.insert(0, quoted_pk)
        if quoted_column not in context_columns:
            context_columns.append(quoted_column)
            
        # Add timestamp columns (creation first, then update)
        if creation_col and creation_col not in context_columns:
            context_columns.append(creation_col)
        if update_col and update_col not in context_columns and len(context_columns) < 9:
            context_columns.append(update_col)
                
        # Limit total columns
        context_columns = context_columns[:9]
        
        query = f"""
        SELECT {', '.join(context_columns)}
        FROM {quoted_table} 
        WHERE {quoted_column} ~ '[\\x00\\x01-\\x08\\x0B\\x0C\\x0E-\\x1F]'
        LIMIT 5
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            column_names = [col.replace('"', '') for col in context_columns]
            rows = result.fetchall()
            return column_names, rows
    except Exception:
        return [], []


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


def _check_table_references(engine, inspector, table_name):
    """Helper function to check foreign key references for a single table."""
    try:
        foreign_keys = _get_foreign_keys(inspector, table_name)
        if not foreign_keys:
            return []
        
        table_reference_issues = []
        pk_column = _get_primary_key_column(inspector, table_name)
        
        for fk_info in foreign_keys:
            orphaned_count, total_count = _find_orphaned_records(engine, table_name, fk_info)
            
            if orphaned_count > 0:
                percentage = (orphaned_count / total_count) * 100 if total_count > 0 else 0
                sample_columns, sample_records = _get_sample_orphaned_records(
                    engine, inspector, table_name, fk_info, pk_column
                )
                
                fk_name = fk_info.get('name', 'unnamed_fk')
                parent_table = fk_info['referred_table']
                child_columns = ', '.join(fk_info['constrained_columns'])
                parent_columns = ', '.join(fk_info['referred_columns'])
                
                table_reference_issues.append({
                    'fk_name': fk_name,
                    'parent_table': parent_table,
                    'child_columns': child_columns,
                    'parent_columns': parent_columns,
                    'orphaned_count': orphaned_count,
                    'total_count': total_count,
                    'percentage': percentage,
                    'sample_columns': sample_columns,
                    'sample_records': sample_records
                })
        
        return table_reference_issues
        
    except Exception:
        return []


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
@click.option('--skip-nan-check', is_flag=True, help='Skip NaN value detection')
@click.option('--skip-references-check', is_flag=True, help='Skip foreign key validation')
@click.option('--skip-encoding-check', is_flag=True, help='Skip character encoding checks')
def check_table(database_url, table_name, numeric_types, date_types, text_types, skip_nan_check, skip_references_check, skip_encoding_check):
    """Run comprehensive data quality checks on a single PostgreSQL table."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        if not inspector.has_table(table_name):
            click.echo(f"Error: Table '{table_name}' not found in database.")
            return
        
        # Determine which checks to run
        checks_to_run = []
        if not skip_nan_check:
            checks_to_run.append("NaN values")
        if not skip_references_check:
            checks_to_run.append("orphaned references")
        if not skip_encoding_check:
            checks_to_run.append("encoding issues")
        
        if not checks_to_run:
            click.echo("All checks have been skipped. Nothing to do.")
            return
        
        checks_description = ", ".join(checks_to_run)
        click.echo(f"Running comprehensive data quality checks ({checks_description}) on table '{table_name}'...")
        
        # Get total row count
        with engine.connect() as conn:
            count_query = f'SELECT COUNT(*) FROM "{table_name}"'
            total_rows = conn.execute(text(count_query)).scalar()
        
        # Results for each check type
        nan_issues = []
        reference_issues = []
        encoding_issues = []
        
        # Run NaN check
        if not skip_nan_check:
            table_issues, _, _ = _check_table_nan_values(
                engine, inspector, table_name, numeric_types, date_types, text_types
            )
            if table_issues:
                nan_issues = table_issues
        
        # Run references check
        if not skip_references_check:
            reference_issues = _check_table_references(engine, inspector, table_name)
        
        # Run encoding check
        if not skip_encoding_check:
            table_issues, _, _ = _check_table_encoding_issues(
                engine, inspector, table_name, numeric_types, date_types, text_types
            )
            if table_issues:
                encoding_issues = table_issues
        
        # Display results
        total_issues_found = bool(nan_issues or reference_issues or encoding_issues)
        
        if total_issues_found:
            click.echo(f"\n{'='*60}")
            click.echo(f"DATA QUALITY ISSUES - Table: {table_name} (Total rows: {total_rows})")
            click.echo(f"{'='*60}")
            
            # Display NaN issues
            if nan_issues and not skip_nan_check:
                click.echo(f"\n🔍 NaN VALUES:")
                click.echo("-" * 30)
                
                for issue in nan_issues:
                    click.echo(f"  {issue['column']}: {issue['nan_count']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                    
                    if issue['sample_records']:
                        click.echo(f"    Sample faulty records:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                        click.echo()
            
            # Display reference issues
            if reference_issues and not skip_references_check:
                click.echo(f"\n🔗 ORPHANED REFERENCES:")
                click.echo("-" * 30)
                
                for issue in reference_issues:
                    click.echo(f"  FK: {issue['child_columns']} -> {issue['parent_table']}.{issue['parent_columns']}")
                    click.echo(f"  Orphaned records: {issue['orphaned_count']}/{issue['total_count']} ({issue['percentage']:.2f}%)")
                    
                    if issue['sample_records']:
                        click.echo(f"    Sample orphaned records:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                        click.echo()
            
            # Display encoding issues  
            if encoding_issues and not skip_encoding_check:
                click.echo(f"\n🔤 ENCODING ISSUES:")
                click.echo("-" * 30)
                
                for issue in encoding_issues:
                    issue_description = ', '.join(issue['issue_types'])
                    click.echo(f"  {issue['column']}: {issue_description}")
                    click.echo(f"  Total issues: {issue['total_issues']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                    
                    if issue['sample_records']:
                        click.echo(f"    Sample records with encoding issues:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                        click.echo()
            
        else:
            click.echo(f"\n✅ No data quality issues found in table '{table_name}'!")
            click.echo(f"   Checks performed: {checks_description}")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}")


@dqc.command()
@click.argument('database_url')
@click.option('--numeric-types', is_flag=True, help='Only check numeric columns')
@click.option('--date-types', is_flag=True, help='Only check date/datetime columns')
@click.option('--text-types', is_flag=True, help='Only check text/string columns')
@click.option('--skip-large-tables', is_flag=True, help='Skip tables with more than 500K rows')
@click.option('--skip-table', multiple=True, help='Skip specific tables by name (can be used multiple times)')
@click.option('--skip-nan-check', is_flag=True, help='Skip NaN value detection')
@click.option('--skip-references-check', is_flag=True, help='Skip foreign key validation')
@click.option('--skip-encoding-check', is_flag=True, help='Skip character encoding checks')
def check_database(database_url, numeric_types, date_types, text_types, skip_large_tables, skip_table, skip_nan_check, skip_references_check, skip_encoding_check):
    """Run comprehensive data quality checks on all tables of a PostgreSQL database."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        table_names = inspector.get_table_names()
        
        if not table_names:
            click.echo("No tables found in database.")
            return
        
        # Determine which checks to run
        checks_to_run = []
        if not skip_nan_check:
            checks_to_run.append("NaN values")
        if not skip_references_check:
            checks_to_run.append("orphaned references")
        if not skip_encoding_check:
            checks_to_run.append("encoding issues")
        
        if not checks_to_run:
            click.echo("All checks have been skipped. Nothing to do.")
            return
        
        checks_description = ", ".join(checks_to_run)
        click.echo(f"Running comprehensive data quality checks ({checks_description}) on {len(table_names)} tables...")
        
        # Results for each check type
        nan_issues = {}
        reference_issues = {}
        encoding_issues = {}
        
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
                
                # Run NaN check
                if not skip_nan_check:
                    table_issues, total_rows, type_flags_used = _check_table_nan_values(
                        engine, inspector, table_name, numeric_types, date_types, text_types
                    )
                    
                    if table_issues is not None and table_issues:
                        nan_issues[table_name] = {
                            'issues': table_issues,
                            'total_rows': total_rows
                        }
                
                # Run references check
                if not skip_references_check:
                    table_reference_issues = _check_table_references(engine, inspector, table_name)
                    if table_reference_issues:
                        # Get total row count for display
                        with engine.connect() as conn:
                            count_query = f'SELECT COUNT(*) FROM "{table_name}"'
                            total_count = conn.execute(text(count_query)).scalar()
                        
                        reference_issues[table_name] = {
                            'issues': table_reference_issues,
                            'total_rows': total_count
                        }
                
                # Run encoding check
                if not skip_encoding_check:
                    table_issues, total_rows, type_flags_used = _check_table_encoding_issues(
                        engine, inspector, table_name, numeric_types, date_types, text_types
                    )
                    
                    if table_issues is not None and table_issues:
                        encoding_issues[table_name] = {
                            'issues': table_issues,
                            'total_rows': total_rows
                        }
                
                # Summary for this table
                issues_found = []
                if table_name in nan_issues:
                    issues_found.append(f"NaN: {len(nan_issues[table_name]['issues'])} columns")
                if table_name in reference_issues:
                    issues_found.append(f"References: {len(reference_issues[table_name]['issues'])} violations")
                if table_name in encoding_issues:
                    issues_found.append(f"Encoding: {len(encoding_issues[table_name]['issues'])} columns")
                
                if issues_found:
                    click.echo(f"  -> Issues found: {', '.join(issues_found)}")
                else:
                    click.echo(f"  -> No issues found")
                    
            except Exception as e:
                click.echo(f"  -> Error: {str(e)}")
                continue
        
        # Display results
        total_issues_found = bool(nan_issues or reference_issues or encoding_issues)
        
        if total_issues_found:
            click.echo(f"\n{'='*70}")
            click.echo("DATA QUALITY ISSUES SUMMARY")
            click.echo(f"{'='*70}")
            
            # Display NaN issues
            if nan_issues and not skip_nan_check:
                click.echo(f"\n🔍 NaN VALUES - Found in {len(nan_issues)} tables:")
                click.echo("-" * 50)
                
                for table_name, data in nan_issues.items():
                    click.echo(f"\nTable: {table_name} (Total rows: {data['total_rows']})")
                    
                    for issue in data['issues']:
                        click.echo(f"  {issue['column']}: {issue['nan_count']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                        
                        if issue['sample_records']:
                            click.echo(f"    Sample faulty records:")
                            _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                            click.echo()
            
            # Display reference issues
            if reference_issues and not skip_references_check:
                click.echo(f"\n🔗 ORPHANED REFERENCES - Found in {len(reference_issues)} tables:")
                click.echo("-" * 50)
                
                for table_name, data in reference_issues.items():
                    click.echo(f"\nTable: {table_name}")
                    
                    for issue in data['issues']:
                        click.echo(f"  FK: {issue['child_columns']} -> {issue['parent_table']}.{issue['parent_columns']}")
                        click.echo(f"  Orphaned records: {issue['orphaned_count']}/{issue['total_count']} ({issue['percentage']:.2f}%)")
                        
                        if issue['sample_records']:
                            click.echo(f"    Sample orphaned records:")
                            _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                            click.echo()
            
            # Display encoding issues
            if encoding_issues and not skip_encoding_check:
                click.echo(f"\n🔤 ENCODING ISSUES - Found in {len(encoding_issues)} tables:")
                click.echo("-" * 50)
                
                for table_name, data in encoding_issues.items():
                    click.echo(f"\nTable: {table_name} (Total rows: {data['total_rows']})")
                    
                    for issue in data['issues']:
                        issue_description = ', '.join(issue['issue_types'])
                        click.echo(f"  {issue['column']}: {issue_description}")
                        click.echo(f"  Total issues: {issue['total_issues']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                        
                        if issue['sample_records']:
                            click.echo(f"    Sample records with encoding issues:")
                            _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="      ")
                            click.echo()
            
        else:
            click.echo(f"\n✅ No data quality issues found across all tables!")
            click.echo(f"   Checks performed: {checks_description}")
            
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


@dqc.command()
@click.argument('database_url')
@click.option('--numeric-types', is_flag=True, help='Only check numeric columns')
@click.option('--date-types', is_flag=True, help='Only check date/datetime columns')
@click.option('--text-types', is_flag=True, help='Only check text/string columns')
@click.option('--skip-large-tables', is_flag=True, help='Skip tables with more than 500K rows')
@click.option('--skip-table', multiple=True, help='Skip specific tables by name (can be used multiple times)')
def check_nan(database_url, numeric_types, date_types, text_types, skip_large_tables, skip_table):
    """Check for NaN values in all tables of a PostgreSQL database."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        table_names = inspector.get_table_names()
        
        if not table_names:
            click.echo("No tables found in database.")
            return
        
        click.echo(f"Checking NaN values across {len(table_names)} tables...")
        
        nan_issues = {}
        
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
                
                if table_issues is None:
                    flag_descriptions = []
                    if numeric_types:
                        flag_descriptions.append("numeric")
                    if date_types:
                        flag_descriptions.append("date/datetime")
                    if text_types:
                        flag_descriptions.append("text/string")
                    
                    click.echo(f"  -> No {' or '.join(flag_descriptions)} columns found")
                    continue
                
                if table_issues:
                    nan_issues[table_name] = {
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
        if nan_issues:
            click.echo(f"\nNaN values found in {len(nan_issues)} tables:")
            click.echo("=" * 60)
            
            for table_name, data in nan_issues.items():
                click.echo(f"\nTable: {table_name} (Total rows: {data['total_rows']})")
                click.echo("-" * 50)
                
                for issue in data['issues']:
                    click.echo(f"  {issue['column']}: {issue['nan_count']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                    
                    # Show sample records
                    if issue['sample_records']:
                        click.echo(f"  Sample faulty records:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="    ")
                        click.echo()
        else:
            if numeric_types or date_types or text_types:
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
@click.option('--skip-large-tables', is_flag=True, help='Skip tables with more than 500K rows')
@click.option('--skip-table', multiple=True, help='Skip specific tables by name (can be used multiple times)')
def check_references(database_url, skip_large_tables, skip_table):
    """Check for orphaned foreign key references in all tables of a PostgreSQL database."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        table_names = inspector.get_table_names()
        
        if not table_names:
            click.echo("No tables found in database.")
            return
        
        click.echo(f"Checking foreign key references across {len(table_names)} tables...")
        
        reference_issues = {}
        
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
                
                # Check foreign key references for this table
                table_reference_issues = _check_table_references(engine, inspector, table_name)
                
                if not table_reference_issues:
                    foreign_keys = _get_foreign_keys(inspector, table_name)
                    if not foreign_keys:
                        click.echo(f"  -> No foreign keys found")
                    else:
                        click.echo(f"  -> No orphaned references found")
                    continue
                
                # Get total row count for display
                with engine.connect() as conn:
                    count_query = f'SELECT COUNT(*) FROM "{table_name}"'
                    table_total_count = conn.execute(text(count_query)).scalar()
                
                reference_issues[table_name] = {
                    'issues': table_reference_issues,
                    'total_rows': table_total_count
                }
                click.echo(f"  -> Found {len(table_reference_issues)} FK constraint violations")
                    
            except Exception as e:
                click.echo(f"  -> Error: {str(e)}")
                continue
        
        # Display results
        if reference_issues:
            click.echo(f"\nOrphaned foreign key references found in {len(reference_issues)} tables:")
            click.echo("=" * 70)
            
            for table_name, data in reference_issues.items():
                click.echo(f"\nTable: {table_name}")
                click.echo("-" * 50)
                
                for issue in data['issues']:
                    click.echo(f"  FK: {issue['child_columns']} -> {issue['parent_table']}.{issue['parent_columns']}")
                    click.echo(f"  Orphaned records: {issue['orphaned_count']}/{issue['total_count']} ({issue['percentage']:.2f}%)")
                    
                    # Show sample records
                    if issue['sample_records']:
                        click.echo(f"  Sample orphaned records:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="    ")
                        click.echo()
        else:
            click.echo(f"\nNo orphaned foreign key references found across all tables.")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}")


@dqc.command()
@click.argument('database_url')
@click.option('--numeric-types', is_flag=True, help='Only check numeric columns')
@click.option('--date-types', is_flag=True, help='Only check date/datetime columns')
@click.option('--text-types', is_flag=True, help='Only check text/string columns')
@click.option('--skip-large-tables', is_flag=True, help='Skip tables with more than 500K rows')
@click.option('--skip-table', multiple=True, help='Skip specific tables by name (can be used multiple times)')
def check_encoding(database_url, numeric_types, date_types, text_types, skip_large_tables, skip_table):
    """Check for character encoding issues in all tables of a PostgreSQL database."""
    
    try:
        engine = create_engine(database_url)
        inspector = inspect(engine)
        
        table_names = inspector.get_table_names()
        
        if not table_names:
            click.echo("No tables found in database.")
            return
        
        click.echo(f"Checking character encoding across {len(table_names)} tables...")
        
        encoding_issues = {}
        
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
                
                table_issues, total_rows, type_flags_used = _check_table_encoding_issues(
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
                    
                    click.echo(f"  -> No {' or '.join(flag_descriptions)} columns found")
                    continue
                
                if table_issues:
                    encoding_issues[table_name] = {
                        'issues': table_issues,
                        'total_rows': total_rows
                    }
                    click.echo(f"  -> Found {len(table_issues)} columns with encoding issues")
                else:
                    click.echo(f"  -> No encoding issues found")
                    
            except Exception as e:
                click.echo(f"  -> Error: {str(e)}")
                continue
        
        # Display results
        if encoding_issues:
            click.echo(f"\nCharacter encoding issues found in {len(encoding_issues)} tables:")
            click.echo("=" * 70)
            
            for table_name, data in encoding_issues.items():
                click.echo(f"\nTable: {table_name} (Total rows: {data['total_rows']})")
                click.echo("-" * 50)
                
                for issue in data['issues']:
                    issue_description = ', '.join(issue['issue_types'])
                    click.echo(f"  {issue['column']}: {issue_description}")
                    click.echo(f"  Total issues: {issue['total_issues']}/{issue['total_rows']} ({issue['percentage']:.2f}%)")
                    
                    # Show sample records
                    if issue['sample_records']:
                        click.echo(f"  Sample records with encoding issues:")
                        _display_dataframe(issue['sample_columns'], issue['sample_records'], indent="    ")
                        click.echo()
        else:
            if numeric_types or date_types or text_types:
                flag_descriptions = []
                if numeric_types:
                    flag_descriptions.append("numeric")
                if date_types:
                    flag_descriptions.append("date/datetime")
                if text_types:
                    flag_descriptions.append("text/string")
                column_description = f"{' and '.join(flag_descriptions)} columns"
            else:
                column_description = "text/string columns"
            
            click.echo(f"\nNo character encoding issues found in {column_description} across all tables.")
            
    except Exception as e:
        click.echo(f"Error: {str(e)}")


if __name__ == '__main__':
    dqc()