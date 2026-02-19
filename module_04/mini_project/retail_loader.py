import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from retail_etl import run_retail_pipeline


# ============================================
# STEP 1: QUALITY CHECKS
# ============================================

def run_quality_checks(df):
    """Validate data before loading into database"""
    
    print("\n" + "=" * 60)
    print("QUALITY CHECKS — START")
    print("=" * 60)

    checks = []
    
    # No nulls in critical columns 
    for col in ['CustomerID', 'Description', 'Quantity', 'UnitPrice']:
        null_count = df[col].isnull().sum()
        
        passed = null_count == 0
        
        checks.append({
            'check': f'No nulls in {col}',
            'passed': passed,
            'details': f'{null_count} nulls' if not passed else 'Clean'
        })
        
        
    # Quantity must be positive
    bad_qty = (df['Quantity'] <= 0).sum()
    checks.append({
        'check': 'Quantity > 0',
        'passed': bad_qty == 0,
        'details': f'{bad_qty} invalid' if bad_qty > 0 else 'Clean'
    })



    # UnitPrice must be positive
    bad_price = (df['UnitPrice'] <= 0).sum()
    checks.append({
        'check': 'UnitPrice > 0',
        'passed': bad_price == 0,
        'details': f'{bad_price} invalid' if bad_price > 0 else 'Clean'
    })
    
    # TotalAmount must be positive
    bad_total = (df['TotalAmount'] <= 0).sum()
    checks.append({
        'check': 'TotalAmount > 0',
        'passed': bad_total == 0,
        'details': f'{bad_total} invalid' if bad_total > 0 else 'Clean'
    })
    
    # No duplicates
    dupes = df.duplicated().sum()
    checks.append({
        'check': 'No duplicate rows',
        'passed': dupes == 0,
        'details': f'{dupes} duplicates' if dupes > 0 else 'Clean'
    })
    
    # Print report
    print(f"\n  {'CHECK':<35} {'STATUS':<8} {'DETAILS'}")
    print("  " + "-" * 60)

    all_passed = True
    for c in checks:
        status = 'PASS' if c['passed'] else 'FAIL'
        symbol = '[+]' if c['passed'] else '[X]'
        print(f"  {symbol} {c['check']:<32} {status:<8} {c['details']}")
        if not c['passed']:
            all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print(f"ALL {len(checks)} CHECKS PASSED — Data ready for loading")
    else:
        print("CHECKS FAILED — DO NOT LOAD")
    print("=" * 60)

    return all_passed



# ============================================
# STEP 2: CREATE DATABASE
# ============================================

def create_database(db_name):
    """Create PostgreSQL database if it doesn't exist."""
    password = quote_plus('Root@1432')
    admin_engine = create_engine(
        f'postgresql://postgres:{password}@localhost:5432/postgres',
        isolation_level='AUTOCOMMIT'
    )

    with admin_engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        )
        if result.fetchone() is None:
            conn.execute(text(f'CREATE DATABASE {db_name}'))
            print(f"  Created database: {db_name}")
        else:
            print(f"  Database already exists: {db_name}")

    admin_engine.dispose()
    
    

# ============================================
# STEP 3: CONNECT TO DATABASE
# ============================================

def connect_to_db(db_name):
    """Connect to the target PostgreSQL database."""
    password = quote_plus('Root@1432')
    engine = create_engine(
        f'postgresql://postgres:{password}@localhost:5432/{db_name}'
    )
    print(f"  Connected to: {db_name}")
    return engine




# ============================================
# STEP 4: LOAD DATA INTO DATABASE
# ============================================

def load_to_database(df, engine, table_name):
    """Load DataFrame into PostgreSQL table."""
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False
    )
    print(f"  Loaded {len(df):,} rows into table: {table_name}")
    



# ============================================
# STEP 5: SQL ANALYTICS
# ============================================

def run_sql_analytics(engine, table_name):
    """Run SQL queries for business insights."""
    print("\n=== SQL ANALYTICS ===")

    # 1. Total revenue and transactions
    query = f'''
        SELECT
            COUNT(*) as total_transactions,
            ROUND(SUM("TotalAmount")::numeric, 2) as total_revenue,
            COUNT(DISTINCT "CustomerID") as unique_customers
        FROM {table_name}
    '''
    result = pd.read_sql(query, engine)
    print(f"\n  OVERVIEW:")
    print(f"    Transactions: {result['total_transactions'].iloc[0]:,}")
    print(f"    Revenue: {result['total_revenue'].iloc[0]:,.2f}")
    print(f"    Customers: {result['unique_customers'].iloc[0]:,}")

    # 2. Revenue by country (top 10)
    query = f'''
        SELECT "Country",
               COUNT(*) as transactions,
               ROUND(SUM("TotalAmount")::numeric, 2) as revenue
        FROM {table_name}
        GROUP BY "Country"
        ORDER BY revenue DESC
        LIMIT 10
    '''
    result = pd.read_sql(query, engine)
    print(f"\n  TOP 10 COUNTRIES BY REVENUE:")
    print(result.to_string(index=False))

    # 3. Monthly revenue trend
    query = f'''
        SELECT "Year", "Month",
               ROUND(SUM("TotalAmount")::numeric, 2) as revenue
        FROM {table_name}
        GROUP BY "Year", "Month"
        ORDER BY "Year", "Month"
    '''
    result = pd.read_sql(query, engine)
    print(f"\n  MONTHLY REVENUE:")
    print(result.to_string(index=False))

    # 4. Top 10 products by revenue
    query = f'''
        SELECT "Description",
               SUM("Quantity") as total_qty,
               ROUND(SUM("TotalAmount")::numeric, 2) as revenue
        FROM {table_name}
        GROUP BY "Description"
        ORDER BY revenue DESC
        LIMIT 10
    '''
    result = pd.read_sql(query, engine)
    print(f"\n  TOP 10 PRODUCTS BY REVENUE:")
    print(result.to_string(index=False))

    # 5. Top 10 customers by spending
    query = f'''
        SELECT "CustomerID",
               COUNT(*) as transactions,
               ROUND(SUM("TotalAmount")::numeric, 2) as total_spent
        FROM {table_name}
        GROUP BY "CustomerID"
        ORDER BY total_spent DESC
        LIMIT 10
    '''
    result = pd.read_sql(query, engine)
    print(f"\n  TOP 10 CUSTOMERS BY SPENDING:")
    print(result.to_string(index=False))
    
    


# ============================================
# STEP 6: RUN RETAIL LOADER
# ============================================

def run_retail_loader(filepath, db_name, table_name):
    """Full ETL: Clean -> Validate -> Load -> Analyze."""
    print("=" * 60)
    print("RETAIL LOADER — START")
    print("=" * 60)

    # EXTRACT & TRANSFORM (from Video 14)
    df = run_retail_pipeline(filepath)

    # VALIDATE
    passed = run_quality_checks(df)
    if not passed:
        print("\nQUALITY CHECKS FAILED — Aborting load!")
        return

    # LOAD
    print("\n--- DATABASE LOADING ---")
    create_database(db_name)
    engine = connect_to_db(db_name)
    load_to_database(df, engine, table_name)

    # VERIFY
    print("\n--- VERIFICATION ---")
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
        count = result.scalar()
        print(f"  Row count in DB: {count:,}")

    # SQL ANALYTICS
    run_sql_analytics(engine, table_name)

    engine.dispose()

    print("\n" + "=" * 60)
    print("RETAIL LOADER COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    run_retail_loader('data/OnlineRetail.csv','retail_analytics_db', 'transactions')