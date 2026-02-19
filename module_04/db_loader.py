import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from clean_pipeline import run_pipeline

from quality_checks import run_quality_checks


# Step 1 - create our database

def create_database(db_name):
    """Create a PostgreSQL database if it doesn't exist."""
    password = quote_plus('Root@1432')
    admin_engine = create_engine(
        f'postgresql://postgres:{password}@localhost:5432/postgres',
        isolation_level='AUTOCOMMIT'
    )

    with admin_engine.connect() as conn:
        # Check if database exists
        result = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
        )
        
        if result.fetchone() is None:
            conn.execute(
                text(f'CREATE DATABASE {db_name}')
            )
        else:
            print(f" Database already exists: {db_name}")
            
    admin_engine.dispose()
    
    
#  Step 2 : - Connect to the Database

def connect_to_db(db_name):
    """Connect to the target POSTGRESQL database"""
    
    password = quote_plus('Root@1432')
    engine = create_engine(
        f'postgresql://postgres:{password}@localhost:5432/{db_name}'
    )
    
    print(f"  Connected to : {db_name}")

    
    return engine


#  Step 3 : - LOAD DATA INTO DATABASE

def load_to_database(df, engine, table_name):
    """Load a Datafram into a PostgreSQL table"""
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False
    )
    
    print(f"  Loaded {len(df)} rows into table: {table_name}")
    
    
# Step 4 :-  Verify the load

def verify_load(engine, table_name):
    """Run verification queries against the loaded table."""
    print("\n=== VERIFICATION ===")

    with engine.connect() as conn:
        # Row count
        result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
        count = result.scalar()
        print(f"  Row count: {count}")

        # Column count
        result = conn.execute(text(
            f"SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_name = '{table_name}'"
        ))
        col_count = result.scalar()
        print(f"  Column count: {col_count}")

    # Preview with pandas
    df_check = pd.read_sql(f'SELECT * FROM {table_name} LIMIT 5', engine)
    print(f"\n  Preview (first 5 rows):")
    print(df_check.to_string(index=False))

    # Null check
    df_nulls = pd.read_sql(
        f'SELECT COUNT(*) as total_nulls FROM {table_name} '
        f'WHERE "Exam_Score" IS NULL',
        engine
    )
    print(f"\n  Null Exam_Score rows: {df_nulls['total_nulls'].iloc[0]}")

    print("\n  Verification PASSED")
        

# Step 5 : - RUN Loader

def run_loader(filepath, db_name, table_name):
    """Full ETL: Clean CSV -> Validate -> Load into PostgreSQL."""
    print("=" * 50)
    print("DB LOADER — START")
    print("=" * 50)

    # EXTRACT & TRANSFORM (from Video 11)
    df = run_pipeline(filepath)

    # VALIDATE (from Video 13) — NEW!
    print("\n")
    passed = run_quality_checks(df)
    if not passed:
        print("\n❌ QUALITY CHECKS FAILED — Aborting load!")
        return

    # LOAD
    print("\n--- DATABASE LOADING ---")
    create_database(db_name)
    engine = connect_to_db(db_name)
    load_to_database(df, engine, table_name)

    # VERIFY
    verify_load(engine, table_name)

    engine.dispose()

    print("\n" + "=" * 50)
    print("DB LOADER COMPLETE")
    print("=" * 50)
    
    
if __name__ == '__main__':
    run_loader(
        filepath='data/StudentPerformanceFactors.csv',
        db_name='student_analytics_db',
        table_name='students'
    )