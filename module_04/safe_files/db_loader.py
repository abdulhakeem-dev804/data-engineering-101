"""
Video 12: Lab — CSV to Database Loader
Module 04: Python for Data Processing
SelfcodeAcademy Data Engineering Course

Loads cleaned data into PostgreSQL.
Run: python db_loader.py
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from clean_pipeline import run_pipeline

# Resolve paths relative to this script's location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', '..', 'data')


# ============================================
# STEP 1: CREATE DATABASE
# ============================================

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
            text(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        )
        if result.fetchone() is None:
            conn.execute(text(f'CREATE DATABASE {db_name}'))
            print(f"  Created database: {db_name}")
        else:
            print(f"  Database already exists: {db_name}")

    admin_engine.dispose()


# ============================================
# STEP 2: CONNECT TO DATABASE
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
# STEP 3: LOAD DATA INTO DATABASE
# ============================================

def load_to_database(df, engine, table_name):
    """Load a DataFrame into a PostgreSQL table."""
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False
    )
    print(f"  Loaded {len(df)} rows into table: {table_name}")


# ============================================
# STEP 4: VERIFY THE LOAD
# ============================================

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


# ============================================
# STEP 5: RUN LOADER
# ============================================

def run_loader(filepath, db_name, table_name):
    """Full ETL: Clean CSV -> Load into PostgreSQL."""
    print("=" * 50)
    print("DB LOADER — START")
    print("=" * 50)

    # EXTRACT & TRANSFORM (from Video 11)
    df = run_pipeline(filepath)

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


# ============================================
# MAIN
# ============================================

if __name__ == '__main__':
    filepath = os.path.join(DATA_DIR, 'StudentPerformanceFactors.csv')
    run_loader(
        filepath=filepath,
        db_name='student_analytics',
        table_name='students'
    )
