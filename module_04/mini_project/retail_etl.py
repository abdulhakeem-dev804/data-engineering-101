import pandas as pd
import numpy as np


# STEP 1 : LOAD DATA

def load_data(filepath):
    """Load the online retail csv file"""
    
    df = pd.read_csv(filepath, encoding='latin1')
    print(f"  Loaded: {filepath}")
    print(f"  Shape: {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


# STEP 2: EXPLORE DATA

def explore_data(df):
    """Discover data quality issues."""
    print("\n=== DATA EXPLORATION ===")

    # Null analysis
    print("\n  NULLS:")
    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    for col, count in null_cols.items():
        pct = count / len(df) * 100
        print(f"    {col}: {count:,} ({pct:.1f}%)")

    # Duplicates
    dupes = df.duplicated().sum()
    print(f"\n  DUPLICATES: {dupes:,}")

    # Negative quantities (returns)
    neg_qty = (df['Quantity'] < 0).sum()
    print(f"\n  NEGATIVE QUANTITIES: {neg_qty:,} ({neg_qty/len(df)*100:.1f}%)")
    print(f"    Min quantity: {df['Quantity'].min():,}")

    # Zero/negative prices
    bad_price = (df['UnitPrice'] <= 0).sum()
    print(f"\n  ZERO/NEGATIVE PRICES: {bad_price:,}")

    # Date type
    print(f"\n  InvoiceDate type: {df['InvoiceDate'].dtype} (needs datetime)")

    # Countries
    print(f"\n  COUNTRIES: {df['Country'].nunique()}")
    print(f"    Top 3:")
    for country, count in df['Country'].value_counts().head(3).items():
        print(f"      {country}: {count:,}")

    return df


# STEP 3 : CLEAN DATA

def clean_data(df):
    """Fix all data quality issues."""
    print("\n=== DATA CLEANING ===")
    original = len(df)
    df = df.copy()

    # 1. Drop null CustomerID
    before = len(df)
    df = df.dropna(subset=['CustomerID'])
    dropped = before - len(df)
    print(f"  Dropped {dropped:,} rows with null CustomerID")

    # 2. Drop null Description
    before = len(df)
    df = df.dropna(subset=['Description'])
    dropped = before - len(df)
    print(f"  Dropped {dropped:,} rows with null Description")

    # 3. Remove negative quantities (returns/cancellations)
    before = len(df)
    df = df[df['Quantity'] > 0]
    dropped = before - len(df)
    print(f"  Removed {dropped:,} rows with negative/zero quantity")

    # 4. Remove zero/negative prices
    before = len(df)
    df = df[df['UnitPrice'] > 0]
    dropped = before - len(df)
    print(f"  Removed {dropped:,} rows with zero/negative price")

    # 5. Remove exact duplicates
    before = len(df)
    df = df.drop_duplicates()
    dropped = before - len(df)
    print(f"  Removed {dropped:,} exact duplicates")

    # 6. Standardize Description (title case)
    df['Description'] = df['Description'].str.strip().str.title()
    print(f"  Standardized descriptions to title case")

    # Summary
    removed = original - len(df)
    print(f"\n  CLEANING SUMMARY: {original:,} -> {len(df):,} rows ({removed:,} removed)")
    print(f"  Remaining nulls: {df.isnull().sum().sum()}")

    return df



# Step 4 : TRANSFORM DATA

def transform_data(df):
    """Add calculated fields and parse dates."""
    print("\n=== DATA TRANSFORMATION ===")
    df = df.copy()

    # 1. Parse InvoiceDate to datetime
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    print(f"  Parsed InvoiceDate to datetime")
    print(f"    Range: {df['InvoiceDate'].min()} to {df['InvoiceDate'].max()}")

    # 2. Calculate total amount
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
    print(f"  Added TotalAmount (Quantity x UnitPrice)")
    print(f"    Total revenue: £{df['TotalAmount'].sum():,.2f}")

    # 3. Extract time components
    df['Year'] = df['InvoiceDate'].dt.year
    df['Month'] = df['InvoiceDate'].dt.month
    df['DayOfWeek'] = df['InvoiceDate'].dt.day_name()
    df['Hour'] = df['InvoiceDate'].dt.hour
    print(f"  Extracted: Year, Month, DayOfWeek, Hour")

    # 4. UK vs International flag
    df['Is_UK'] = np.where(df['Country'] == 'United Kingdom', 'UK', 'International')
    print(f"  Added Is_UK flag")

    # 5. Convert CustomerID to integer
    df['CustomerID'] = df['CustomerID'].astype(int)
    print(f"  Converted CustomerID to integer")

    print(f"\n  Final shape: {df.shape[0]:,} rows, {df.shape[1]} columns")

    return df


# STEP 5 : GENRATE ANALYTICS
def generate_analytics(df):
    """Print key business insights from the cleaned data."""
    print("\n=== ANALYTICS ===")

    # Overview
    print(f"  Total transactions: {len(df):,}")
    print(f"  Total revenue: £{df['TotalAmount'].sum():,.2f}")
    print(f"  Unique customers: {df['CustomerID'].nunique():,}")
    print(f"  Unique products: {df['StockCode'].nunique():,}")

    # Revenue by country (top 5)
    print("\n  TOP 5 COUNTRIES BY REVENUE:")
    country_rev = df.groupby('Country')['TotalAmount'].sum().sort_values(ascending=False).head()
    for country, rev in country_rev.items():
        print(f"    {country}: £{rev:,.2f}")

    # Top 10 products by quantity sold
    print("\n  TOP 10 PRODUCTS BY QUANTITY:")
    top_products = df.groupby('Description')['Quantity'].sum().sort_values(ascending=False).head(10)
    for desc, qty in top_products.items():
        print(f"    {desc}: {qty:,} units")

    # Monthly revenue trend
    print("\n  MONTHLY REVENUE TREND:")
    monthly = df.groupby(['Year', 'Month'])['TotalAmount'].sum().round(2)
    for (year, month), rev in monthly.items():
        print(f"    {year}-{month:02d}: £{rev:,.2f}")

    # UK vs International
    print("\n  UK vs INTERNATIONAL:")
    uk_split = df.groupby('Is_UK').agg(
        transactions=('InvoiceNo', 'count'),
        revenue=('TotalAmount', 'sum')
    ).round(2)
    print(uk_split.to_string())

    return df


# STEP 6: RUN PIPELINE

def run_retail_pipeline(filepath):
    """Execute the full ETL pipeline."""
    print("=" * 60)
    print("ONLINE RETAIL ETL PIPELINE — START")
    print("=" * 60)

    df = load_data(filepath)
    df = explore_data(df)
    df = clean_data(df)
    df = transform_data(df)
    df = generate_analytics(df)

    print("\n" + "=" * 60)
    print(f"PIPELINE COMPLETE — {df.shape[0]:,} rows, {df.shape[1]} columns")
    print("=" * 60)

    return df


if __name__ == '__main__':
    df_clean = run_retail_pipeline('data/OnlineRetail.csv')
    print(f"\nNulls remaining: {df_clean.isnull().sum().sum()}")
    print(f"Final shape: {df_clean.shape}")