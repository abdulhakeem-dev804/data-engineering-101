import pandas as pd
import numpy as np
import os

# Resolve paths relative to this script's location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, '..', '..', 'data')


# ============================================
# STEP 1: LOAD DATA
# ============================================

def load_data(filepath):
    """Load CSV file and print basic info."""
    df = pd.read_csv(filepath)
    print(f"  Loaded: {filepath}")
    print(f"  Shape: {df.shape[0]} rows, {df.shape[1]} columns")
    return df


# ============================================
# STEP 2: INSPECT DATA
# ============================================

def inspect_data(df):
    """Check for nulls, duplicates, and data types."""
    print("\n=== DATA INSPECTION ===")

    # Null check
    nulls = df.isnull().sum()
    null_cols = nulls[nulls > 0]
    if len(null_cols) > 0:
        print("  Columns with nulls:")
        for col, count in null_cols.items():
            print(f"    {col}: {count} ({count/len(df)*100:.1f}%)")
    else:
        print("  No nulls found")

    # Duplicate check
    dupes = df.duplicated().sum()
    print(f"  Duplicates: {dupes}")

    # Data types
    print(f"  Numeric columns: {len(df.select_dtypes(include='number').columns)}")
    print(f"  Text columns: {len(df.select_dtypes(include='object').columns)}")

    return df


# ============================================
# STEP 3: HANDLE MISSING VALUES
# ============================================

def handle_missing(df):
    """Fill missing values: mode for categorical, median for numeric."""
    df = df.copy()

    for col in df.select_dtypes(include='object').columns:
        if df[col].isnull().sum() > 0:
            fill_value = df[col].mode()[0]
            df[col] = df[col].fillna(fill_value)
            print(f"  Filled '{col}' nulls with mode: '{fill_value}'")

    for col in df.select_dtypes(include='number').columns:
        if df[col].isnull().sum() > 0:
            fill_value = df[col].median()
            df[col] = df[col].fillna(fill_value)
            print(f"  Filled '{col}' nulls with median: {fill_value}")

    print(f"  Remaining nulls: {df.isnull().sum().sum()}")
    return df


# ============================================
# STEP 4: STANDARDIZE TEXT
# ============================================

def standardize_text(df):
    """Strip whitespace and apply title case to text columns."""
    df = df.copy()
    text_cols = df.select_dtypes(include='object').columns

    for col in text_cols:
        df[col] = df[col].str.strip().str.title()

    print(f"  Standardized {len(text_cols)} text columns")
    return df


# ============================================
# STEP 5: REMOVE DUPLICATES
# ============================================

def remove_duplicates(df):
    """Remove exact duplicate rows."""
    df = df.copy()
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    print(f"  Removed {removed} duplicates ({before} -> {len(df)} rows)")
    return df


# ============================================
# STEP 6: ADD FEATURES
# ============================================

def add_features(df):
    """Engineer new columns from existing data."""
    df = df.copy()

    # Encode motivation level
    df['Motivation_Score'] = df['Motivation_Level'].map(
        {'Low': 1, 'Medium': 2, 'High': 3}
    )

    # Binary encode internet access
    df['Has_Internet'] = df['Internet_Access'].map({'Yes': 1, 'No': 0})

    # Pass/Fail flag
    df['Pass_Fail'] = np.where(df['Exam_Score'] >= 65, 'Pass', 'Fail')

    # Score band
    df['Score_Band'] = pd.cut(
        df['Exam_Score'],
        bins=[0, 60, 70, 80, 90, 101],
        labels=['Very Low', 'Low', 'Medium', 'High', 'Very High']
    )

    new_cols = ['Motivation_Score', 'Has_Internet', 'Pass_Fail', 'Score_Band']
    print(f"  Added {len(new_cols)} features: {new_cols}")
    return df


# ============================================
# STEP 7: GENERATE SUMMARY
# ============================================

def generate_summary(df):
    """Print key analytics from the cleaned data."""
    print("\n=== ANALYTICS SUMMARY ===")

    # Overall stats
    print(f"  Total students: {len(df)}")
    print(f"  Average exam score: {df['Exam_Score'].mean():.1f}")
    print(f"  Pass rate: {(df['Pass_Fail'] == 'Pass').mean()*100:.1f}%")

    # By motivation
    print("\n  Score by Motivation Level:")
    motivation_stats = df.groupby('Motivation_Level', observed=True).agg(
        avg_score=('Exam_Score', 'mean'),
        count=('Exam_Score', 'count')
    ).round(1)
    print(motivation_stats.to_string(index=True))

    return df


# ============================================
# STEP 8: RUN PIPELINE
# ============================================

def run_pipeline(filepath):
    """Execute the full cleaning pipeline."""
    print("=" * 50)
    print("CLEANING PIPELINE — START")
    print("=" * 50)

    df = load_data(filepath)
    df = inspect_data(df)

    print("\n--- CLEANING ---")
    df = handle_missing(df)
    df = standardize_text(df)
    df = remove_duplicates(df)

    print("\n--- ENRICHMENT ---")
    df = add_features(df)

    df = generate_summary(df)

    print("\n" + "=" * 50)
    print(f"PIPELINE COMPLETE — {df.shape[0]} rows, {df.shape[1]} columns")
    print("=" * 50)

    return df


if __name__ == '__main__':
    df_clean = run_pipeline('data/StudentPerformanceFactors.csv')
    print(f"\nNulls remaining: {df_clean.isnull().sum().sum()}")
    print(f"Final shape: {df_clean.shape}")