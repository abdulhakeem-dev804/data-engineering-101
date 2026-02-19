import pandas as pd
from clean_pipeline import run_pipeline


# ============================================
# CHECK 1: NO NULLS IN CRITICAL COLUMNS
# ============================================

def check_no_nulls(df, columns):
    """Verify that specified columns have zero null values."""
    results = []
    for col in columns:
        null_count = df[col].isnull().sum()
        passed = null_count == 0
        results.append({
            'check': f'No nulls in {col}',
            'passed': passed,
            'details': f'{null_count} nulls found' if not passed else 'Clean'
        })
    return results



# ============================================
# CHECK 2: VALUES WITHIN EXPECTED RANGE
# ============================================

def check_value_range(df, col, min_val, max_val):
    """Verify that all values in a column fall within min-max range."""
    out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
    passed = len(out_of_range) == 0
    return {
        'check': f'{col} in range [{min_val}, {max_val}]',
        'passed': passed,
        'details': f'{len(out_of_range)} values out of range' if not passed else 'Clean'
    }
    
    
# ============================================
# CHECK 3: ONLY ALLOWED VALUES IN COLUMN
# ============================================

def check_allowed_values(df, col, allowed):
    """Verify that a column only contains expected values."""
    actual = set(df[col].dropna().unique())
    unexpected = actual - set(allowed)
    passed = len(unexpected) == 0
    return {
        'check': f'{col} has only allowed values',
        'passed': passed,
        'details': f'Unexpected: {unexpected}' if not passed else 'Clean'
    }
    

# ============================================
# CHECK 4: NO DUPLICATE ROWS
# ============================================

def check_no_duplicates(df):
    """Verify that no exact duplicate rows exist."""
    dupe_count = df.duplicated().sum()
    passed = dupe_count == 0
    return {
        'check': 'No duplicate rows',
        'passed': passed,
        'details': f'{dupe_count} duplicates found' if not passed else 'Clean'
    }
    
    

# ============================================
# CHECK 5: CORRECT COLUMN DATA TYPES
# ============================================

def check_column_types(df, expected_types):
    """Verify that columns have the expected data types."""
    results = []
    for col, expected in expected_types.items():
        actual = str(df[col].dtype)
        passed = expected in actual
        results.append({
            'check': f'{col} dtype is {expected}',
            'passed': passed,
            'details': f'Got {actual}' if not passed else 'Clean'
        })
    return results



# ============================================
# RUN ALL QUALITY CHECKS
# ============================================

def run_quality_checks(df):
    """Run all data quality checks and return pass/fail status."""
    print("=" * 50)
    print("DATA QUALITY CHECKS — START")
    print("=" * 50)

    all_results = []

    # CHECK 1: No nulls in critical columns
    critical_columns = ['Exam_Score', 'Hours_Studied', 'Attendance']
    all_results.extend(check_no_nulls(df, critical_columns))

    # CHECK 2: Value ranges
    all_results.append(check_value_range(df, 'Exam_Score', 0, 101))
    all_results.append(check_value_range(df, 'Hours_Studied', 0, 50))
    all_results.append(check_value_range(df, 'Attendance', 0, 100))

    # CHECK 3: Allowed values
    all_results.append(check_allowed_values(
        df, 'Motivation_Level', ['Low', 'Medium', 'High']
    ))
    all_results.append(check_allowed_values(
        df, 'Internet_Access', ['Yes', 'No']
    ))

    # CHECK 4: No duplicates
    all_results.append(check_no_duplicates(df))

    # CHECK 5: Column types
    expected_types = {
        'Exam_Score': 'int',
        'Hours_Studied': 'int',
        'Attendance': 'int',
        'Motivation_Level': 'object'
    }
    all_results.extend(check_column_types(df, expected_types))

    # REPORT
    print("\n{:<45} {:<8} {}".format('CHECK', 'STATUS', 'DETAILS'))
    print("-" * 75)

    passed_count = 0
    failed_count = 0

    for result in all_results:
        status = 'PASS' if result['passed'] else 'FAIL'
        symbol = '✅' if result['passed'] else '❌'
        print(f"  {symbol} {result['check']:<42} {status:<8} {result['details']}")

        if result['passed']:
            passed_count += 1
        else:
            failed_count += 1

    total = passed_count + failed_count
    all_passed = failed_count == 0

    print("\n" + "=" * 50)
    if all_passed:
        print(f"ALL {total} CHECKS PASSED — Data is ready for loading")
    else:
        print(f"FAILED: {failed_count}/{total} checks failed — DO NOT LOAD")
    print("=" * 50)

    return all_passed






# ============================================
# MAIN
# ============================================

if __name__ == '__main__':
    # Get clean data from our pipeline
    df = run_pipeline('data/StudentPerformanceFactors.csv')

    # Run quality checks
    print("\n")
    passed = run_quality_checks(df)

    if passed:
        print("\nData quality verified — safe to load into database.")
    else:
        print("\nData quality issues found — fix before loading!")