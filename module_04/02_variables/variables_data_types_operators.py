name = "John Doe"
age = 28
salary = 75000.50
is_active = True 

print(name)
print(type(name))
print(type(age))
print(type(salary))
print(type(is_active))



# Naming conventions (PEP 8)
# Use snake_case for variables and functions
customer_id = 1001           # Good — descriptive
total_revenue = 250000.00    # Good — clear meaning
# x = 250000.00              # Bad — what does x mean?

# Use UPPERCASE for constants
MAX_RETRIES = 3
DB_HOST = "localhost"
BATCH_SIZE = 1000



# Data Types - The BIG 6 typs

# Int - Whole numbers
row_count = 1000000
batch_size = 500 

print(f"Processing {row_count} rows in batches of {batch_size}")


# Float - Decimal numbers 
price = 29.99
tax_rate = 0.08
total = price * (1 + tax_rate)
print(f"Total price with tax: {total:.2f}")

# Bool - True or False
is_valid = True
has_data = False
print(f"Valid: {is_valid}, Has Data: {has_data}")


# String - Text data 

# f-strings — embed variables directly
table = "orders"
count = 5000
elapsed = 2.37

print(f"SELECT * FROM {table}") 
print(f"Processed {count} records in {elapsed:.2f} seconds")
print(f"data/2024/export_{table}.csv")


# String cleaning methods 
raw_name = "  John Doe  "
print(f"Original: '{raw_name}'")
print(f"Strip(): '{raw_name.strip()}'")
print(f"Lower(): '{raw_name.lower()}'")
print(f"Upper(): '{raw_name.upper()}'")

# replace() — swap characters
messy = "hello-world-data"
clean = messy.replace("-","_")
print(f"replace: {messy} → {clean}")

# split() — break into parts
csv_line = "id,name,email,created_at"
columns = csv_line.split(",")
print(f"split: {columns}")


# Lists



columns = ["id", "name", "email", "created_at"]
print(f"Columns: {columns}")
print(f"First column: {columns[0]}")
print(f"Second column: {columns[1]}")
print(f"Last column: {columns[-1]}")

columns.append("updated_at")
print(f"After append: {columns}")

print("Iterating over columns:")
for col in columns:
    print(f"Column: {col}")
    
    



# dictionaries

config = {
    "host": "localhost",
    "port": 5432,
    "database": "sales_db",
    "user": "etl_user",
    "password": "secure_password_123"
}

print(f"Connecting to: {config['host']}:{config['port']}")
print(f"Database: {config['database']}")

password = config.get("password", "not set")
print(f"Password: {password}")


# Type Conversion


row_count = "1000"
price_str = "29.99"

# result = row_count + 500 # TypeError: can only concatenate str (not "int") to str

row_count = int(row_count)
price = float(price_str)

print(f"Row count: {row_count} (type: {type(row_count).__name__})")
print(f"Price: {price} (type: {type(price).__name__})")

# Coverting numbers to strings 
total_records = 5000
message = f"Processed {total_records} records"
print(message)
# print(f"Processed {total_records} records")


bad_value = "hello"
try:
    result = int(bad_value)
except ValueError as e:
    print(F"Error: Cannot convert `{bad_value}` to int - {e}")
    result = 0

print(f"Result: {result}")


# OPERATORS — Arithmetic, Comparison, Logical


#Arithmetic
revenue = 50000
cost = 32000
profit = revenue - cost 
margin = (profit / revenue) * 100
print(f"Profit: ${profit}, Margin: {margin:.1f}%")


# Floor division & modulo — useful for batching
total_rows = 1000
batch_size = 300
full_batches = total_rows // batch_size
remaining = total_rows % batch_size
print(f"Full batches: {full_batches}, Remaining rows: {remaining}")



# Comparison - Returns True or False 
row_count = 1500 
print(f"Has data: {row_count > 0}")
print(f"Is Large: {row_count > 10000}")
print(f"Exactly 1500: {row_count == 1500}")

# Logical - combine conditions
is_valid = True 
has_data = row_count > 0 
should_process = is_valid and has_data 
print(f"Should process: {should_process}")
