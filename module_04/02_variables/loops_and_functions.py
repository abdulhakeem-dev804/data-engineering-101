# Conditionals - Making Decisions 

row_count = 1500

if row_count > 0:
    print(f"Processing {row_count} rows...")
elif row_count == 0:
    print("No data to process.!")
else:
    print("Error: Row count cannot be negative.")
    
    
# Real piple validation simulation
price = 29.99
quantity = -5
email = "admin@company.com"


#Validate price
if price <=0 :
    print("Invalid Price")
else : 
    print(f"Price ok: ${price}")
    
# Validate quantity
if quantity < 0:
    print(f"Negative quantity : {quantity}")
else:
    print(f"Quantity ok: {quantity}")
    
# Validate email
if "@" in email:
    print(f"Valid Email : {email}")
else: print(f"Invalid Email : {email}") 
    
    

# Combining conditions with and/or
is_valid = True
has_data = row_count > 0

# Both must be true 
if is_valid and has_data:
    print("Ready to process data.")
    
# Either one triggers action
is_urgent = True
has_errors = False 

if is_urgent or has_errors:
    print("Immediate attention required!")
else:
    print("No urgent issues detected.")
    
    
    
    
# Loops - Repeating Actions
# For loop - iterate over a sequence
# While loop - repeat until a condition is met


columns = ["id", "name", "email", "created_at"]

print("Table Columns:")
for col in columns:
    print(f"- {col}")
    
    
# While loop example
print("\n While Loop (Retry):")
attempts = 0 

while attempts < 3:
    print(f" Connection attempt {attempts + 1} ")
    attempts += 1
    
print("Connected..")


raw_names = ["  John Doe  ", " Jane Smith ", " Bob Johnson "]

# One - Line loop to clean data
clean_names = [n.strip().upper() for n in raw_names]
print(f"Clean Names: {clean_names}")



# Functions - Reusable Blocks of Code
def clean_name(name):
    """Strip whitespace and convert to Title."""
    return name.strip().title()

print(f"`{clean_name("  john doe  ")}`")
print(f"`{clean_name(" jane smith ")}`")