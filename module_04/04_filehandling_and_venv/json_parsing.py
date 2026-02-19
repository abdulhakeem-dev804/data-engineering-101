import json

raw_json = '''
{
    "users": [
        {"id": 1, "name": "Alice", "meta": {"login_count": 15, "role": "admin"}},
        {"id": 2, "name": "Bob", "meta": {"login_count": 3, "role": "user"}}
    ]
}
'''

data = json.loads(raw_json)

# Accessing nested data
for user in data['users']:
    name = user['name']
    role = user['meta']['role'] # Nested access
    print(f"{name} has role: {role}")
  
  
  
  
print("\nReading from config.json ...")  
with open('data/config.json', 'r') as f:
    config = json.load(f)
    
    
# Accessing nest keys
db_host = config['database']['host']
print(f"Database Host: {db_host}")
    