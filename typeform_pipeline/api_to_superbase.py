import requests
import psycopg2
import os

# Supabase Database Connection (Replace with your actual credentials)
SUPABASE_URL = "your-supabase-url"
SUPABASE_DB_NAME = "postgres"
SUPABASE_USER = "your-db-user"
SUPABASE_PASSWORD = "your-db-password"
SUPABASE_HOST = "your-db-host"
SUPABASE_PORT = "5432"

# API endpoint
API_URL = "https://api.typeform.com/forms/{form_id}/responses"

# Connect to Supabase PostgreSQL
conn = psycopg2.connect(
    dbname=SUPABASE_DB_NAME,
    user=SUPABASE_USER,
    password=SUPABASE_PASSWORD,
    host=SUPABASE_HOST,
    port=SUPABASE_PORT
)
cur = conn.cursor()

# Fetch data from API
response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()

    # Loop through data and insert into Supabase
    for item in data:
        name = item["name"]
        value = item["value"]

        cur.execute("INSERT INTO api_data (name, value) VALUES (%s, %s)", (name, value))
    
    conn.commit()
    print("Data inserted successfully!")

# Close connection
cur.close()
conn.close()
