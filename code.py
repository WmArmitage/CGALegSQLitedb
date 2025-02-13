import sqlite3
import pandas as pd
import requests
import os

# Define database and CSV URL
DB_NAME = "data.db"
TABLE_NAME = "my_table"
CSV_URL = "https://www.cga.ct.gov/ftp/pub/data/LegislatorDatabase.csv"

def download_csv(url, filename="data.csv"):
    """Download the CSV file from a given URL."""
    response = requests.get(url)
    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)
        print("CSV downloaded successfully.")
        return filename
    else:
        print("Failed to download CSV.")
        return None

def create_or_update_table(db_name, csv_file, table_name):
    """Create an SQLite table if it doesn't exist and insert/update data."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Read CSV into pandas DataFrame
    df = pd.read_csv(csv_file)
    
    # Infer SQL table schema from CSV headers
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    
    conn.commit()
    conn.close()
    print("Database updated successfully.")

def main():
    csv_file = download_csv(CSV_URL)
    if csv_file:
        create_or_update_table(DB_NAME, csv_file, TABLE_NAME)

if __name__ == "__main__":
    main()
