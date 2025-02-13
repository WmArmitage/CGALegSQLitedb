import urllib.request
import ssl
import sqlite3
import pandas as pd

# Constants
CSV_URL = "https://www.cga.ct.gov/ftp/pub/data/LegislatorDatabase.csv"
CSV_FILE = "LegislatorDatabase.csv"
DB_NAME = "legislators.db"
TABLE_NAME = "legislators"

def download_csv(url, filename):
    """Download the CSV file from a given URL and save it locally."""
    context = ssl.create_default_context()  # Ensure proper SSL handling

    try:
        with urllib.request.urlopen(url, context=context) as response:
            data = response.read().decode("utf-8")
            with open(filename, "w", encoding="utf-8") as file:
                file.write(data)
            print("✅ CSV downloaded successfully.")
            return filename
    except Exception as e:
        print(f"❌ Error downloading CSV: {e}")
        return None

def create_or_update_table(db_name, csv_file, table_name):
    """Create or update an SQLite table from a CSV file."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        # Load CSV into pandas DataFrame
        df = pd.read_csv(csv_file)
        
        # Replace table with updated data
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        
        conn.commit()
        print("✅ Database updated successfully.")
    except Exception as e:
        print(f"❌ Error processing database: {e}")
    finally:
        conn.close()

def main():
    csv_file = download_csv(CSV_URL, CSV_FILE)
    if csv_file:
        create_or_update_table(DB_NAME, csv_file, TABLE_NAME)

if __name__ == "__main__":
    main()
