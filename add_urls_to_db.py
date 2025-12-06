"""
add_urls_to_db.py

Reads a list of product URLs from a text file (e.g., 'links.txt') and
inserts them into the 'products' table in the SQLite database.

It uses 'INSERT OR IGNORE' to safely skip any URLs that already exist
in the database, preventing duplicates and errors.
"""

import sqlite3
import sys
from pathlib import Path

# ---------- Configuration ----------
DB_FILE = "products.db"      # The database file used by the scraper
INPUT_TXT = "links.txt"      # The text file containing URLs, one per line
# -----------------------------------

def add_urls_to_database():
    """Reads URLs from a text file and inserts them into the SQLite database."""
    # Check if the input file exists
    input_file = Path(INPUT_TXT)
    if not input_file.is_file():
        print(f"Error: Input file '{INPUT_TXT}' not found.")
        print(f"Please create this file and add one URL per line.")
        sys.exit(1)

    # Read all non-empty lines from the text file
    with open(input_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    if not urls:
        print(f"No URLs found in '{INPUT_TXT}'. Nothing to add.")
        return

    # Connect to the SQLite database
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Insert URLs. 'INSERT OR IGNORE' prevents errors if a URL (PRIMARY KEY) already exists.
    added_count = 0
    for url in urls:
        cursor.execute("INSERT OR IGNORE INTO products (link) VALUES (?)", (url,))
        if cursor.rowcount > 0:
            added_count += 1

    conn.commit()
    conn.close()

    print(f"Process complete.")
    print(f" - Found {len(urls)} URLs in '{INPUT_TXT}'.")
    print(f" - Added {added_count} new unique URLs to '{DB_FILE}'.")

if __name__ == "__main__":
    add_urls_to_database()
