#!/usr/bin/env python

import argparse
from database import DatabaseManager
import pandas as pd
import os
import sqlite3

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def main():
    parser = argparse.ArgumentParser(description="Check failed downloads in the database")
    parser.add_argument('db_path', help="Path to the downloads.db file")
    parser.add_argument('-n', '--num_failures', type=int, default=5, 
                       help="Number of detailed failures to show")
    args = parser.parse_args()

    # Verify database exists
    if not os.path.exists(args.db_path):
        print(f"Error: Database not found at {args.db_path}")
        return

    print(f"Checking database at: {args.db_path}")
    
    try:
        # Quick check of database content
        conn = sqlite3.connect(args.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM downloads")
        total_records = cursor.fetchone()[0]
        print(f"\nTotal records in database: {total_records}")
        
        cursor.execute("SELECT COUNT(*) FROM downloads WHERE status='failed'")
        failed_count = cursor.fetchone()[0]
        print(f"Failed downloads: {failed_count}")
        conn.close()

        # Now use our DatabaseManager
        db = DatabaseManager(args.db_path)
        
        # Print summary
        print("\n=== Failure Summary ===")
        db.print_failure_summary()
        
        # Get and display detailed failures
        failed = db.get_failed_details()
        if not failed.empty:
            print(f"\n=== Detailed Failures ===")
            print(f"Showing {min(args.num_failures, len(failed))} of {len(failed)} failures:")
            print(failed.head(args.num_failures).to_string())
        else:
            print("\nNo failures found in database")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main() 