#!/usr/bin/env python

import argparse
from database import DatabaseManager
import os
import pandas as pd
from tabulate import tabulate

def main():
    parser = argparse.ArgumentParser(description="List completed TESS sectors")
    parser.add_argument('--db_path', type=str, default="./tess_downloads.db", 
                        help="Path to the database file")
    parser.add_argument('--pipeline', type=str, choices=['spoc', 'qlp', 'tglc', 'all'], default='all',
                        help="Filter by pipeline")
    args = parser.parse_args()

    # Verify database exists
    if not os.path.exists(args.db_path):
        print(f"Error: Database not found at {args.db_path}")
        return

    # Create database manager
    db_manager = DatabaseManager(args.db_path)
    
    # Get completed sectors
    if args.pipeline != 'all':
        completed = db_manager.get_completed_sectors(args.pipeline)
        pipeline_filter = args.pipeline
    else:
        completed = db_manager.get_completed_sectors()
        pipeline_filter = None
    
    if not completed:
        print(f"No completed sectors found" + 
              (f" for pipeline {pipeline_filter}" if pipeline_filter else ""))
        return
    
    # Convert to DataFrame for nice display
    df = pd.DataFrame(completed, 
                     columns=['Sector', 'Pipeline', 'Completion Time', 'Files', 'Success'])
    
    # Calculate success rate
    df['Success Rate'] = (df['Success'] / df['Files'] * 100).round(2).astype(str) + '%'
    
    # Group by pipeline if showing all
    if args.pipeline == 'all':
        # Print summary by pipeline
        print("\nSummary by Pipeline:")
        summary = df.groupby('Pipeline').agg({
            'Sector': 'count',
            'Files': 'sum',
            'Success': 'sum'
        }).reset_index()
        summary['Success Rate'] = (summary['Success'] / summary['Files'] * 100).round(2).astype(str) + '%'
        summary.rename(columns={'Sector': 'Sectors'}, inplace=True)
        print(tabulate(summary, headers='keys', tablefmt='pretty', showindex=False))
        
        # Print details grouped by pipeline
        for pipeline, group in df.groupby('Pipeline'):
            print(f"\nCompleted sectors for pipeline {pipeline}:")
            print(tabulate(group, headers='keys', tablefmt='pretty', showindex=False))
    else:
        # Print all sectors for the specified pipeline
        print(f"\nCompleted sectors for pipeline {args.pipeline}:")
        print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))
    
    # Print overall statistics
    total_files = df['Files'].sum()
    total_success = df['Success'].sum()
    overall_rate = (total_success / total_files * 100) if total_files > 0 else 0
    
    print(f"\nOverall statistics:")
    print(f"Total sectors: {len(df)}")
    print(f"Total files: {total_files}")
    print(f"Successfully downloaded: {total_success} ({overall_rate:.2f}%)")

if __name__ == '__main__':
    main() 