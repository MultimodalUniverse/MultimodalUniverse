#!/usr/bin/env python

import argparse
from target_lists import TargetListManager
import os

def main():
    parser = argparse.ArgumentParser(description="List available TESS targets for a sector")
    parser.add_argument('-s', '--sector', type=int, required=True, 
                        help="TESS Sector number")
    parser.add_argument('--cadence', type=str, choices=['2m', '20s'], default='2m',
                        help="Cadence to use (2m or 20s)")
    parser.add_argument('--gi-only', action='store_true',
                        help="Only show Guest Investigator targets")
    parser.add_argument('--cache-dir', type=str, default=None,
                        help="Directory to cache downloaded target lists")
    args = parser.parse_args()

    # Create target list manager
    manager = TargetListManager(cache_dir=args.cache_dir)
    
    try:
        # Get target IDs
        target_ids = manager.get_target_ids(args.sector, args.cadence, args.gi_only)
        
        # Print summary
        target_type = "Guest Investigator" if args.gi_only else "All"
        print(f"\nSector {args.sector} {args.cadence} {target_type} Targets:")
        print(f"Total targets: {len(target_ids)}")
        
        # Print a sample of targets
        if target_ids:
            print("\nSample of target IDs:")
            for tid in list(target_ids)[:10]:
                print(f"  TIC {tid}")
            
            if len(target_ids) > 10:
                print(f"  ... and {len(target_ids) - 10} more")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main() 