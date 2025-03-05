#!/usr/bin/env python

import argparse
import logging
import sys
from target_lists import TargetListManager

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_sector(manager, sector, cadence, gi_only):
    """Test downloading and parsing a target list for a specific sector"""
    print(f"\n=== Testing Sector {sector}, {cadence} cadence ({'GI only' if gi_only else 'All targets'}) ===")
    
    try:
        # Try to get the URL
        url = manager.get_target_list_url(sector, cadence, gi_only)
        print(f"Target list URL: {url}")
        
        # Try to download the file
        file_path = manager.download_target_list(sector, cadence, gi_only)
        print(f"Downloaded to: {file_path}")
        
        # Try to get target IDs
        target_ids = manager.get_target_ids(sector, cadence, gi_only)
        print(f"Found {len(target_ids)} target IDs")
        
        if target_ids:
            print("Sample of target IDs:")
            for tid in list(target_ids)[:5]:
                print(f"  {tid}")
            return True
        else:
            print("No target IDs found")
            return False
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test TESS target list downloading")
    parser.add_argument('-s', '--sector', type=int, default=None, help="TESS sector number")
    parser.add_argument('--cadence', choices=['2m', '20s'], default=None, help="Cadence")
    parser.add_argument('--gi-only', action='store_true', help="Only get Guest Investigator targets")
    parser.add_argument('--test-all', action='store_true', help="Test multiple sectors and cadences")
    args = parser.parse_args()
    
    manager = TargetListManager(cache_dir="./target_lists_cache")
    
    if args.test_all:
        # Test a range of sectors with different cadences
        test_sectors = [1, 10, 20, 30, 40, 50, 60]
        cadences = ['2m', '20s']
        
        results = []
        
        for sector in test_sectors:
            for cadence in cadences:
                for gi_only in [False, True]:
                    success = test_sector(manager, sector, cadence, gi_only)
                    results.append((sector, cadence, gi_only, success))
        
        # Print summary
        print("\n=== Test Summary ===")
        for sector, cadence, gi_only, success in results:
            status = "SUCCESS" if success else "FAILED"
            target_type = "GI" if gi_only else "All"
            print(f"Sector {sector:3d}, {cadence:4s}, {target_type:3s}: {status}")
        
        # Count successes
        successes = sum(1 for _, _, _, success in results if success)
        print(f"\nTotal: {successes}/{len(results)} tests passed")
        
        return 0 if successes > 0 else 1
    else:
        # Test a single sector
        sector = args.sector or 1
        cadence = args.cadence or '2m'
        success = test_sector(manager, sector, cadence, args.gi_only)
        return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 