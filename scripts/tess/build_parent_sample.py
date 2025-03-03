import argparse
from downloaders import SPOC_Downloader, QLP_Downloader, TGLC_Downloader
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('tess_downloader')

PIPELINES = ['qlp', 'spoc', 'tglc']

# Define available sectors for each pipeline
AVAILABLE_SECTORS = {
    'spoc': list(range(1, 81)),  # Sectors 1-80
    'qlp': list(range(1, 81)),   # Sectors 1-80
    'tglc': list(range(1, 53))   # Sectors 1-52
}

def parse_sectors(sector_arg, pipeline):
    """Parse the sector argument into a list of sector numbers"""
    if sector_arg == 'all':
        return AVAILABLE_SECTORS[pipeline]
    
    # Handle comma-separated list of sectors
    if isinstance(sector_arg, str) and ',' in sector_arg:
        sectors = []
        for part in sector_arg.split(','):
            part = part.strip()
            # Handle ranges like "1-5"
            if '-' in part:
                start, end = map(int, part.split('-'))
                sectors.extend(range(start, end + 1))
            else:
                sectors.append(int(part))
        return sectors
    
    # Handle single sector
    return [int(sector_arg)]

def main():
    parser = argparse.ArgumentParser(description="This script downloads TESS pipeline data to a user-provided endpoint.")
    parser.add_argument('-s', '--sector', type=str, 
                       help="TESS Sector(s) to download. Can be a single sector number, a comma-separated list (e.g., '1,2,3'), "
                            "a range (e.g., '1-5'), or 'all' to download all available sectors.")
    parser.add_argument('-t', '--tiny', action='store_true', help="Use a tiny subset of the data for testing.")
    parser.add_argument('-n', '--n_processes', type=int, default=4, help="Number of processes to use for parallel processing.")
    parser.add_argument('--hdf5_output_path', type=str, help="Path to save the hdf5 lightcurve data.")
    parser.add_argument('--data_path', type=str, help="Data path for storing downloaded products.")
    parser.add_argument('--fits_output_path', type=str, help="Path to save the fits lightcurve data.")
    parser.add_argument('--pipeline', type=str, default='spoc', help=f"TESS pipeline to download. Options are {PIPELINES}. Defaults to 'spoc'.")
    parser.add_argument('--db_path', type=str, help="Path to the database file for tracking downloads. Default is data_path/tess_downloads.db")
    parser.add_argument('--resume', action='store_true', help="Resume failed downloads from previous runs")
    parser.add_argument('--skip_existing', action='store_true', help="Skip sectors that already have data in the output directory")
    args = parser.parse_args()

    if args.pipeline not in PIPELINES:
        raise ValueError(f"Invalid pipeline {args.pipeline}. Options are {PIPELINES}")

    # Set default database path if not provided
    if args.db_path is None and args.data_path:
        args.db_path = os.path.join(args.data_path, 'tess_downloads.db')

    # Parse sectors
    sectors = parse_sectors(args.sector, args.pipeline)
    logger.info(f"Preparing to download {len(sectors)} sectors: {sectors}")

    Downloader = {
        'spoc': SPOC_Downloader,
        'qlp': QLP_Downloader,
        'tglc': TGLC_Downloader
    }
    
    # Process each sector
    for sector in sectors:
        # Check if we should skip this sector
        if args.skip_existing and args.hdf5_output_dir:
            sector_dir = os.path.join(args.hdf5_output_dir, f"{args.pipeline}/s{sector:04d}")
            if os.path.exists(sector_dir) and os.listdir(sector_dir):
                logger.info(f"Sector {sector} already exists in {sector_dir}, skipping...")
                continue
        
        logger.info(f"Processing sector {sector}")
        try:
            # Create a new downloader for each sector
            downloader = Downloader[args.pipeline](
                sector=sector, 
                data_path=args.data_path, 
                hdf5_output_dir=args.hdf5_output_path,
                fits_dir=args.fits_output_path,
                n_processes=args.n_processes,
                db_path=args.db_path
            )
            
            downloader.download_sector(
                tiny=args.tiny, 
                show_progress=True, 
                save_catalog=True,
                resume_failed=args.resume
            )
            
            logger.info(f"Completed sector {sector}")
            
        except Exception as e:
            logger.error(f"Error processing sector {sector}: {str(e)}")
            # Continue with next sector instead of stopping
            continue
        
if __name__ == '__main__':
    main()