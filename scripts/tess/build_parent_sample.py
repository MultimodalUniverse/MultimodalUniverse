import argparse
from downloaders import SPOC_Downloader, QLP_Downloader, TGLC_Downloader

PIPELINES = ['qlp', 'spoc', 'tglc']

def main():
    parser = argparse.ArgumentParser(description="This script downloads TESS pipeline data to a user-provided endpoint.")
    parser.add_argument('-s', '--sector', type=int, help="TESS Sector to download.")
    parser.add_argument('-t', '--tiny', action='store_true', help="Use a tiny subset of the data for testing.")
    parser.add_argument('-n', '--n_processes', type=int, help="Number of processes to use for parallel processing.")
    parser.add_argument('--hdf5_output_path', type=str, help="Path to save the hdf5 lightcurve data.")
    parser.add_argument('--data_path', type=str, help="Data path for storing downloaded products.") # This is confusing is it required?
    parser.add_argument('--fits_output_path', type=str, help="Path to save the fits lightcurve data.")
    parser.add_argument('--pipeline', type=str, default='spoc', help=f"TESS pipeline to download. Options are {PIPELINES}. Defaults to 'spoc'.")
    args = parser.parse_args()

    if args.pipeline not in PIPELINES:
        raise ValueError(f"Invalid pipeline {args.pipeline}. Options are {PIPELINES}")

    Downloader = {
        'spoc': SPOC_Downloader,
        'qlp': QLP_Downloader,
        'tglc': TGLC_Downloader
    }
    
    downloader = Downloader[args.pipeline](
            sector = args.sector, 
            data_path = args.data_path, 
            hdf5_output_dir = args.hdf5_output_path,
            fits_dir = args.fits_output_path,
            n_processes = args.n_processes
    )
    downloader.download_sector(tiny = args.tiny, show_progress = True, save_catalog = True)
        
if __name__ == '__main__':
    main()