import argparse

PIPELINES = ['qlp', 'spoc', 'tglc']

def main():
    parser = argparse.ArgumentParser(description="This script downloads TESS pipeline data to a user-provided endpoint.")
    parser.add_argument('-s', '--sector', type=int, help="TESS Sector to download.")
    parser.add_argument('-t', '--tiny', action='store_true', help="Use a tiny subset of the data for testing.")
    parser.add_argument('-n', '--n_processes', type=int, help="Number of processes to use for parallel processing.")
    parser.add_argument('--hdf5_output_path', type=str, help="Path to save the hdf5 lightcurve data.")
    parser.add_argument('--data_path', type=str, help="Data path for storing downloaded products.") # This is confusing is it required?
    parser.add_argument('--fits_output_path', type=str, help="Path to save the fits lightcurve data.")
    parser.add_argument('--pipeline', type=str, help=f"TESS pipeline to download. Options are {PIPELINES}.")
    args = parser.parse_args()

    if args.pipeline not in PIPELINES:
        raise ValueError(f"Invalid pipeline {args.pipeline}. Options are {PIPELINES}")

    if args.pipeline == 'spoc':
        from spoc import SPOC_Downloader as Downloader
    elif args.pipeline == 'qlp':
        from qlp import QLP_Downloader as Downloader
    else:
        from tglc import TGLC_Downloader as Downloader
    
    downloader = Downloader(
            sector = args.sector, 
            data_path = args.data_path, 
            hdf5_output_dir = args.hdf5_output_path,
            fits_dir = args.fits_output_path,
            n_processes = args.n_processes
    )
    downloader.download_sector(tiny = args.tiny, show_progress = True, save_catalog = True)
        
if __name__ == '__main__':
    main()