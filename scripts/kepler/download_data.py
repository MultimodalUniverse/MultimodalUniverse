import lightkurve as lk
from multiprocessing import Pool
import os
import numpy as np
import pandas as pd
import logging
import shutil
import argparse

# Configure logging
logging.basicConfig(
    filename='download.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_sample(args):
    idx, all_samples, output_path = args
    all_files = os.listdir(output_path)
    try:
        kic = all_samples.loc[idx, 'KID']
        # if f'{kic}.fits' in all_files:
        #     logging.info(f"KIC {kic} already processed")
        #     return
        search_results = lk.search_lightcurve('KIC ' + str(kic), mission='kepler', cadence='long', quarter=list(range(1, 18)))
        lc = search_results.download_all().stitch().remove_nans()
        lc.to_fits(path=f'{output_path}/{kic}.fits', overwrite=True)
        logging.info(f"Processed KIC {kic}, shape: {lc.flux.shape}")
        
        # Additionally, remove the cache directory to ensure it's completely empty
        cache_dir = os.path.expanduser('/root/.lightkurve/cache')
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        
        logging.info(f"Cleared cache for KIC {kic}")
        return
    except Exception as e:
        logging.info(f"Error processing KIC {kic}: {e}")
        return

def download_data(args):
    os.makedirs(args.output_path, exist_ok=True)
    all_samples = pd.read_csv(args.catalog_path)
    num_samples = len(all_samples)
    indices = np.arange(num_samples)
    if args.tiny:
        indices = indices[:10]
    num_cpus = args.num_processes
    process_args = [(idx, all_samples, args.output_path) for idx in indices]
    
    print(f'Starting download {num_samples} samples with {num_cpus} processes', flush=True)
    with Pool(num_cpus) as pool:
        pool.map(process_sample, process_args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Downloads Kepler data to a user-provided endpoint.")

    parser.add_argument('catalog_path', type=str, help='Path to the catalog')
    parser.add_argument('output_path', type=str, help='Path to the output directory')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    parser.add_argument('-nproc', '--num_processes', type=int, default=8, help="number of processes.")
    args = parser.parse_args()

    download_data(args)
