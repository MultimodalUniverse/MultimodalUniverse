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

all_files = os.listdir('fits_data')

def process_sample(idx):
    try:
        all_samples = pd.read_csv('all_kepler_samples.csv')
        qs = list(range(1, 18))
        kic = all_samples.loc[idx, 'KID']
        if f'{kic}.fits' in all_files:
            logging.info(f"KIC {kic} already processed")
            return
        search_results = lk.search_lightcurve('KIC ' + str(kic), mission='kepler', cadence='long', quarter=qs)
        lc = search_results.download_all().stitch().remove_nans()
        lc.to_fits(path=f'./fits_data/{kic}.fits', overwrite=True)
        logging.info(f"Processed KIC {kic}, shape: {lc.shape}")
        
        # Additionally, remove the cache directory to ensure it's completely empty
        cache_dir = os.path.expanduser('/root/.lightkurve/cache')
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        
        logging.info(f"Cleared cache for KIC {kic}")
        return
    except Exception as e:
        logging.info(f"Error processing KIC {kic}: {e}")
        return

def download_data_parallel(args):
    os.mkdir('./fits_data', exist_ok=True)
    all_samples = pd.read_csv(args.catalog_path)
    num_samples = len(all_samples)
    indices = np.arange(num_samples)
    if args.tiny:
        indices = indices[:10]
    # Get the number of CPUs from SLURM environment variable
    num_cpus = args.num_processes
    print(num_cpus, indices)
    
    with Pool(num_cpus) as pool:
        pool.map(process_sample, indices)

def download_data():
    all_samples = pd.read_csv('/data/lightPred/tables/all_kepler_samples.csv')
    for idx in range(len(all_samples)):
        process_sample(idx)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Downloads TESS data to a user-provided endpoint.")

    parser.add_argument('catalog_path', type=str, help='Path to the catalog')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    parser.add_argument('-nproc', '--num_processes', type=int, default=8, help="number of processes.")
    args = parser.parse_args()

    download_data_parallel(args)
