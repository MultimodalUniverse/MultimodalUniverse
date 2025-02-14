import lightkurve as lk
from multiprocessing import Pool
import os
import numpy as np
import pandas as pd
import logging
import shutil
import argparse
import warnings
from astropy.io import fits
from astropy.time import Time
from collections import defaultdict

# Configure logging
logging.basicConfig(
    filename='download.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Suppress lightkurve warnings
warnings.filterwarnings("ignore", module="lightkurve")
logging.getLogger("lightkurve").setLevel(logging.WARNING)

def process_sample(args):
    idx, all_samples, output_path = args
    try:
        kic = all_samples.loc[idx, 'KID']
        search_results = lk.search_lightcurve('KIC ' + str(kic), mission='kepler', cadence='long', quarter=list(range(1, 18)))
        lc_collection = search_results.download_all()
        
        if len(lc_collection) == 0:
            logging.warning(f"No data found for KIC {kic}")
            return
        
        # Process each quarter separately and collect all valid data points
        all_times = []
        all_sap_flux = []
        all_pdcsap_flux = []
        all_sap_flux_err = []
        all_pdcsap_flux_err = []
        all_quality = []
        
        ra, dec = None, None
        
        for lc in lc_collection:
            if not (hasattr(lc, 'sap_flux') and hasattr(lc, 'pdcsap_flux')):
                continue
                
            valid_mask = ~np.isnan(lc.sap_flux) & ~np.isnan(lc.pdcsap_flux)
            if hasattr(lc, 'sap_flux_err') and hasattr(lc, 'pdcsap_flux_err'):
                valid_mask &= ~np.isnan(lc.sap_flux_err) & ~np.isnan(lc.pdcsap_flux_err)
            
            if np.sum(valid_mask) == 0:
                continue
            
            if (ra is None or dec is None) and hasattr(lc, 'meta'):
                if 'RA_OBJ' in lc.meta:
                    ra = lc.meta['RA_OBJ']
                if 'DEC_OBJ' in lc.meta:
                    dec = lc.meta['DEC_OBJ']
                
                
            all_times.append(lc.time.value[valid_mask])
            all_sap_flux.append(lc.sap_flux[valid_mask])
            all_pdcsap_flux.append(lc.pdcsap_flux[valid_mask])
            
            if hasattr(lc, 'sap_flux_err'):
                all_sap_flux_err.append(lc.sap_flux_err[valid_mask])
            else:
                all_sap_flux_err.append(np.full_like(lc.sap_flux[valid_mask], np.nan))
                
            if hasattr(lc, 'pdcsap_flux_err'):
                all_pdcsap_flux_err.append(lc.pdcsap_flux_err[valid_mask])
            else:
                all_pdcsap_flux_err.append(np.full_like(lc.pdcsap_flux[valid_mask], np.nan))
            
            if hasattr(lc, 'quality'):
                all_quality.append(lc.quality[valid_mask])
            else:
                all_quality.append(np.zeros_like(lc.time.value[valid_mask], dtype=np.int32))
        
        # Concatenate all valid data points
        if len(all_times) == 0:
            logging.warning(f"No valid data points found for KIC {kic}")
            return
            
        time_values = np.concatenate(all_times)
        sap_flux_values = np.concatenate(all_sap_flux)
        pdcsap_flux_values = np.concatenate(all_pdcsap_flux)
        sap_flux_err_values = np.concatenate(all_sap_flux_err)
        pdcsap_flux_err_values = np.concatenate(all_pdcsap_flux_err)
        quality_values = np.concatenate(all_quality)
        
        # Create the primary header with metadata
        primary_hdu = fits.PrimaryHDU()
        primary_hdu.header['TELESCOP'] = 'KEPLER'
        primary_hdu.header['KEPLERID'] = kic
        primary_hdu.header['RA_OBJ'] = ra
        primary_hdu.header['DEC_OBJ'] = dec
        primary_hdu.header['OBSMODE'] = 'LC'  # Long Cadence
        
        # Create the columns for the data table
        col_time = fits.Column(name='TIME', format='D', array=time_values)
        col_sap_flux = fits.Column(name='SAP_FLUX', format='D', array=sap_flux_values)
        col_pdcsap_flux = fits.Column(name='PDCSAP_FLUX', format='D', array=pdcsap_flux_values)
        col_sap_flux_err = fits.Column(name='SAP_FLUX_ERR', format='D', array=sap_flux_err_values)
        col_pdcsap_flux_err = fits.Column(name='PDCSAP_FLUX_ERR', format='D', array=pdcsap_flux_err_values)
        col_quality = fits.Column(name='SAP_QUALITY', format='J', array=quality_values)
        
        # Create the table HDU
        cols = [col_time, col_sap_flux, col_pdcsap_flux, col_sap_flux_err, col_pdcsap_flux_err, col_quality]
        table_hdu = fits.BinTableHDU.from_columns(cols)
        
        # Save time metadata if available
        if hasattr(lc_collection[0].time, 'format'):
            table_hdu.header['TIMEFMT'] = lc_collection[0].time.format
        if hasattr(lc_collection[0].time, 'scale'):
            table_hdu.header['TIMESCL'] = lc_collection[0].time.scale
        
        # Create the HDU list and write to file
        hdul = fits.HDUList([primary_hdu, table_hdu])
        fits_path = os.path.join(output_path, f'{kic}.fits')
        hdul.writeto(fits_path, overwrite=True)
        
        logging.info(f"Processed KIC {kic}, saved {len(time_values)} data points")
        
        # Clear cache
        cache_dir = os.path.expanduser('/root/.lightkurve/cache')
        if os.path.exists(cache_dir):
            shutil.rmtree(cache_dir)
        
        logging.info(f"Cleared cache for KIC {kic}")
        return
    except Exception as e:
        logging.error(f"Error processing KIC {kic}: {str(e)}")
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