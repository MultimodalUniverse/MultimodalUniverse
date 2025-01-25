import argparse
import os
from functools import partial
from multiprocessing import Pool
from typing import Dict, List

import h5py
import healpy as hp
import numpy as np
from astropy.table import Table
from filelock import FileLock
import pandas as pd
import wget
import matplotlib.pyplot as plt
from astropy.coordinates import SkyCoord
from astropy import units as u

_PIXEL_SCALE = 0.262 # arcsec per pixel
_healpix_nside = 16
_cutout_size = 96 # pixels, matching the 'MultimodalUniverse/legacysurvey' data.


_SINGLE_CATALOG_VALUES = [
    'ra', 
    'dec',
    'redshift',
    'pixel_scale',
]

_CLUMP_CATALOG_INFORMATION = [
    # Clump fluxes
    'uCFlux', 'e_uCFlux', 'gCFlux', 'e_gCFlux', 'rCFlux', 'e_rCFlux',
    'iCFlux', 'e_iCFlux', 'zCFlux', 'e_zCFlux',
    # Background fluxes
    'uBFlux', 'e_uBFlux', 'gBFlux', 'e_gBFlux', 'rBFlux', 'e_rBFlux',
    'iBFlux', 'e_iBFlux', 'zBFlux', 'e_zBFlux',
    # Clump parameters
    "unusual",
    "completeness",
    "SHAPE_R",
    "SHAPE_E1",
    "SHAPE_E2",
    "X",
    "Y",
    "clump_ra",
    "clump_dec",
]

def print_healpix_error(err, healpix_filename: str):
    print(f"Failed to write {healpix_filename} due to {err}")

def load_clump_catalog(data_url="https://content.cld.iop.org/journals/0004-637X/931/1/16/revision1/apjac6512t3_mrt.txt"):
    """Load and parse the Galaxy Zoo Clump Scout catalog."""
    data_file = "apjac6512t3_mrt.txt"

    # Download if needed
    if not os.path.exists(data_file):
        print(f"Downloading clump catalog from {data_url} ...")
        wget.download(data_url, data_file)
        print("\nDownload complete!")

    # Define column names
    names = [
        'SDSS', 'Index', 'Sample', 'z', 'GRAdeg', 'GDEdeg', 'rFWHM',
        'uGFlux', 'e_uGFlux', 'gGFlux', 'e_gGFlux', 'rGFlux', 'e_rGFlux',
        'iGFlux', 'e_iGFlux', 'zGFlux', 'e_zGFlux',
        'BRAdeg', 'BDEdeg', 'reff', 'logM*', 'logSFR', 'logsSFR',
        'CRAdeg', 'CDEdeg', 'Offset',
        'uCFlux', 'e_uCFlux', 'gCFlux', 'e_gCFlux', 'rCFlux', 'e_rCFlux',
        'iCFlux', 'e_iCFlux', 'zCFlux', 'e_zCFlux',
        'uBFlux', 'e_uBFlux', 'gBFlux', 'e_gBFlux', 'rBFlux', 'e_rBFlux',
        'iBFlux', 'e_iBFlux', 'zBFlux', 'e_zBFlux',
        'fLu', 'Frac', 'Unusual', 'Comp'
    ]
    
    # Read and process the catalog
    df = pd.read_fwf(data_file, names=names, skiprows=79)
    
    # Rename flux columns
    df.rename(columns={
        'z': 'redshift', 
        'Unusual': 'unusual', 
        'Comp': 'completeness',
        'rFWHM': 'SHAPE_R',
        'GRAdeg': 'ra',
        'GDEdeg': 'dec',
        'CRAdeg': 'clump_ra',
        'CDEdeg': 'clump_dec',
    }, inplace=True)
    
    # Convert positions to SkyCoord objects
    galaxy_coords = SkyCoord(df['ra'], df['dec'], unit='deg', frame='icrs')
    clump_coords = SkyCoord(df['clump_ra'], df['clump_dec'], unit='deg', frame='icrs')
    
    # Calculate offsets accounting for cos(dec)
    ra_offset = (clump_coords.ra - galaxy_coords.ra) * np.cos(galaxy_coords.dec.rad)
    dec_offset = clump_coords.dec - galaxy_coords.dec
    
    # Convert to pixel coordinates
    df['X'] = (-ra_offset.arcsec / _PIXEL_SCALE + _cutout_size // 2).astype(int)
    df['Y'] = (-dec_offset.arcsec / _PIXEL_SCALE + _cutout_size // 2).astype(int)
    # Offloading this decision to the user
    # df = df[(df["X"] >= 0) & (df["Y"] >= 0) & (df["X"] < _cutout_size) & (df["Y"] < _cutout_size)]
    # Set shape parameters to circular
    df['SHAPE_E1'] = 0
    df['SHAPE_E2'] = 0
    # Set pixel scale
    df['pixel_scale'] = np.float32(_PIXEL_SCALE)

    os.remove(data_file)

    return df

def _processing_fn(df: pd.DataFrame) -> pd.DataFrame:
    """Get the nearby clumps for each entry in the catalog within a subset of the catalog."""
    # SDSS column is unique
    unique_sdss = df['SDSS'].unique()
    out = {key: [] for key in _CLUMP_CATALOG_INFORMATION + _SINGLE_CATALOG_VALUES}
    out['object_id'] = []
    for sdss in unique_sdss:
        out['object_id'].append(sdss)
        # Sort clumps by offset
        tmp = df[df['SDSS'] == sdss].sort_values(by='Offset', ascending=True)
        for key in _SINGLE_CATALOG_VALUES:
            out[key].append(tmp[key].values[0])
        for key in _CLUMP_CATALOG_INFORMATION:
            out_array = np.zeros(20) # 8 is actually the maximum number of clumps, but using 20 to match the legacysurvey catalog
            out_array[0:len(tmp[key].values)] = tmp[key].values
            out[key].append(out_array)
    return pd.DataFrame(out)

def build_catalog_gzclumps(data_dir: str, output_dir: str, num_processes: int = 1, 
                          n_output_files: int = 10, proc_id: int = None) -> List[str]:
    """Build HDF5 catalog files from the Galaxy Zoo Clump Scout catalog."""
    # Load the clump catalog
    df = load_clump_catalog()
    df = _processing_fn(df)
    
    # Calculate healpix indices for each entry
    npix = hp.nside2npix(_healpix_nside)
    
    df['healpix'] = hp.ang2pix(_healpix_nside, df['ra'], df['dec'], lonlat=True, nest=True)
    # Verify healpix indices are within valid range
    assert (df['healpix'] >= 0).all() and (df['healpix'] < npix).all(), \
           f"Invalid healpix indices found! Should be 0 to {npix-1}"
    # Get unique healpix indices
    unique_healpix = np.unique(df['healpix'].values)
    print(f"Found {len(unique_healpix)} unique HEALPix pixels out of {npix} possible")
    
    # If proc_id is specified, only process that split
    if proc_id is not None:
        splits = np.array_split(unique_healpix, n_output_files)
        unique_healpix = splits[proc_id]
    
    output_files = []
    healpix_num_digits = len(str(hp.nside2npix(_healpix_nside)))
    for healpix in unique_healpix:
        healpix_str = str(healpix).zfill(healpix_num_digits)
        # Create healpix subdirectory
        healpix_dir = os.path.join(output_dir, f'healpix={healpix_str}')
        os.makedirs(healpix_dir, exist_ok=True)
        
        output_file = os.path.join(healpix_dir, '001-of-001.h5')
        output_files.append(output_file)
        
        # Skip if file exists
        if os.path.exists(output_file):
            continue
        
        # Get entries for this healpix pixel
        subset = df[df['healpix'] == healpix]
        # Write to HDF5 file with lock
        lock_file = output_file + '.lock'
        with FileLock(lock_file):
            with h5py.File(output_file, 'w') as f:
                for key in subset.columns:
                    data = subset[key].values
                    # Check if the data is an array of arrays
                    if key in _CLUMP_CATALOG_INFORMATION:
                        data = np.stack(data)
                    f.create_dataset(key, data=data, compression="lzf", chunks=True)

        # Clean up the lock file after we're done with it
        if os.path.exists(lock_file):
            os.remove(lock_file)

    return output_files

def main(args):
    # Create the output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir, exist_ok=True)

    # Check if ran as part of a slurm job, if so, only the procid will be processed
    slurm_procid = int(os.getenv('SLURM_PROCID')) if 'SLURM_PROCID' in os.environ else None

    # Build the catalogs
    catalog_files = build_catalog_gzclumps(
        args.data_dir, args.output_dir, 
        num_processes=args.num_processes,
        n_output_files=args.nsplits,
        proc_id=slurm_procid
    )
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Builds a catalog for the Legacy Survey images from DR10.')
    parser.add_argument('data_dir', default='./gzclumps', type=str, help='Path to the local copy of the data')
    parser.add_argument('output_dir', default='./gzclumps', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=20, help='Number of parallel processes to use')
    parser.add_argument('--catalog_only', action='store_true', help='Only compile the catalog, do not extract cutouts')
    parser.add_argument('--nsplits', type=int, default=10, help='Number of splits for the catalog')
    parser.add_argument('--healpix_idx', nargs="+", type=int, default=None, help='List of healpix indices to process')
    args = parser.parse_args()
    main(args)
