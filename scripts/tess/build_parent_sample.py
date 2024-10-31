import os
import argparse
import numpy as np
import pandas as pd
import pdb
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import h5py
import healpy as hp
from quality import TESSQualityFlags
from pathlib import Path
import shutil

_healpix_nside = 16

# Breakdown of the different TESS pipelines, each one will be stored as a subdataset
PIPELINES = ['spoc  ']

def processing_fn(args):
    """ Parallel processing function reading all requested light curves from one sector.
    """
    filename, object_id = args

    with fits.open(filename, mode='readonly', memmap=True) as hdu:
        # The header of hdu[0] contains the following information:
        # Page 34, Section 5 - https://archive.stsci.edu/files/live/sites/mast/files/home/missions-and-data/active-missions/tess/_documents/EXP-TESS-ARC-ICD-TM-0014-Rev-F.pdf
        try:
            telescope = hdu[0].header.get('TELESCOP')
            if telescope == 'TESS' and hdu[0].header.get('ORIGIN') == 'NASA/Ames':
                targetid = hdu[0].header.get('TICID')
                ra = hdu[0].header.get('RA_OBJ')
                dec = hdu[0].header.get('DEC_OBJ')

                time = hdu['LIGHTCURVE'].data['TIME']
                time_format = 'btjd'
                # Units: BTJD (Barycenter corrected TESS Julian Date; BJD - 2457000, days)

                flux = hdu['LIGHTCURVE'].data['PDCSAP_FLUX']
                flux_err = hdu['LIGHTCURVE'].data['PDCSAP_FLUX_ERR']
                # Units: e-/s (electrons per second) -> can be read from the flux files, see the TESS data products documentation (TUNIT4)

                quality = np.asarray(hdu['LIGHTCURVE'].data['QUALITY'], dtype='int32')
                quality_bitmask = TESSQualityFlags.DEFAULT_BITMASK
        except Exception as e:
            print(f"Error processing {filename}: {e}", flush=True)
            return None

    exclude_bad_data = True
    if exclude_bad_data:
        indx = TESSQualityFlags.filter(quality, flags=quality_bitmask)
        time, flux, flux_err = time[indx], flux[indx], flux_err[indx]

    # Return the results
    return {
            'object_id': object_id,
            'time': time,
            'flux': flux,
            'flux_err': flux_err
            }

# Open all files and extract the targetid, ra, dec
def process_file(filename):
    with fits.open(filename, mode='readonly', memmap=True) as hdu:
        targetid = hdu[0].header.get('TICID')
        ra = hdu[0].header.get('RA_OBJ')
        dec = hdu[0].header.get('DEC_OBJ')
    return targetid, ra, dec, filename

def save_in_standard_format(args):
    """ This function takes care of iterating through the different input files
    corresponding to this healpix index, and exporting the data in standard format.
    """
    catalog, output_filename = args
    output_filename = Path(output_filename)

    # Create the output directory if it does not exist
    if output_filename.parent.exists():
        print(f"output dir {output_filename.parent} already exists, deleting...")
        shutil.rmtree(output_filename.parent)
    output_filename.parent.mkdir(parents=True)

    # Process all files
    results = []
    for row in catalog[['filename', 'object_id']].itertuples():
        results.append(processing_fn((row.filename, row.object_id)))

    # Pad all light curves to the same length
    results_cleaned = [d for d in results if d is not None]
    print(f"number of light curves after cleaning: {len(results_cleaned)} / {len(results)}")
    max_length = max([len(d['time']) for d in results_cleaned])
    for i in range(len(results_cleaned)):
        results_cleaned[i]['time'] = np.pad(results_cleaned[i]['time'], (0,max_length - len(results_cleaned[i]['time'])), mode='constant')
        results_cleaned[i]['flux'] = np.pad(results_cleaned[i]['flux'], (0,max_length - len(results_cleaned[i]['flux'])), mode='constant')
        results_cleaned[i]['flux_err'] = np.pad(results_cleaned[i]['flux_err'], (0,max_length - len(results_cleaned[i]['flux_err'])), mode='constant')

    # Aggregate all light curves into an astropy table
    lightcurves = Table({k: [d[k] for d in results_cleaned]
                     for k in results_cleaned[0].keys()})
    # Convert catalog to astropy table
    catalog = catalog.drop(columns=['filename'])
    if 'ra' in catalog.columns:
        catalog = catalog.rename(columns={'ra': 'RA', 'dec': 'DEC'})
    catalog_table = Table.from_pandas(catalog)

    # Join catalog_table with lightcurves
    catalog = join(catalog_table, lightcurves, keys='object_id', join_type='inner')

    # Making sure we didn't lose anyone
    assert len(catalog) == len(lightcurves), "There was an error in the join operation, probably some spectra files are missing"

    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            if catalog[key].dtype == 'object':
                #TODO: time is a list of lists but nothing else is??
                print(key)
                # print(catalog[key].values[:5])
                print(type(catalog[key][0]))
            hdf5_file.create_dataset(key, data=catalog[key])
    return 1

def main(args):
    output_dir = Path(args.output_dir)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)

    # First write a catalog of all sources
    if (output_dir / 'catalog.csv').exists():
        catalog = pd.read_csv(output_dir / 'catalog.csv')
        print(f"catalog with {len(catalog)} rows loaded from {output_dir / 'catalog.csv'}")
    else:
        print("catalog not found, creating new catalog")
        files_generator = Path(args.tess_data_path).glob("**/*.fits")

        with Pool(processes=args.num_processes) as pool:
            results = list(tqdm(pool.imap(process_file, files_generator, chunksize=10_000),
                                desc="creating catalog from files"))

        targetids, ras, decs, filenames = zip(*results)

        catalog = pd.DataFrame({
            "object_id": targetids,
            "RA": ras,
            "DEC": decs,
            "filename": filenames
        })
        catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['RA'], catalog['DEC'], lonlat=True, nest=True)

        # save to csv
        catalog.to_csv(output_dir / 'catalog.csv', index=False)
        print(f"Catalog saved to {str(output_dir / 'catalog.csv')}")

    # Now process the lightcurves using the info in the catalog
    map_args = []
    # Group by healpix
    df_grouped = catalog.groupby('healpix')
    for i, (_, group) in enumerate(df_grouped):
        # Create a filename for the group
        group_filename = output_dir / 'spoc/healpix={}/001-of-001.hdf5'.format(group['healpix'].iloc[0])
        map_args.append((group, group_filename))

    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args)))

    if sum(results) != len(map_args):
        print("There was an error in the parallel processing, some files may not have been processed correctly")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts light curves from all TESS light curves downloaded from MAST')
    parser.add_argument('tess_data_path', type=str, help='Path to the local copy of the TESS data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('-nproc', '--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    args = parser.parse_args()

    main(args)
