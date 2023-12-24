import os
import argparse
import numpy as np
from astropy.table import Table
import desispec.io                             
from desispec import coaddition 
from multiprocessing import Pool
import h5py
from tqdm import tqdm

# Set the log level to warning to avoid too much output
os.environ['DESI_LOGLEVEL'] = 'WARNING'

DESI_COLUMNS = [
    'TARGETID',
    'SURVEY',
    'PROGRAM',
    'HEALPIX',
    'TARGET_RA',
    'TARGET_DEC',
    'Z',
    'EBV',
    'FLUX_G',
    'FLUX_R',
    'FLUX_Z',
    'FLUX_IVAR_G',
    'FLUX_IVAR_R',
    'FLUX_IVAR_Z'
]

def selection_fn(catalog):
    """ Returns a mask for the catalog based on the selection function
    """
    mask = catalog['SURVEY'] == 'sv3'           # Only use data from the one percent survey
    mask &= catalog['SV_PRIMARY']               # Only use the primary spectrum for each object  
    mask &= catalog['OBJTYPE'] == 'TGT'         # Only use targets (ignore sky and others)
    mask &= catalog['COADD_FIBERSTATUS'] == 0   # Only use fibers with good status
    return mask

def find_matching_indices(arr1, arr2):
    sort_idx_arr1 = np.argsort(arr1)
    sort_idx_arr2 = np.argsort(arr2)
    inverse_sort_idx_arr1 = np.argsort(sort_idx_arr1)
    matching_indices = sort_idx_arr2[inverse_sort_idx_arr1]
    return matching_indices

def processing_fn(args):
    """ Parallel processing function reading a spectrum file and returning the spectra
    of all requested targets in that file.
    """
    filename, target_ids = args

    # Load and select the requested targets
    spectra = desispec.io.read_spectra(filename).select(targets=target_ids)

    # Coadd the cameras
    combined_spectra = coaddition.coadd_cameras(spectra)

    # Reorder the spectra to match the target ids
    reordering_idx = find_matching_indices(target_ids, combined_spectra.target_ids())
    
    # Extract fluxes and ivars
    wave = combined_spectra.wave['brz'][reordering_idx].astype(np.float32)
    flux = combined_spectra.flux['brz'][reordering_idx].astype(np.float32)
    ivar = combined_spectra.ivar['brz'][reordering_idx].astype(np.float32)
    tgt_ids = np.array(combined_spectra.target_ids())[reordering_idx]
    
    assert np.all(tgt_ids == target_ids), ("There was an error in reading the requested spectra from the file", len(tgt_ids), len(target_ids), target_ids[:10], tgt_ids[:10])

    # Return the results
    return {'target_ids': tgt_ids,
            'wave': wave, 
            'flux': flux, 
            'ivar': ivar}

def main(args):

    # Load the catalog file and apply main cuts
    catalog = Table.read(os.path.join(args.desi_data_path, "zall-pix-fuji.fits"))
    catalog = catalog[selection_fn(catalog)]

    # Select the columns we want to keep
    catalog = catalog[DESI_COLUMNS]

    # Save the catalog
    catalog_filename = os.path.join(args.output_dir, 'desi_catalog.fits')
    catalog.write(catalog_filename, overwrite=True)

    # Extract the spectra by looping over all files
    catalog = catalog.group_by(['SURVEY', 'PROGRAM', 'HEALPIX'])
    
    # Preparing the arguments for the parallel processing
    map_args = []
    for group in catalog.groups:
        survey = group['SURVEY'][0]
        program = group['PROGRAM'][0]
        healpix = group['HEALPIX'][0]
        target_ids = np.array(group['TARGETID'])
        map_args += [(os.path.join(args.desi_data_path, f'coadd-{survey}-{program}-{healpix}.fits'), target_ids)]

    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(tqdm(pool.imap(processing_fn, map_args), total=len(map_args)))

    # Export the results to disk in hdf5 format
    hdf5_filename = os.path.join(args.output_dir, 'desi_spectra.hdf5')
    with h5py.File(hdf5_filename, 'w') as hdf5_file:
        for key in ['target_ids', 'wave', 'flux', 'ivar']:
            hdf5_file.create_dataset(key, data=np.concatenate([result[key] for result in results]))

    # Export the catalog as astropy table 
    catalog_filename = os.path.join(args.output_dir, 'desi_catalog.fits')
    catalog.write(catalog_filename, overwrite=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from the DESI data release downloaded through Globus')
    parser.add_argument('desi_data_path', type=str, help='Path to the local copy of the DESI data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    main(args)