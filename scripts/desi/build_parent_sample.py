import os
import argparse
import numpy as np
from astropy.table import Table, join
from scipy.optimize import curve_fit
import desispec.io                             
from desispec import coaddition 
from multiprocessing import Pool
import h5py
from tqdm import tqdm

# Set the log level to warning to avoid too much output
os.environ['DESI_LOGLEVEL'] = 'WARNING'

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
    wavelength = combined_spectra.wave['brz'].astype(np.float32)
    flux = combined_spectra.flux['brz'][reordering_idx].astype(np.float32)
    ivar = combined_spectra.ivar['brz'][reordering_idx].astype(np.float32)
    res = combined_spectra.resolution_data['brz'][reordering_idx].astype(np.float32)

    tgt_ids = np.array(combined_spectra.target_ids())[reordering_idx]

    # Get an averaged estimated Gaussian line spread function
    lsf = res.mean(axis=-1).mean(axis=0)
    def _gauss(x, a, x0, sigma):
        return a * np.exp(-(x - x0) ** 2 / (2 * sigma ** 2))
    popt, pcov = curve_fit(_gauss, np.arange(len(lsf)), lsf, p0=[1, 5, 1])
    
    assert np.all(tgt_ids == target_ids), ("There was an error in reading the requested spectra from the file", len(tgt_ids), len(target_ids), target_ids[:10], tgt_ids[:10])

    # Return the results
    return {'TARGETID': tgt_ids,
            'spectrum_lambda_min': wavelength.min()*np.ones(shape=[len(tgt_ids)], dtype=np.float32), 
            'spectrum_lambda_max': wavelength.max()*np.ones(shape=[len(tgt_ids)], dtype=np.float32), 
            'spectrum_flux': flux, 
            'spectrum_ivar': ivar,
            'spectrum_lsf_sigma':  popt[2]*np.ones(shape=[len(tgt_ids)], dtype=np.float32), # The sigma of the estimated Gaussian line spread function, in pixel units
            'spectrum_lsf': res.mean(axis=-1)}


def save_in_standard_format(args):
    """ This function takes care of iterating through the different input files 
    corresponding to this healpix index, and exporting the data in standard format.
    """
    catalog, output_filename, desi_data_path = args
    # Create the output directory if it does not exist
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    # Rename columns to match the standard format
    catalog['ra'] = catalog['TARGET_RA']
    catalog['dec'] = catalog['TARGET_DEC']
    catalog['healpix'] = catalog['HEALPIX']
    catalog['object_id'] = catalog['TARGETID']
    
    # Extract the spectra by looping over all files
    catalog = catalog.group_by(['SURVEY', 'PROGRAM', 'HEALPIX'])
    
    # Preparing the arguments for the parallel processing
    map_args = []
    for group in catalog.groups:
        survey = group['SURVEY'][0]
        program = group['PROGRAM'][0]
        healpix = group['HEALPIX'][0]
        target_ids = np.array(group['TARGETID'])
        map_args += [(os.path.join(desi_data_path, f'coadd-{survey}-{program}-{healpix}.fits'), target_ids)]

    # Process all files
    results = []
    for args in map_args:
        results.append(processing_fn(args))

    # Aggregate all spectra into an astropy table
    spectra = Table({k: np.concatenate([d[k] for d in results],axis=0) 
                     for k in results[0].keys()})

    # Join on target id with the input catalog
    catalog = join(catalog, spectra, keys='TARGETID', join_type='inner')

    # Making sure we didn't lose anyone
    assert len(catalog) == len(spectra), "There was an error in the join operation"

    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            hdf5_file.create_dataset(key, data=catalog[key])
    return 1

def main(args):

    # Load the catalog file and apply main cuts
    catalog = Table.read(os.path.join(args.desi_data_path, "zall-pix-fuji.fits"))
    catalog = catalog[selection_fn(catalog)]

    # Extract the spectra by looping over all files
    catalog = catalog.group_by(['HEALPIX'])

    # Preparing the arguments for the parallel processing
    map_args = []
    for group in catalog.groups:
        # Create a filename for the group
        group_filename = os.path.join(args.output_dir, 'edr_sv3/healpix={}/001-of-001.hdf5'.format(group['HEALPIX'][0]))
        map_args.append((group, group_filename, args.desi_data_path))

    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args)))

    if sum(results) != len(map_args):
        print("There was an error in the parallel processing, some files may not have been processed correctly")
    else:
        print("All done!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from the DESI data release downloaded through Globus')
    parser.add_argument('desi_data_path', type=str, help='Path to the local copy of the DESI data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    main(args)