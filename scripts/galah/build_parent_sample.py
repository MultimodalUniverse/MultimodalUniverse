import os
import argparse
import numpy as np
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import h5py
from collections import defaultdict
import pandas as pd

# Following https://www.galah-survey.org/dr3/using_the_data/#recommended-flag-values
def selection_fn(catalog):
    mask = catalog['snr_c3_iraf'] > 30            # Reasonable signal-to-noise ratio
    mask &= catalog['flag_sp'] == 0
    mask &= catalog['flag_fe_h'] == 0
    mask &= catalog['survey_name'] == 'galah_main'
    return mask

def process_band_fits(filename):
    spectrum = {}
    try:
        hdus = fits.open(filename)
        
        # Unnormalized, sky-substracted spectrum
        flux = hdus[0].data
        sigma = hdus[1].data
        
        # Normalized spectrum
        norm_flux = hdus[4].data
        
        start_wavelength = hdus[0].header["CRVAL1"]
        dispersion       = hdus[0].header["CDELT1"]
        nr_pixels        = hdus[0].header["NAXIS1"]
        reference_pixel  = hdus[0].header["CRPIX1"]
        if reference_pixel == 0:
            reference_pixel = 1
        
        spectrum['lambda'] = ((np.arange(0,nr_pixels)--reference_pixel+1)*dispersion+start_wavelength)
        spectrum['flux'] = flux
        spectrum['flux_error'] = flux * sigma
        
        norm_start_wavelength = hdus[4].header["CRVAL1"]
        norm_dispersion       = hdus[4].header["CDELT1"]
        norm_nr_pixels        = hdus[4].header["NAXIS1"]
        norm_reference_pixel  = hdus[4].header["CRPIX1"]
        if norm_reference_pixel == 0:
            norm_reference_pixel=1
        spectrum['norm_lambda'] = ((np.arange(0,norm_nr_pixels)--norm_reference_pixel+1)*norm_dispersion+norm_start_wavelength)
        spectrum['norm_flux'] = norm_flux
        spectrum['norm_flux_error'] = norm_flux * sigma
        
        return spectrum
    except FileNotFoundError as e:
        return defaultdict(list)

def processing_fn(args):
    filename_B, filename_G, filename_R, object_id = args
    
    spectrum_B, spectrum_G, spectrum_R = process_band_fits(filename_B), process_band_fits(filename_G), process_band_fits(filename_R)

    # Return the results
    return {'object_id': object_id,
            'spectrum_lambda': np.concatenate([spectrum_B['lambda'], spectrum_G['lambda'], spectrum_R['lambda']]),
            'spectrum_flux': np.concatenate([spectrum_B['flux'], spectrum_G['flux'], spectrum_R['flux']]),
            'spectrum_flux_error': np.concatenate([spectrum_B['flux_error'], spectrum_G['flux_error'], spectrum_R['flux_error']]),
            'spectrum_norm_lambda': np.concatenate([spectrum_B['norm_lambda'], spectrum_G['norm_lambda'], spectrum_R['norm_lambda']]),
            'spectrum_norm_flux': np.concatenate([spectrum_B['norm_flux'], spectrum_G['norm_flux'], spectrum_R['norm_flux']]),
            'spectrum_norm_flux_error': np.concatenate([spectrum_B['norm_flux_error'], spectrum_G['norm_flux_error'], spectrum_R['norm_flux_error']])
            }


def save_in_standard_format(args):
    """ This function takes care of iterating through the different input files 
    corresponding to this healpix index, and exporting the data in standard format.
    """
    catalog, output_filename, galah_data_path = args
    catalog['object_id'] = catalog['sobject_id']
    # Create the output directory if it does not exist
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    # Process all files
    results = [
        processing_fn((
            os.path.join(galah_data_path, str(obj_id) + '1.fits'),
            os.path.join(galah_data_path, str(obj_id) + '2.fits'),
            os.path.join(galah_data_path, str(obj_id) + '3.fits'),
            obj_id
        )) for obj_id in catalog['object_id']
    ]
    
    # Pad all spectra to the same length
    max_length = max([len(d['spectrum_flux']) for d in results])
    for i in tqdm(range(len(results))):
        results[i]['spectrum_flux'] = np.pad(results[i]['spectrum_flux'], (0, max_length - len(results[i]['spectrum_flux'])), mode='constant')
        results[i]['spectrum_flux_error'] = np.pad(results[i]['spectrum_flux_error'], (0, max_length - len(results[i]['spectrum_flux_error'])), mode='constant')
        results[i]['spectrum_lambda'] = np.pad(results[i]['spectrum_lambda'], (0 , max_length - len(results[i]['spectrum_lambda'])), mode='constant', constant_values=-1)
        results[i]['spectrum_norm_flux'] = np.pad(results[i]['spectrum_norm_flux'], (0, max_length - len(results[i]['spectrum_norm_flux'])), mode='constant')
        results[i]['spectrum_norm_flux_error'] = np.pad(results[i]['spectrum_norm_flux_error'], (0, max_length - len(results[i]['spectrum_norm_flux_error'])), mode='constant')
        results[i]['spectrum_norm_lambda'] = np.pad(results[i]['spectrum_norm_lambda'], (0, max_length - len(results[i]['spectrum_norm_lambda'])), mode='constant', constant_values=-1)

    # Aggregate all spectra into an astropy table
    spectra = Table({k: np.vstack([d[k] for d in results])
                     for k in results[0].keys()})
    spectra['object_id'] = spectra['object_id'].flatten()
    
    catalog = catalog[['object_id',
    'teff',
    'e_teff',
    'logg',
    'e_logg',
    'fe_h',
    'e_fe_h',
    'fe_h_atmo',
    'vmic',
    'vbroad',
    'e_vbroad',
    'alpha_fe',
    'e_alpha_fe'
    ]]

    catalog = join(catalog, spectra, keys='object_id', join_type='right')

    # Making sure we didn't lose anyone
    assert len(catalog) == len(spectra), "There was an error in the join operation, probably some spectra files are missing"

    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            hdf5_file.create_dataset(key, data=catalog[key])
    return 1

def main(args):
    # Load the catalog file and apply main cuts
    catalog = Table.read(os.path.join(args.galah_data_path, 'objects.csv'))
    
    catalog = catalog[selection_fn(catalog)]

    # TODO: add multiprocessing, sharding (e.g. field_id?)
    save_in_standard_format((catalog, args.output_dir, args.galah_data_path))

    print("All done!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from all SDSS spectra downloaded through Globus')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('galah_data_path', type=str, help='Path to the local copy of the GALAH data')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    main(args)