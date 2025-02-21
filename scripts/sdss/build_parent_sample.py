import os
import argparse
import numpy as np
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import healpy as hp
import h5py

_healpix_nside = 16

# Breakdown of the different surveys, each one will be stored as a subdataset
SURVEYS = ['sdss  ', 
           'segue1', 
           'segue2', 
           'boss  ', 
           'eboss ']

def selection_fn(catalog):
    mask = catalog['SPECPRIMARY'] == 1            # Only use the primary spectrum for each object  
    mask &= catalog['TARGETTYPE'] == "SCIENCE "   # Only use science targets (ignore sky and others)
    mask &= catalog['PLATEQUALITY'] == "good    " # Only use plates with good status
    return mask

def processing_fn(args):
    """ Parallel processing function reading all requested spectra from one plate.
    """
    filename, fiber_ids, object_id = args
    fiber_ids = fiber_ids -1 # fiber ids are 1-indexed, we need 0-indexed
    
    # Load the plate file
    hdus = fits.open(filename)

    flux = hdus[0].data[fiber_ids]
    ivar = hdus[1].data[fiber_ids]
    and_mask = hdus[2].data[fiber_ids]
    lsf_sigma = hdus[4].data[fiber_ids]

    # compute bitmask
    mask = and_mask.astype(bool) | (ivar <= 1e-6)

    # The header of hdu[0] contains the following information
    # CRVAL1     Central wavelength (log10) of first pixel
    # CD1_1      Log10 dispersion per pixel
    # CRPIX1     Starting pixel (1-indexed)
    # CTYPE1
    # DC-FLAG    Log-linear flag
    # BUNIT      1E-17 erg/cm^2/s/Ang
    # Let's compute the log lambda values for this flux (this formula has been double checked)
    loglam = hdus[0].header['CRVAL1'] + hdus[0].header['CD1_1'] * (np.arange(len(flux[0])) + 1 - hdus[0].header['CRPIX1'])
    lam = np.repeat(10**loglam.reshape(1,-1),len(fiber_ids),axis=0).astype(np.float32)

    # Return the results
    return {'object_id': object_id,
            'spectrum_lambda': lam.astype(np.float32), 
            'spectrum_flux': flux, 
            'spectrum_ivar': ivar,
            'spectrum_mask': mask,
            'spectrum_lsf_sigma': lsf_sigma}


def save_in_standard_format(args):
    """ This function takes care of iterating through the different input files 
    corresponding to this healpix index, and exporting the data in standard format.
    """
    catalog, output_filename, sdss_data_path = args
    # Create the output directory if it does not exist
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    # Rename columns to match the standard format
    catalog['ra'] = catalog['PLUG_RA']
    catalog['dec'] = catalog['PLUG_DEC']
    catalog['object_id'] = catalog['SPECOBJID']
    
    # Extract the spectra by looping over all files
    catalog = catalog.group_by(['SURVEY', 'PLATE'])
    
    # Preparing the arguments for the parallel processing
    map_args = []
    for group in catalog.groups:
        survey = group['SURVEY'][0]
        plate = group['PLATE'][0]
        mjd = group['MJD'][0]
        fiberid = group['FIBERID']
        object_id = group['object_id']
        filename = "spPlate-{}-{}.fits".format(str(plate).zfill(4), mjd)
        map_args += [(os.path.join(sdss_data_path, survey.strip(),str(plate).zfill(4), filename), 
                      fiberid, object_id)]

    # Process all files
    results = []
    for args in map_args:
        results.append(processing_fn(args))

    # Pad all spectra to the same length
    max_length = max([len(d['spectrum_flux'][0]) for d in results])
    for i in range(len(results)):
        results[i]['spectrum_flux'] = np.pad(results[i]['spectrum_flux'], ((0,0),(0, max_length - len(results[i]['spectrum_flux'][0]))), mode='edge')
        results[i]['spectrum_ivar'] = np.pad(results[i]['spectrum_ivar'], ((0,0),(0, max_length - len(results[i]['spectrum_ivar'][0]))), mode='constant')
        results[i]['spectrum_lambda'] = np.pad(results[i]['spectrum_lambda'], ((0,0),(0, max_length - len(results[i]['spectrum_lambda'][0]))), mode='constant', constant_values=-1)
        results[i]['spectrum_lsf_sigma'] = np.pad(results[i]['spectrum_lsf_sigma'], ((0,0),(0, max_length - len(results[i]['spectrum_lsf_sigma'][0]))), mode='edge')
        results[i]['spectrum_mask'] = np.pad(results[i]['spectrum_mask'], ((0,0),(0, max_length - len(results[i]['spectrum_mask'][0]))), mode='constant', constant_values=True)
        
    # Aggregate all spectra into an astropy table
    spectra = Table({k: np.concatenate([d[k] for d in results], axis=0) 
                     for k in results[0].keys()})

    # Join on target id with the input catalog
    catalog = join(catalog, spectra, keys='object_id', join_type='inner')

    # Making sure we didn't lose anyone
    assert len(catalog) == len(spectra), "There was an error in the join operation, probably some spectra files are missing"

    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            hdf5_file.create_dataset(key, data=catalog[key])
    return 1

def main(args):
    # Load the catalog file and apply main cuts
    catalog = Table.read(os.path.join(args.sdss_data_path, "specObj-dr17.fits"))
    catalog = catalog[selection_fn(catalog)]

    # Add healpix index to the catalog
    catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['PLUG_RA'], catalog['PLUG_DEC'], lonlat=True, nest=True)

    # Exporting each survey as a separate dataset
    for survey in SURVEYS:
        print("Processing survey:", survey)

        cat_survey = catalog[catalog['SURVEY'] == survey]
        cat_survey = cat_survey.group_by(['healpix'])

        # Preparing the arguments for the parallel processing
        map_args = []
        for group in cat_survey.groups:
            # Create a filename for the group
            group_filename = os.path.join(args.output_dir, '{}/healpix={}/001-of-001.hdf5'.format(survey.strip(), group['healpix'][0]))
            map_args.append((group, group_filename, args.sdss_data_path))

        # Run the parallel processing
        with Pool(args.num_processes) as pool:
            results = list(tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args)))

        if sum(results) != len(map_args):
            print("There was an error in the parallel processing, some files may not have been processed correctly")

    print("All done!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from all SDSS spectra downloaded through Globus')
    parser.add_argument('sdss_data_path', type=str, help='Path to the local copy of the SDSS data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    main(args)