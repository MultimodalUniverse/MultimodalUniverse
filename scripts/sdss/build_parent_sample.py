import os
import argparse
import numpy as np
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import healpy as hp
import h5py

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
    filename, fiber_ids, mjds = args
    # Open each fiber file and extract the spectrum
    object_ids = []
    fluxes = []
    ivars  = []
    loglams = []
    wdisps = []
    max_length = 0
    for fiber, mjd in zip(fiber_ids, mjds):
        try:
            hdus = fits.open(filename.format(mjd, str(fiber).zfill(4)))
            data = hdus[1].data
            loglam = data["loglam"].astype(np.float32)
            flux = data["flux"].astype(np.float32)
            ivar = data["ivar"].astype(np.float32)
            wdisp = data["wdisp"].astype(np.float32) # Wavelength dispersion (sigma of fitted Gaussian) in units of number of pixel
            # apply bitmask, remove small values
            mask = data["and_mask"].astype(bool) | (ivar <= 1e-6)
            ivar[mask] = 0
            # Save object id to double check and join catalogs later
            object_id = hdus[2].data["SPECOBJID"][0]
            fluxes.append(flux)
            ivars.append(ivar)
            loglams.append(loglam)
            wdisps.append(wdisp)
            object_ids.append(object_id)
            max_length = max(max_length, len(flux))
        except:
            print("Error reading file", filename.format(mjd, str(fiber).zfill(4)))
            continue
    # Pad all spectra to the same length
    for i in range(len(fluxes)):
        fluxes[i] = np.pad(fluxes[i], (0, max_length - len(fluxes[i])), mode='constant')
        ivars[i] = np.pad(ivars[i], (0, max_length - len(ivars[i])), mode='constant')
        loglams[i] = np.pad(loglams[i], (0, max_length - len(loglams[i])), mode='constant', constant_values=-1)
        wdisps[i] = np.pad(wdisps[i], (0, max_length - len(wdisps[i])), mode='constant', constant_values=-1)
        
    # Stack the spectra
    flux = np.stack(fluxes, axis=0)
    ivar = np.stack(ivars, axis=0)
    lamb = 10**np.stack(loglams, axis=0) # Convert from loglam to lam in (wavelength [Å])
    lsf_sigma = np.stack(wdisps, axis=0)
    object_ids = np.stack(object_ids,axis=0).astype('S22')
        
    # Return the results
    return {'object_id': object_ids,
            'spectrum_lambda': lamb.astype(np.float32), 
            'spectrum_flux': flux, 
            'spectrum_ivar': ivar,
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
        fiberid = group['FIBERID']
        mjd = group['MJD']
        filename = "spec-%s-{}-{}.fits"%str(plate).zfill(4)
        map_args += [(os.path.join(sdss_data_path, survey.strip(),str(plate).zfill(4), filename), 
                      fiberid, mjd)]

    # Process all files
    results = []
    for args in map_args:
        results.append(processing_fn(args))

    # Pad all spectra to the same length
    max_length = max([len(d['spectrum_flux'][0]) for d in results])
    for i in range(len(results)):
        results[i]['spectrum_flux'] = np.pad(results[i]['spectrum_flux'], ((0,0),(0, max_length - len(results[i]['spectrum_flux'][0]))), mode='constant')
        results[i]['spectrum_ivar'] = np.pad(results[i]['spectrum_ivar'], ((0,0),(0, max_length - len(results[i]['spectrum_ivar'][0]))), mode='constant')
        results[i]['spectrum_lambda'] = np.pad(results[i]['spectrum_lambda'], ((0,0),(0, max_length - len(results[i]['spectrum_lambda'][0]))), mode='constant', constant_values=-1)
        results[i]['spectrum_lsf_sigma'] = np.pad(results[i]['spectrum_lsf_sigma'], ((0,0),(0, max_length - len(results[i]['spectrum_lsf_sigma'][0]))), mode='constant', constant_values=-1)
        
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
    catalog['healpix'] = hp.ang2pix(64, catalog['PLUG_RA'], catalog['PLUG_DEC'], lonlat=True, nest=True)

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