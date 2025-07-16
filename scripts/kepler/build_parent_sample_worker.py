import os
import argparse
import numpy as np
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import h5py
import healpy as hp
import re
import pandas as pd
import pdb
from pathlib import Path
from collections import defaultdict

_healpix_nside = 16

# Breakdown of the different Kepler pipelines

def convert_to_list(string_list:str):
    """
    Convert a string representation of a list to a list.

    Args:
        string_list (str): The string representation of the list.

    Returns:
        List: The list.
    """
    # Extract content within square brackets
    matches = re.findall(r'\[(.*?)\]', string_list)
    if matches:
        # Split by comma, remove extra characters except period, hyphen, underscore, and comma, and strip single quotes
        cleaned_list = [re.sub(r'[^A-Za-z0-9\-/_,.~]', '', s) for s in matches[0].split(',')]
        cleaned_list = [x.replace("PosixPath", "") for x in cleaned_list]
        return cleaned_list
    else:
        return []

def convert_ints_to_list(string:str):
    """
    Convert a string representation of a list of integers to a list of integers.

    Args:
        string (str): The string representation of the list of integers.

    Returns:
        List: The list of integers.
    """
    values = string.strip('[]').split(',')
    return [int(value) for value in values]

def processing_fn(args):
    """ Parallel processing function reading all requested light curves.
    """
    filenames, object_id = args
    # print(f"processing obj {object_id}", flush=True)
    sap_fluxes = []
    pdcsap_fluxes = []
    times = []
    sap_fluxes_errs = []
    pdcsap_fluxes_errs = []
    ra = np.nan
    dec = np.nan
    obsmode = np.nan
    for i, filename in enumerate(filenames):
        try:
            with fits.open(filename, mode='readonly', memmap=True) as hdu:
                # Kepler header parsing
                telescope = hdu[0].header.get('TELESCOP')
                if telescope.lower() == 'kepler':
                    # Kepler-specific header information extraction
                    targetid = hdu[0].header.get('KEPLERID')
                    assert targetid == object_id, "Target ID mismatch"
                    ra = hdu[0].header.get('RA_OBJ')
                    dec = hdu[0].header.get('DEC_OBJ')
                    obsmode = hdu[0].header.get('OBSMODE')
                    # Time handling for Kepler (Kepler Julian Date)
                    time = hdu[1].data['TIME']
                    # Two flux options: SAP (Simple Aperture Photometry) and PDCSAP (Pre-Search Data Conditioning)
                    if 'SAP_FLUX' in hdu[1].columns.names:
                        sap_flux = hdu[1].data['SAP_FLUX']
                        sap_flux_err = hdu[1].data['SAP_FLUX_ERR']
                        pdcsap_flux = hdu[1].data['PDCSAP_FLUX']
                        pdcsap_flux_err = hdu[1].data['PDCSAP_FLUX_ERR']
                    # Some pipelines has only FLUX column. This is usually PSCSAP flux
                    elif 'FLUX' in hdu[1].columns.names:
                        pdcsap_flux = hdu[1].data['FLUX']
                        sap_flux = np.zeros_like(pdcsap_flux)
                        pdcsap_flux_err = hdu[1].data['FLUX_ERR']
                        sap_flux_err = np.zeros_like(pdcsap_flux_err)
                    else:
                        raise ValueError("Unknown flux columns")
                    # Quality flags for Kepler
                    quality = np.asarray(hdu[1].data['SAP_QUALITY'], dtype='int32')
                    good_data_mask = (quality == 0) & \
                                    np.isfinite(time) & \
                                    np.isfinite(sap_flux) & \
                                    np.isfinite(sap_flux_err) & \
                                    np.isfinite(pdcsap_flux) & \
                                    np.isfinite(pdcsap_flux_err)

                    times.append(time[good_data_mask])
                    sap_fluxes.append(sap_flux[good_data_mask])
                    pdcsap_fluxes.append(pdcsap_flux[good_data_mask])
                    sap_fluxes_errs.append(sap_flux_err[good_data_mask])
                    pdcsap_fluxes_errs.append(pdcsap_flux_err[good_data_mask])
                else:
                    raise ValueError(f"Unknown telescope {telescope}")
        except FileNotFoundError as e:
            print(f"File {filename} not found")
            times.append(np.array([]))
            sap_fluxes.append(np.array([]))
            pdcsap_fluxes.append(np.array([]))
            sap_fluxes_errs.append(np.array([]))
            pdcsap_fluxes_errs.append(np.array([]))
        except TypeError as e:
            print(f"bad file {filename}: {str(e)}")
            times.append(np.array([]))
            sap_fluxes.append(np.array([]))
            pdcsap_fluxes.append(np.array([]))
            sap_fluxes_errs.append(np.array([]))
            pdcsap_fluxes_errs.append(np.array([]))


    # # Since each quarter might have different mean we normialize each quarter's light curve to the global mean
    # sap_fluxes = normalize_lightcurve(sap_fluxes)
    # pdcsap_fluxes = normalize_lightcurve(pdcsap_fluxes)
    # Combine times and light curves
    if len(times) > 1:
        times = np.concatenate(times)
        sap_fluxes = np.concatenate(sap_fluxes)
        pdcsap_fluxes = np.concatenate(pdcsap_fluxes)
        sap_fluxes_errs = np.concatenate(sap_fluxes_errs)
        pdcsap_fluxes_errs = np.concatenate(pdcsap_fluxes_errs)
    elif len(times) == 1:
        times = times[0]
        sap_fluxes = sap_fluxes[0]
        pdcsap_fluxes = pdcsap_fluxes[0]
        sap_fluxes_errs = sap_fluxes_errs[0]
        pdcsap_fluxes_errs = pdcsap_fluxes_errs[0]


    # Return the results
    return {
        'object_id': object_id,
        'time': times,
        'sap_flux': sap_fluxes,
        'sap_flux_err': sap_fluxes_errs,
        'pdcsap_flux': pdcsap_fluxes,
        'pdcsap_flux_err': pdcsap_fluxes_errs,
        'ra': ra,
        'dec': dec,
        'cadence': obsmode
    }


def normalize_lightcurve(lc):
        # Combine all quarter's light curves
        combined_lc = np.concatenate(lc)
        # Calculate the mean of the combined light curve
        global_mean = np.nanmean(combined_lc)

        # Normalize each quarter's light curve
        normalized_lc = []
        for quarter_lc in lc:
            quarter_mean = np.nanmean(quarter_lc)
            normalized_quarter = quarter_lc * (global_mean / quarter_mean)
            normalized_lc.append(normalized_quarter)
        return normalized_lc


def save_in_standard_format(args):
    """ Process Kepler light curves and save in standard format with chunking and compression.
    """
    healpix = args.healpix
    catalog = pd.read_csv(args.kepler_catalog_path)
    # catalog.columns = ['kepid', 'ra', 'dec', 'data_file_paths', 'healpix']
    # catalog['healpix'] =
    catalog = catalog[catalog['healpix'] == healpix]
    catalog['data_file_path'] = catalog['data_file_path'].apply(convert_to_list)
    out_path = "/mnt/ceph/users/polymathic/MultimodalUniverse/kepler"
    output_filename = os.path.join(out_path, f"healpix={healpix}/001-of-001.hdf5")

    if os.path.exists(output_filename):
        print(f"healpix {healpix} already done")
        return 1

    print(f"processing healpix {healpix}")

    # Create the output directory if it does not exist
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    # Rename columns to match the standard format
    if 'KID' in catalog.columns:
        catalog['object_id'] = catalog['KID']
    elif 'KIC' in catalog.columns:
        catalog['object_id'] = catalog['KIC']
    elif 'kepid' in catalog.columns:
        catalog['object_id'] = catalog['kepid']
    else:
        raise ValueError("Unknown target ID column")

    # Process all files
    if healpix==910:
        print("processing lightcurve files", flush=True)
    # results = []
    map_args = [(x.data_file_path, x.object_id) for x in catalog.itertuples()]
    print(f"num objects in healpix {healpix}: {len(map_args)}")
    with Pool(os.cpu_count() // 2) as pool:
        results = list(tqdm(pool.imap_unordered(processing_fn, map_args), total=len(map_args), desc=f"healpix {healpix}"))
    # for i, args in enumerate():
    #     results.append(processing_fn(args))

        # if healpix == 910 and i % 100 == 0:
        #     print(f"processed {i} objects", flush=True)

    # Pad all light curves to the same length
    max_length = max([len(d['time']) for d in results])
    for i in range(len(results)):
        results[i]['time'] = np.pad(results[i]['time'], (0, max_length - len(results[i]['time'])), mode='constant',
                                    constant_values=np.nan)
        results[i]['sap_flux'] = np.pad(results[i]['sap_flux'], (0, max_length - len(results[i]['sap_flux'])), mode='constant',
                                    constant_values=np.nan)
        results[i]['sap_flux_err'] = np.pad(results[i]['sap_flux_err'], (0, max_length - len(results[i]['sap_flux_err'])),
                                        mode='constant', constant_values=np.nan)
        results[i]['pdcsap_flux'] = np.pad(results[i]['pdcsap_flux'], (0, max_length - len(results[i]['pdcsap_flux'])),
                                        mode='constant',
                                        constant_values=np.nan)
        results[i]['pdcsap_flux_err'] = np.pad(results[i]['pdcsap_flux_err'],
                                            (0, max_length - len(results[i]['pdcsap_flux_err'])),
                                            mode='constant', constant_values=np.nan)


    # Aggregate all light curves into an astropy table
    lightcurves = Table({k: [d[k] for d in results]
                         for k in results[0].keys()})
    catalog = Table.from_pandas(catalog)

    if healpix == 910:
        print(f"got {len(lightcurves)} lightcurves", flush=True)

    # Convert list columns to strings before joining
    for key in catalog.colnames:
        if isinstance(catalog[key][0], list):
            catalog[key] = [','.join(map(str, item)) for item in catalog[key]]

    # Join on target id with the input catalog
    catalog = join(catalog, lightcurves, keys='object_id', join_type='inner')
    catalog.convert_unicode_to_bytestring()
    catalog.remove_columns(['ra_2', 'dec_2'])
    catalog.rename_column('ra_1', 'ra')
    catalog.rename_column('dec_1', 'dec')
    if healpix == 910:
        print(catalog, flush=True)
    # print(lightcurves)
    # print(len(catalog))
    # print(len(lightcurves))

    # Making sure we didn't lose anyone
    assert len(catalog) == len(lightcurves), "There was an error in the join operation, probably some light curve files are missing"

    # Calculate good chunk sizes
    def get_chunk_size(shape):
        if len(shape) == 1:
            return min(1000, shape[0])  # For 1D arrays
        else:
            return (min(100, shape[0]), min(1000, shape[1]))  # For 2D arrays

    if healpix == 910:
        print("starting to write to hdf5", flush=True)
    # Save all columns to disk in HDF5 format with chunking and compression
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in catalog.colnames:
            data = catalog[key]

            # Convert object dtype to string if necessary
            if data.dtype == object:
                # Convert to fixed-length string
                max_length = max(len(str(item)) for item in data)
                string_dtype = h5py.string_dtype(encoding='ascii', length=max_length)
                data = np.array([str(item) for item in data], dtype=string_dtype)
            # Convert other special types if needed
            elif isinstance(data, Table.Column):
                data = np.array(data)

            try:
                # Get the shape of the data
                shape = data.shape

                # Calculate chunk size based on data shape
                chunks = get_chunk_size(shape)

                # Create the dataset with chunking and compression
                hdf5_file.create_dataset(
                    key,
                    data=data,
                    chunks=chunks,
                    compression="gzip",
                    compression_opts=5  # Compression level (1-9, higher = better compression but slower)
                )
            except TypeError as e:
                print(f"Failed to save column {key} with dtype {data.dtype}: {str(e)}")
                continue
            except ValueError as e:
                print(f"Failed to save column {key} with shape {shape}: {str(e)}")
                continue
            except Exception as e:
                print(f"worker failed: {str(e)}")
                continue
            if healpix == 910:
                print(f"healpix {healpix}: processed key {key}", flush=True)
    print(f"healpix {healpix} complete", flush=True)
    return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts light curves from Kepler data downloaded from MAST')
    parser.add_argument('healpix', type=int, help='Path to the data directory')
    parser.add_argument('--kepler_catalog_path', type=str, help='Path to the local copy of the Kepler catalog')
    args = parser.parse_args()

    save_in_standard_format(args)
