import argparse
import glob
import os
from multiprocessing import Pool
import tarfile

import h5py
import healpy as hp
import numpy as np
import requests
from astropy.io import fits
from astropy.table import Table
from tqdm import tqdm


URL = "http://vipers.inaf.it/data/pdr2/spectra/"
SURVEYS = ["VIPERS_W1_SPECTRA_1D_PDR2.tar.gz", "VIPERS_W4_SPECTRA_1D_PDR2.tar.gz"]

SURVEY_SAVE_DIRS = ["vipers_w1", "vipers_w4"]
HEADER_KEYS = ['ID', 'RA', 'DEC', 'REDSHIFT', 'REDFLAG', 'EXPTIME', 'NORM', 'MAG']

_healpix_nside = 16

def download_data(vipers_data_path: str = '', tiny: bool = False):
    """Download the VIPERS data from the web and unpack it into the specified directory."""
    if tiny: 
        surveys = SURVEYS[1:]
    else:
        surveys = SURVEYS

    # Download each file
    for file in surveys:
        local_path = os.path.join(vipers_data_path, file)
        subdirectory_path = os.path.join(vipers_data_path, file.replace(".tar.gz", ""))

        # Create a subdirectory for each file
        if not os.path.exists(subdirectory_path):
            os.makedirs(subdirectory_path)

        # Check if file needs to be downloaded
        if not os.path.exists(local_path):
            print(f"Downloading {file}...")
            response = requests.get(URL + file, stream=True)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            else:
                print(f"Failed to download {file}. Status code: {response.status_code}")
                continue

        # Unpack the tar.gz file into its specific subdirectory
        print(f"Unpacking into {subdirectory_path}...")
        with tarfile.open(local_path, "r:gz") as tar:

            # If tiny, only extract the first 10 files
            if tiny:
                members = tar.getmembers()[:10]
                tar.extractall(path=subdirectory_path, members=members)

            # Otherwise, extract all files
            else:
                tar.extractall(path=subdirectory_path)
        print(f"Unpacked successfully!\n")

        # Remove the tar files
        os.remove(local_path)


def extract_data(filename):
    """Extract the contents of a tar file to a dictionary for each file"""
    hdu = fits.open(filename)
    header = hdu[1].header
    data = hdu[1].data

    results = {}

    # Loop through the header keys and add them to the results dictionary
    for key in HEADER_KEYS:
        if key == "REDSHIFT":
            results["z"] = float(header[key])
        else:
            results[key] = float(header[key])
    
    # Add the spectrum data to the results dictionary
    results['spectrum_flux'] = data['FLUXES'].astype(np.float32)
    results['spectrum_wave'] = data['WAVES'].astype(np.float32)
    results['spectrum_noise'] = data['NOISE'].astype(np.float32)
    results['spectrum_mask'] = data['MASK'].astype(np.float32)
    
    hdu.close()
    return results


def save_in_standard_format(results: Table, survey_subdir: str, nside: int):
    """Save the extracted data in a standard format for the given survey."""
    table = Table(results)

    # Get keys
    keys = table.keys()

    # Get healpix files
    healpix_indices = hp.ang2pix(nside, table['RA'], table['DEC'], lonlat=True, nest=True)
    unique_indices = np.unique(healpix_indices)

    for index in tqdm(unique_indices, desc="Processing HEALPix indices"):
        mask = healpix_indices == index
        grouped_data = table[mask]
        healpix_subdir = os.path.join(survey_subdir, f'healpix={index}')

        if not os.path.exists(healpix_subdir):
            os.makedirs(healpix_subdir)

        output_path = os.path.join(healpix_subdir, '001-of-001.h5')

        with h5py.File(output_path, 'w') as output_file:
            for key in keys:
                output_file.create_dataset(key.lower(), data=grouped_data[key])
            output_file.create_dataset('object_id', data=grouped_data['ID'])
            output_file.create_dataset('healpix', data=np.full(grouped_data['ID'].shape, index))


def main(vipers_data_path: str = '', nside: int = 16, num_processes: int = 10, tiny: bool = False):
    """
    Download and extract the VIPERS spectra into a standard format using HEALPix indices.

    Args:
        vipers_data_path (str): The path to the directory where the VIPERS data is stored.
        nside (int): The nside parameter for the HEALPix indexing.
        num_processes (int): The number of parallel processes to run for extracting the data.
        tiny (bool): Whether to use a tiny subset of the data for testing.
    """
    # If tiny, only process the W4 survey
    if tiny: 
        SURVEYS, SURVEY_SAVE_DIRS = ["VIPERS_W4_SPECTRA_1D_PDR2.tar.gz"], ["vipers_w4"]
    else:
        SURVEYS, SURVEY_SAVE_DIRS = ["VIPERS_W1_SPECTRA_1D_PDR2.tar.gz", "VIPERS_W4_SPECTRA_1D_PDR2.tar.gz"], ["vipers_w1", "vipers_w4"]

    # Create the output directory if it does not exist
    if not os.path.exists(vipers_data_path):
        os.makedirs(vipers_data_path)
    
    # Download the data
    download_data(vipers_data_path, tiny)

    # Loop through the surveys and process them
    for survey, survey_save_dir in zip(SURVEYS, SURVEY_SAVE_DIRS):
        print(f"Processing {survey}...", flush=True)

        # Load all fits file, standardize them and append to HDF5 file
        survey = survey.replace(".tar.gz", "")
        files = glob.glob(os.path.join(vipers_data_path, survey, '*.fits'))
        files = files

        # If tiny, only process the first 10 files
        if tiny: files = files[:10]

        # Extract the data from the files
        with Pool(num_processes) as pool:
            results = list(tqdm(pool.imap(extract_data, files), total=len(files)))
            
        survey_save_dir = os.path.join(vipers_data_path, survey_save_dir)
        if not os.path.exists(survey_save_dir):
            os.makedirs(survey_save_dir)

        save_in_standard_format(results, survey_save_dir, nside)
        print(f"Finished processing {survey}!\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from all VIPERS spectra downloaded from the web')
    parser.add_argument('vipers_data_path', type=str, help='Path to the local copy of the VIPERS data')
    parser.add_argument('--nside', type=str, default=_healpix_nside, help='NSIDE for the HEALPix indexing')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    args = parser.parse_args()

    main(args.vipers_data_path, args.nside, args.num_processes, args.tiny)