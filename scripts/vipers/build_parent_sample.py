import os
import requests
import tarfile
import glob
import healpy as hp
from astropy.io import fits
from astropy.table import Table
from multiprocessing import Pool
import numpy as np
from tqdm import tqdm
import h5py


def download_data(vipers_data_path: str = ''):
    """Download the VIPERS data from the web and unpack it into the specified directory."""
    # Create the output directory if it does not exist
    if not os.path.exists(vipers_data_path):
        os.makedirs(vipers_data_path)

    url = "http://vipers.inaf.it/data/pdr2/spectra/"
    files = ["VIPERS_W1_SPECTRA_1D_PDR2.tar.gz", "VIPERS_W4_SPECTRA_1D_PDR2.tar.gz"]

    # Download each file
    for file in files:
        local_path = os.path.join(vipers_data_path, file)
        subdirectory_path = os.path.join(vipers_data_path, file.replace(".tar.gz", ""))

        # Create a subdirectory for each file
        if not os.path.exists(subdirectory_path):
            os.makedirs(subdirectory_path)

        # Check if file needs to be downloaded
        if not os.path.exists(local_path):
            print(f"Downloading {file}...")
            response = requests.get(url + file, stream=True)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            else:
                print(f"Failed to download {file}. Status code: {response.status_code}")
                continue

        # Unpack the tar.gz file into its specific subdirectory
        print(f"Unpacking {file} into {subdirectory_path}...")
        with tarfile.open(local_path, "r:gz") as tar:
            tar.extractall(path=subdirectory_path)
        print(f"{file} unpacked successfully into {subdirectory_path}.")

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
        results[key] = float(header[key])
    
    # Add the spectrum data to the results dictionary
    results['spectrum_flux'] = data['FLUXES'].astype(np.float32)
    results['spectrum_wave'] = data['WAVES'].astype(np.float32)
    results['spectrum_ivar'] = data['NOISE'].astype(np.float32)
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


def main(args):

    # Download the data
    download_data(args.vipers_data_path)

    # Load all fits file, standardize them and append to HDF5 file
    files = glob.glob(os.path.join(args.vipers_data_path, '*.fits'))
    files = files

    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(tqdm(pool.imap(extract_data, files), total=len(files)))

    if len(results) != len(files):
        print("There was an error in the parallel processing, some files may not have been processed correctly")

    # Create HDF5 file in standard format
    save_to_file(results, os.path.join(args.output_dir, 'vipers_dr2.hdf5'))

    # Remove all temp fits files, keep HDF5 and tar.gz
    print ("Cleaning up")
    for f in files:
        pathlib.Path(f).unlink(missing_ok=True)

    print("All done!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from all VIPERS spectra downloaded from the web')
    parser.add_argument('vipers_data_path', type=str, help='Path to the local copy of the VIPERS data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    main(args)