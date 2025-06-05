import argparse
import time
import os
import cv2 
import tarfile
import requests
import h5py
import numpy as np
import healpy as hp

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

from filelock import FileLock
from astropy.io import fits
from astropy.nddata.utils import Cutout2D
from astropy.table import Table, join
from astropy.wcs import WCS
from tqdm import tqdm


# Define the filters for each wavelength type
filters = ['f200w', 'f356w']

_healpix_nside = 16
_cutout_size = 96

# Magnitude cut in the reference filter for each mosaic
_mag_auto_cut = {'ceers': 27. }

# Minimum number of filters to retain an object in the catalog
_min_filters_cut = 2

# PSF FWHM in arcsec for each filter as documented here: 
# https://jwst-docs.stsci.edu/jwst-near-infrared-camera/nircam-performance/nircam-point-spread-functions#gsc.tab=0
_empirical_psf_fwhm = {'f200w': 0.066, 'f356w': 0.116}

_utf8_filter_type = h5py.string_dtype("utf-8", 5)


def collect_all_fits(fits_path):
    """
    Collects FITS files from subfolders structured by filter and redshift.
    Groups files by redshift and object ID, including both SCI and WHT_FULL.

    Parameters
    ----------
    fits_path : str
        Path to the main directory containing subfolders per filter and redshift.

    Returns
    -------
    dict
        Nested dictionary structured as:
        { "object_id_z6": { "f200w": {"sci": HDUList, "wht_full": HDUList}, ... }, ... }
    """
    data = defaultdict(dict)

    for subfolder in os.listdir(fits_path):
        subfolder_path = os.path.join(fits_path, subfolder)
        if not os.path.isdir(subfolder_path):
            continue

        # Detect redshift and filter from folder name
        redshift = None
        filter_name = None

        for token in subfolder.lower().split('_'):
            if token.startswith('z') and token[1:].isdigit():
                redshift = token
            elif token.lower() in filters:
                filter_name = token.lower()

        if not redshift or not filter_name:
            continue

        for fname in os.listdir(subfolder_path):
            if not fname.endswith(".fits"):
                continue

            full_path = os.path.join(subfolder_path, fname)

            if fname.endswith("_wht_full.fits"):
                object_id = fname[:-14]  # remove _wht_full.fits
                kind = "wht_full"
            else:
                object_id = fname[:-5]   # remove .fits
                kind = "sci"

            key = f"{object_id}_{redshift}"
            
            with fits.open(full_path) as hdul:
                name = 'sci' if kind == 'sci' else 'wht'
                data_array = hdul[name].data.copy()
                header = hdul[name].header.copy()
                
            data[key].setdefault(filter_name, {})[kind] = {
                'data':data_array, 
                'header': header,
            }

    return data


def process_images(fits_dir, output_dir):
    """ 
    Function that will process all the mock images.
    
    Parameters
    ----------
    fits_dir: str
        Local directory where the images FITS files are stored.
    output_dir: str
        Output directory where the HDF5 files will be stored.
    """
    images_dict = collect_all_fits(fits_dir)
    
    out_images = []
    for image_id in images_dict.keys():        

        ra = images_dict[image_id][filters[0]]['sci']['header']['RA_V1'] 
        dec = images_dict[image_id][filters[0]]['sci']['header']['DEC_V1']

        images = []
        invvar = []
        for filter in filters:
            wcs = WCS(images_dict[image_id][filter]['sci']['header'])
            pix_scale = np.sqrt(np.linalg.det(abs(wcs.pixel_scale_matrix))) * 3600
            pix_scale = round(pix_scale, 4)
            images_dict[image_id][filter]['pix_scale'] = pix_scale
            images_dict[image_id][filter]['wcs'] = wcs

            # x, y = wcs.all_world2pix(ra, dec, 0)
            # position = (x, y)
            # size = (_cutout_size, _cutout_size)
            # images.append(Cutout2D(img[filter]['sci'].data, position, size, wcs=wcs, 
            #                       mode='partial',fill_value=0).data)
            # invvar.append(Cutout2D(img[filter]['wht_full'].data, position, size, wcs=wcs,
            #                       mode='partial',fill_value=0).data)

            sci_image = images_dict[image_id][filter]['sci']['data']
            wht_image = images_dict[image_id][filter]['wht_full']['data']

            images.append(cv2.resize(sci_image, (96, 96)))
            invvar.append(cv2.resize(wht_image, (96, 96)))

        images = np.stack(images, axis=0)
        invvar = np.stack(invvar, axis=0)

        # Convert all nans to zeros in images and invvar with np.nan_to_num
        images = np.nan_to_num(images)
        invvar = np.nan_to_num(invvar)

        # Computing a mask
        mask = invvar > 0

        healpix = hp.ang2pix(_healpix_nside, ra, dec, lonlat=True, nest=True)

        obj_data = {
                "healpix": healpix,
                "object_id": np.string_(image_id),
                "image_band": np.array([f.lower().encode("utf-8") for f in filters],
                                       dtype=_utf8_filter_type),
                "image_ivar": invvar,
                "image_flux": images,
                "image_mask": mask.astype("bool"),
                "image_psf_fwhm": np.array([_empirical_psf_fwhm[f] for f in filters]).astype(np.float32),
                "image_scale": np.array([images_dict[image_id][f]['pix_scale'] for f in filters]).astype(np.float32),
            }
        out_images.append(obj_data)
        
    # Aggregate all images into an astropy table
    catalog = Table({k: [d[k] for d in out_images] for k in out_images[0].keys()})

    # Group the catalog by healpix index
    catalog.group_by("healpix")
    
    for group in catalog.groups:
        group_filename = f"{output_dir}/ceers/healpix={group['healpix'][0]}/001-of-001.hdf5"

        # Create the output directory if it does not exist
        out_path = os.path.dirname(group_filename)
        if not os.path.exists(out_path):
            os.makedirs(out_path, exist_ok=True)

        with FileLock(group_filename + ".lock"):
            if os.path.exists(group_filename):
                # Load the existing file and concatenate the data with current data
                with h5py.File(group_filename, 'a') as hdf5_file:
                    for key in catalog.colnames:
                        # If this key does not already exist, we skip it
                        if key not in hdf5_file:
                            continue
                        shape = catalog[key].shape
                        hdf5_file[key].resize(hdf5_file[key].shape[0] + shape[0], axis=0)
                        hdf5_file[key][-shape[0]:] = catalog[key]
            else:           
                # This is the first time we write the file, so we define the datasets
                with h5py.File(group_filename, 'w') as hdf5_file:
                    for key in catalog.colnames:
                        shape = catalog[key].shape
                        if len(shape) == 1:
                            hdf5_file.create_dataset(key, data=catalog[key], compression="gzip",
                                                     chunks=True, maxshape=(None,))
                        else:
                            hdf5_file.create_dataset(key, data=catalog[key], compression="gzip", 
                                                     chunks=True, maxshape=(None, *shape[1:]))
    
    del images_dict, catalog, out_images


def build_total_inverse_variance(fits_fir):
    """
    Builds the total inverse variance for all FITS files inside `fits_fir` (including subdirectories),
    by adding VAR_POISSON to the variance from WHT, and saves the result as a new FITS file
    in the same directory as the input file.

    Parameters
    ----------
    fits_dir : str
        Path to the root directory containing FITS files (recursively).
    """
    for root, dirs, files in os.walk(fits_fir):
        for filename in files:
            if filename.endswith(".fits") and not filename.endswith("_wht_full.fits"):
                fits_file = os.path.join(root, filename)

                with fits.open(fits_file) as hdul:
                    wht_data = hdul['WHT'].data
                    var_poisson = hdul['VAR_POISSON'].data
                    header = hdul['WHT'].header

                with np.errstate(divide='ignore', invalid='ignore'):
                    var_from_wht = np.where(wht_data > 0, 1.0 / wht_data, 0.0)

                var_total = var_from_wht + var_poisson

                with np.errstate(divide='ignore', invalid='ignore'):
                    wht_full = np.where(var_total > 0, 1.0 / var_total, 0.0)

                # Save in the same directory as the input FITS file
                output_path = os.path.join(root, f"{filename[:-5]}_wht_full.fits")

                fits.PrimaryHDU(data=wht_full, header=header).writeto(output_path, overwrite=True)
        

def download_files(local_dir, max_workers=10, tiny=False):
    """
    Downloads all files from the TNG website to a local directory using multiple threads.

    Parameters
    ----------
    local_dir : str
        Local directory to save files.
    max_workers : int
        Number of threads for parallel downloads
    tiny : bool
        If True, only download a small subset of data
    
    Returns
    -------
    List
        List of downloaded file paths
    """
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Building the list of URLs to download
    files = []

    # Normal mode: download all files
    for filter in filters:
        if tiny:
            # For tiny mode, just download one specific file 
            files.append(f"https://www.tng-project.org/api/TNG50-1/files/skirt_images_jwst_{filter}_z6.tar.gz")
        else:
            for z in [3, 4, 5, 6]:
                files.append(f"https://www.tng-project.org/api/TNG50-1/files/skirt_images_jwst_{filter}_z{z}.tar.gz")

    print(f"Found {len(files)} tarball files to download.")

    def download_file(url, local_dir, headers=None):
        """ Helper function to download a single file with optional headers """
        if headers is None:
            headers = {"API-Key": "dddbcd3f72e1161f97818eb62ce1d4e2"}

        local_file_path = os.path.join(local_dir, os.path.basename(url))
        start_time = time.time()
        response = requests.get(url, stream=True, headers=headers, timeout=30)
        # print(f"\nDownloaded {url} in {time.time() - start_time:.2f} seconds")

        response.raise_for_status()  # Raise an error if the request failed
        total_size = int(response.headers.get('content-length', 0))

        # Skip download if the file already exists and is complete
        if os.path.exists(local_file_path) and os.path.getsize(local_file_path) == total_size:
            return local_file_path

        with open(local_file_path, 'wb') as file:
            for data in response.iter_content(chunk_size=10 * 1024):
                if data:
                    file.write(data)

        return local_file_path

    # Use ThreadPoolExecutor for parallel downloads with tqdm progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_file, url, local_dir) for url in files]
        for future in tqdm(futures, total=len(futures), desc="Downloading files"):
            try:
                file_path = future.result()
            except Exception as e:
                print(f"Error downloading file: {e}")       


def extract_tarballs(source_dir, output_dir):
    """
    Extracts all .tar.gz or .tgz archives from a source directory into an output directory,
    preserving the original internal folder structure of each archive.

    Parameters
    ----------
    source_dir : str
        Path to the directory containing the .tar.gz or .tgz files.
    output_dir : str
        Path to the directory where the files will be extracted. Will be created if it doesn't exist.
    """
    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(source_dir):
        if filename.endswith(".tar.gz") or filename.endswith(".tgz"):
            tar_path = os.path.join(source_dir, filename)

            with tarfile.open(tar_path, 'r:gz') as tar:
                tar.extractall(path=output_dir)  # preserve internal folder structure

        
def main(args):
    # Create directories
    if not os.path.exists(args.download_dir):
        os.makedirs(args.download_dir)
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    if not os.path.exists(args.fits_dir):
        os.makedirs(args.fits_dir)

    # (1) Download all files
    print("Downloading all files...")   
    download_files(args.download_dir, max_workers=args.max_workers, tiny=args.tiny)
    print("All files downloaded.")
    
    # (2) Extract FITS files 
    print("Extracting FITS files...")   
    extract_tarballs(args.download_dir, args.fits_dir)
    print("All FITS extracted.")
    
    # (3) Create inverse variance images
    print("Preparing inverse variance images...")
    build_total_inverse_variance(args.fits_dir)
    print("All inverse variance maps generated.")
    
    # (4) Process images
    process_images(args.fits_dir, args.output_dir)

    print("All done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process TNG JWST NIRCAM FITS files")
    parser.add_argument("--output_dir", type=str, default="./output",
                        help="Path to the output directory")
    parser.add_argument("--download_dir", type=str, default="./download",
                        help="Path to the download directory for tarball files")
    parser.add_argument("--fits_dir", type=str, default="./fits_files",
                        help="Path for storing all the FITS files")
    parser.add_argument("--max_workers", type=int, default=1, 
                        help="Number of threads for parallel processing")
    parser.add_argument("--redshift", type=str, choices=["z3", "z4", "z5", "z6", "all"], default="all", 
                        help="Specific redshift to process")
    parser.add_argument("--tiny", action="store_true", default=False,
                        help="If set, only process a small subset of the data (z=6)")
    
    args = parser.parse_args()
    main(args)
