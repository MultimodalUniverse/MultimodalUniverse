import os
import argparse
import requests
import astropy.units as u
import healpy as hp
import numpy as np
import h5py

from tqdm import tqdm
from astropy.table import Table, join
from astropy.wcs import WCS
from astropy.nddata import Cutout2D
from astropy.io import fits
from filelock import FileLock
from collections import defaultdict
from skimage.transform import resize 
from concurrent.futures import ThreadPoolExecutor

_filters = ['HSC-G', 'HSC-R', 'HSC-I', 'HSC-Z', 'HSC-Y']
_utf8_filter_type = h5py.string_dtype('utf-8', 5)
_image_size = 160
_pixel_scale = 0.167 # Size of a pixel in arcseconds
_healpix_nside = 16

_psf_fwhm = {
    'HSC-G': 0.8750302195549011, 
    'HSC-R': 0.9132629632949829, 
    'HSC-I': 0.6462167501449585,
    'HSC-Z': 0.769660234451294, 
    'HSC-Y': 0.6392034888267517
}


def build_image_dict(download_dir):
    """
    Parses FITS files from a directory and organizes their contents into a nested dictionary by object ID and filter.

    Parameters
    ----------
    download_dir : str
        Path to the directory containing the downloaded FITS files.

    Returns
    -------
    dict
        A nested dictionary structured as:
        {
            "object_id": {
                "HSC-G": {
                    "image": HDUList,
                    "mask": HDUList,
                    "var": HDUList,
                    "psf": HDUList
                },
                ...
            },
            ...
        }

    Notes
    -----
    - The `object_id` is extracted from characters 27 to -5 of the FITS filename.
    - Assumes FITS files follow the naming convention used in the TNG SKIRT-HSC mock outputs.
    - The filters are defined in a global `_filters` variable (e.g., `["HSC-G", "HSC-R", "HSC-I"]`).
    - Each FITS file is expected to contain HDUs named:
      - `"SUBARU_HSC.X"` for the image
      - `"SUBARU_HSC.X MASK"` for the mask
      - `"SUBARU_HSC.X VARIANCE"` for the variance
      - `"SUBARU_HSC.X PSF"` for the PSF
      where `X` is the last character of the filter name.
    """
    data = defaultdict(dict)

    for fname in os.listdir(download_dir):
        if not fname.endswith(".fits"):
            continue
        
        image_id = fname[27:-5]
        
        full_path = os.path.join(download_dir, fname)
        with fits.open(full_path) as hdul:
            fdict = defaultdict(dict)
            for filter in _filters: 
                fdict[filter] = {
                    'image': hdul[f"SUBARU_HSC.{filter[-1]}"].copy(),
                    'mask': hdul[f"SUBARU_HSC.{filter[-1]} MASK"].copy(),
                    'var': hdul[f"SUBARU_HSC.{filter[-1]} VARIANCE"].copy(),
                    'psf': hdul[f"SUBARU_HSC.{filter[-1]} PSF"].copy()
                }
        
        data[image_id] = fdict

    return data


def _process_single_image(image_id, images_dict):
    """
    Processes a single mock galaxy image across multiple filters.

    This function handles cleaning, resizing, and metadata extraction for
    one galaxy's multi-band image. It applies a bitmask to filter out
    unwanted pixels, rescales all image-related arrays to a standard shape,
    computes the inverse variance, updates pixel scale metadata, and prepares
    a dictionary compatible with downstream HDF5 storage.

    Parameters
    ----------
    image_id : str
        Identifier of the object/image to be processed. This key should exist
        in the `images_dict`.

    images_dict : dict
        Nested dictionary containing image data loaded from FITS files. 

    Returns
    -------
    dict
        Dictionary containing all data required to save this image into an HDF5 catalog.
        The structure includes:
            - healpix : int
            - object_id : bytes
            - image_band : np.ndarray
            - image_ivar : np.ndarray
            - image_flux : np.ndarray
            - image_mask : np.ndarray (boolean)
            - image_psf_fwhm : np.ndarray
            - image_scale : np.ndarray

        All image-related arrays are resized to (_image_size, _image_size) and
        stacked across filter channels.
    """
    ra = images_dict[image_id][_filters[0]]['image'].header['RA'] 
    dec = images_dict[image_id][_filters[0]]['image'].header['DEC']

    images, invvar, mask = [], [], []
    for filter in _filters: 
        img = images_dict[image_id][filter]['image'].data
        var = images_dict[image_id][filter]['var'].data
        msk = images_dict[image_id][filter]['mask'].data

        maskclean = np.ones_like(msk, dtype=bool)
        set_maskbits = [0, 1, 8]
        for bit in set_maskbits:
            maskclean &= (msk & 2**bit) == 0
        bin_msk = maskclean.astype(np.uint8)

        old_image_size = img.shape[0] 
        new_pix_scale = (old_image_size * _pixel_scale) / _image_size
        images_dict[image_id][filter]['pix_scale'] = new_pix_scale

        shape = (_image_size, _image_size)
        resized_img = resize(img, shape, order=0, anti_aliasing=False, preserve_range=True)
        resized_var = resize(var, shape, order=0, anti_aliasing=False, preserve_range=True)
        resized_msk = resize(bin_msk, shape, order=0, anti_aliasing=False, preserve_range=True)

        images.append(resized_img)
        invvar.append(1/resized_var)
        mask.append(resized_msk)

    images = np.nan_to_num(np.stack(images, axis=0))
    invvar = np.nan_to_num(np.stack(invvar, axis=0))
    mask = np.stack(mask, axis=0)

    healpix = hp.ang2pix(_healpix_nside, ra, dec, lonlat=True, nest=True)

    return {
        "healpix": healpix,
        "object_id": np.string_(image_id),
        "image_band": np.array([f.lower().encode("utf-8") for f in _filters], dtype=_utf8_filter_type),
        "image_ivar": invvar,
        "image_flux": images,
        "image_mask": mask.astype(bool),
        "image_psf_fwhm": np.array([_psf_fwhm[f] for f in _filters]).astype(np.float32),
        "image_scale": np.array([images_dict[image_id][f]['pix_scale'] for f in _filters]).astype(np.float32),
    }


def process_images(download_dir, output_dir, max_workers):
    """
    Process mock SKIRT HSC images by cleaning, resizing, and packaging them into HDF5 files grouped by HEALPix index.

    Parameters
    ----------
    download_dir : str
        Path to the directory containing the raw FITS files.
    output_dir : str
        Path where the processed HDF5 files will be saved.
    max_workers : int
        Number of threads to use for parallel processing.

    Description
    -----------
    This function loads FITS data using `build_image_dict()`, extracts science image, variance, and mask HDUs
    for each filter, applies a bitmask to clean flagged pixels, resizes arrays to a uniform shape, and computes
    inverse variance. The pixel scale is adjusted accordingly for resized images.

    Processed image data is aggregated into per-object dictionaries and grouped by HEALPix index, then saved
    to disk in HDF5 format. NaN values in flux and inverse variance arrays are replaced with zeros.

    Notes
    -----
    - Relies on global constants:
        * `_filters`: list of filter names (e.g., `["HSC-G", "HSC-R", "HSC-I"]`)
        * `_pixel_scale`: original pixel scale (arcsec/pixel)
        * `_image_size`: target square image dimension (in pixels)
        * `_psf_fwhm`: dictionary mapping filters to their PSF FWHM values
        * `_healpix_nside`: HEALPix NSIDE resolution
        * `_utf8_filter_type`: numpy dtype used to encode filter names
    - Resizing is performed using `skimage.transform.resize` with:
        * `order=0` (nearest-neighbor)
        * `anti_aliasing=False`
        * `preserve_range=True`
    - Mask is cleaned using bit flags `[0, 1, 8]` (bad pixels, saturated, no data).
    - Output structure:
        `output_dir/hsc/healpix=<index>/001-of-001.hdf5`
    - HDF5 datasets are gzip-compressed and chunked to optimize I/O.

    Raises
    ------
    KeyError
        If expected HDUs (e.g., image, mask, variance, PSF) are missing in a FITS file.
    OSError
        If directories or files cannot be created, read, or written.
    """

    images_dict = build_image_dict(download_dir)
    image_ids = list(images_dict.keys())

    out_images = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = executor.map(lambda image_id: _process_single_image(image_id, images_dict),
                               image_ids)
        for result in tqdm(futures, total=len(image_ids), desc="Processing images"):
            out_images.append(result)
        
    # Aggregate all images into an astropy table
    catalog = Table({k: [d[k] for d in out_images] for k in out_images[0].keys()})

    # Group the catalog by healpix index
    catalog.group_by("healpix")
    
    for group in catalog.groups:
        group_filename = f"{output_dir}/hsc/healpix={group['healpix'][0]}/001-of-001.hdf5"

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


def _download_single_file(file_url, download_dir, headers):
    """
    Downloads a single FITS file from the provided URL and saves it with a descriptive filename.

    Parameters
    ----------
    file_url : str
        The full URL of the FITS file to download.
    download_dir : str
        The local directory where the downloaded file should be saved.
    headers : dict
        Dictionary of HTTP headers to include in the request (e.g., for API authentication).

    Description
    -----------
    The function extracts the snapshot and subhalo ID from the URL path and embeds them into the output filename 
    for clarity and traceability. The file is streamed and written in chunks to handle large files efficiently.

    Notes
    -----
    - Output filenames are of the format: 
      `<original_filename>_snap<snapshot>_subhalo<subhalo>.fits`
    - Uses `requests` for downloading with stream mode enabled.
    - Assumes that the FITS file ends with `.fits` and is named accordingly in the URL.

    Raises
    ------
    requests.HTTPError
        If the download request fails or the server responds with an error.
    IOError
        If writing the file to disk fails.
    """
    filename = os.path.basename(file_url)
    url_parts = file_url.split('/')
    snapshot = url_parts[-5]
    subhalo = url_parts[-3]

    out_path = os.path.join(
        download_dir, 
        filename[:-5] + f'_snap{snapshot}_subhalo{subhalo}.fits'
    )

    with requests.get(file_url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

                
def download_from_json(json_url, download_dir, tiny, max_workers=4):
    """
    Downloads FITS files listed in a JSON file from the TNG API and saves them with descriptive filenames.
    
    Parameters
    ----------
    json_url : str
        URL pointing to a JSON file containing a list of FITS file URLs.
    download_dir : str
        Path where downloaded FITS files will be saved.
    tiny : bool
        If True, downloads only the first 10 files for testing purposes.
    max_workers : int
        Number of threads to use for parallel downloading.

    Notes
    -----
    Uses a thread pool to download files concurrently.
    """
    headers = {"API-Key": "dddbcd3f72e1161f97818eb62ce1d4e2"}

    response = requests.get(json_url, headers=headers)
    response.raise_for_status()
    data = response.json()
    files = data["files"][:10] if tiny else data["files"]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(
            executor.map(lambda url: _download_single_file(url, download_dir, headers), files),
            total=len(files),
            desc="Downloading FITS files"
        ))
             
                    
def download_data(download_dir, tiny, max_workers):
    """
    Iterates over multiple simulations, snapshots, and camera views to download SKIRT HSC-realistic FITS images.

    Parameters
    ----------
    download_dir : str
        Path to the directory where all downloaded files will be stored.
    tiny : bool
        If True, only a small subset (up to 10 files per camera view per snapshot) will be downloaded.

    Notes
    -----
    This function downloads data for:
    - Simulations: 'TNG50-1' and 'TNG100-1'
    - Snapshots: 72 to 91 (inclusive)
    - Camera views: 0 to 3

    It constructs a URL for each combination and passes it to `download_from_json`.
    """
    for sim in ['TNG50-1', 'TNG100-1']:
        for snap in range(72, 92): 
            for cam in range(4):
                json_url = f"http://www.tng-project.org/api/{sim}/files/skirt_images_hsc_realistic_v{cam}_{snap}/"
                download_from_json(json_url, download_dir, tiny, max_workers)
                if tiny and sim == 'TNG50-1' and snap == 72 and cam == 0:
                    return # Stop after the first 10 files 
    
    
def main(args):
    # Create directories
    os.makedirs(args.download_dir, exist_ok=True)
    os.makedirs(args.output_dir, exist_ok=True)
   
    # (1) Download images from TNG's website
    print('Downloading all files...')
    download_data(args.download_dir, args.tiny, args.max_workers)
    print('All images downloaded')
    
    # (2) Process images
    print('Processing images...')
    process_images(args.download_dir, args.output_dir, args.max_workers)
    
    print('All done!')
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process TNG HSC-SSP mock images")   
    parser.add_argument("--output_dir", type=str, default="./output",
                        help="Directory where the processed HDF5 files will be stored")
    parser.add_argument("--download_dir", type=str, default="./download",
                        help="Directory to store/download the raw FITS files from the TNG API")
    parser.add_argument("--tiny", action="store_true", default=False,
                        help="Download only a small subset of the data (10 files max) for testing")
    parser.add_argument("--max_workers", type=int, default=1, 
                        help="Number of threads for parallel processing")
    
    args = parser.parse_args()
    main(args)
    
