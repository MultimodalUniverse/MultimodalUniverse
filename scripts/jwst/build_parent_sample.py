import argparse
import os
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


import h5py
import healpy as hp
import numpy as np
import requests

from filelock import FileLock
from astropy.io import fits
from astropy.nddata.utils import Cutout2D
from astropy.table import Table, join
from astropy.wcs import WCS
from scipy.ndimage import maximum_filter

_healpix_nside = 16
_cutout_size = 96
_mosaics = [
    # Primer fields
    "primer-cosmos-east-grizli-v7.0", "primer-cosmos-west-grizli-v7.0",
    "primer-uds-north-grizli-v7.2", "primer-uds-south-grizli-v7.2",
    # CEERS fields
    "ceers-full-grizli-v7.4",
    # NGDEEP fields
    "ngdeep-grizli-v7.2",
    # GOODS fields
    "gds-grizli-v7.2", "gdn-grizli-v7.3",
]
# Filters used in the mosaics
_filters = ['f090w', 'f115w', 'f150w', 'f200w',
            'f277w', 'f356w', 'f444w']

# Magnitude cut in the F140W reference filter
_f140w_mag_cut = 25.5

# PSF FWHM in arcsec for each filter as documented here: 
# https://jwst-docs.stsci.edu/jwst-near-infrared-camera/nircam-performance/nircam-point-spread-functions#gsc.tab=0
_empirical_psf_fwhm = {
    'f090w': 0.033, 
    'f115w': 0.040, 
    'f150w': 0.050, 
    'f200w': 0.066,
    'f277w': 0.092,
    'f356w': 0.116,
    'f444w': 0.145,
}

_s3_prefix = "JwstMosaics/v7"
_s3_bucket = "grizli-v2"

_utf8_filter_type = h5py.string_dtype("utf-8", 5)

def selection_function(cat, mag_cut):
    """ Applies full color cut and magnitude cut to catalog
    
    :param cat: astropy.table.Table
        Catalog of objects
    :param mag_cut: float
        mag_auto cut in the F140W reference filter
    """
    # Full color cut, we need to have detections in all filters
    # NOTE: some fields may not have all filters, so we only keep the ones that are present
    # in the catalog
    filters = [f for f in _filters if f'{f}_flux_aper_0' in cat.colnames]
    sel = np.all([cat[f'{filter}_flux_aper_0'] > 0 for filter in filters], axis=0)

    # Magnitude cut
    sel = sel & (cat['mag_auto'] < mag_cut)
    return sel

def process_mosaic(mosaic, local_dir, output_dir):
    """ Function that will process a single mosaic and return a catalog with 
        cutouts for all objects in the mosaic.
    
    :param mosaic: str
        Name of the mosaic
    :param local_dir: str
        Local directory where the mosaic files are stored
    :param output_dir: str
        Output directory where the HDF5 files will be stored
    """
    # Opening catalog file
    catalog = Table.read(f"{local_dir}/{mosaic}-fix_phot_apcorr.fits")
    # Adding healpix index
    catalog["healpix"] = hp.ang2pix(
        _healpix_nside, catalog["ra"], catalog["dec"], lonlat=True, nest=True
    )
    catalog["object_id"] = catalog["id"] + hash(mosaic)
    # Apply selection function
    sel = selection_function(catalog, mag_cut=_f140w_mag_cut)
    catalog = catalog[sel]
    print('Keeping', len(catalog), 'objects in the catalog for mosaic', mosaic)

    # For ngdeep, we only use 6 filters instead of 7
    if 'ngdeep' in mosaic:
        filters = _filters[1:]
    else:
        filters = _filters

    # Opening all filters files
    img = {
        filter: {
            ext: fits.open(f"{local_dir}/{mosaic}-{filter}-clear_drc_{ext}.fits.gz")[0]
            for ext in ["sci", "wht_full"]
        } for filter in filters
    }

    # Computing pixel scale for all bands from the header
    for filter in filters:
        wcs = WCS(img[filter]['sci'].header)
        pix_scale = np.sqrt(np.linalg.det(abs(wcs.pixel_scale_matrix))) * 3600
        pix_scale = round(pix_scale, 4)
        img[filter]['pix_scale'] = pix_scale
        img[filter]['wcs'] = wcs
    
    # Getting cutouts for all objects in the catalog
    out_images = []
    for row in catalog:
        ra, dec = row["ra"], row["dec"]

        images = []
        invvar = []
        for filter in filters:
            wcs = img[filter]['wcs']
            x, y = wcs.all_world2pix(ra, dec, 0)
            position = (x, y)
            size = (_cutout_size, _cutout_size)
            images.append(Cutout2D(img[filter]['sci'].data, position, size, wcs=wcs, mode='partial',fill_value=0).data)
            invvar.append(Cutout2D(img[filter]['wht_full'].data, position, size, wcs=wcs, mode='partial',fill_value=0).data)
        images = np.stack(images, axis=0)
        invvar = np.stack(invvar, axis=0)

        # Computing a mask
        mask = invvar > 0
        
        obj_data = {
                "object_id": row["object_id"],
                "image_band": np.array(
                    [f.lower().encode("utf-8") for f in filters],
                    dtype=_utf8_filter_type,
                ),
                "image_ivar": invvar,
                "image_array": images,
                "image_mask": mask.astype("bool"),
                "image_psf_fwhm": np.array([_empirical_psf_fwhm[f] for f in filters]).astype(np.float32),
                "image_scale": np.array([img[f]['pix_scale'] for f in filters]).astype(np.float32),
            }
        out_images.append(obj_data)

    # Aggregate all images into an astropy table
    out_images = Table({k: [d[k] for d in out_images] for k in out_images[0].keys()})

    # Join on object_id with the input catalog
    catalog = join(catalog, out_images, 'object_id', join_type='inner')

    # Group the catalog by healpix index
    catalog.group_by("healpix")

    for group in catalog.groups:
        if 'primer-cosmos' in mosaic:
            survey = 'primer-cosmos'
        elif 'primer-uds' in mosaic:
            survey = 'primer-uds'
        else:
            survey = mosaic.split('-')[0]
        group_filename = f"{output_dir}/{survey}/healpix={group['healpix'][0]}/001-of-001.hdf5"

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
                            hdf5_file.create_dataset(key, data=catalog[key], compression="gzip", chunks=True, maxshape=(None,))
                        else:
                            hdf5_file.create_dataset(key, data=catalog[key], compression="gzip", chunks=True, maxshape=(None, *shape[1:]))
    
    del img, catalog

def build_total_inverse_variance(mosaic, filter, local_dir):
    """
    Builds the total inverse variance image for a mosaic by combining the original weight image 
    with the Poisson variance from the science image.

    This is based on https://dawn-cph.github.io/dja/blog/2023/07/18/image-data-products/
    
    :param mosaic: str
        Name of the mosaic
    :param filter: str
        Name of the filter
    :param local_dir: str
        Local directory where the mosaic files are stored
    """
    # Opening necessary files
    img = {
        filter: {
            ext: fits.open(f"{local_dir}/{mosaic}-{filter}-clear_drc_{ext}.fits.gz")
            for ext in ["sci", "wht", "exp"]
        }
    }
    # First rescale the exp image to match the science image
    full_exp = np.zeros(img[filter]['sci'][0].data.shape, dtype=int)
    full_exp[2::4,2::4] += img[filter]["exp"][0].data*1
    full_exp = maximum_filter(full_exp, 4)

    header = img[filter]['exp'][0].header

    # Multiplicative factors that have been applied since the original count-rate images
    phot_scale = 1.

    for k in ['PHOTMJSR','PHOTSCAL']:
        phot_scale /= header[k]

    # Unit and pixel area scale factors
    if 'OPHOTFNU' in header:
        phot_scale *= header['PHOTFNU'] / header['OPHOTFNU']

    # "effective_gain" = electrons per DN of the mosaic
    effective_gain = (phot_scale * full_exp)

    # Poisson variance in mosaic DN
    var_poisson_dn = np.maximum(img[filter]['sci'][0].data, 0) / effective_gain

    # Original variance from the `wht` image = RNOISE + BACKGROUND
    var_wht = 1/img[filter]['wht'][0].data

    # New total variance
    var_total = var_wht + var_poisson_dn
    full_wht = 1 / var_total

    # Null weights
    full_wht[var_total <= 0] = 0

    output = fits.HDUList([fits.PrimaryHDU(data=full_wht, header=img[filter]['wht'][0].header)])

    # Save the new weight image
    output.writeto(f"{local_dir}/{mosaic}-{filter}-clear_drc_wht_full.fits.gz", overwrite=True)

    # Close all files and delete the img dictionary
    for ext in ["sci", "wht", "exp"]:
        img[filter][ext].close()


def bulk_download(local_dir, max_workers=10):
    """
    Downloads all files from a public S3 bucket's folder (prefix) to a local directory using multiple threads.

    :param local_dir: Local directory to save files
    :param max_workers: Number of threads for parallel downloads
    """
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Building the list of URLs to download
    files = []
    for mosaic in _mosaics:
        # Adding catalog file
        files.append(f"https://{_s3_bucket}.s3.amazonaws.com/{_s3_prefix}/{mosaic}-fix_phot_apcorr.fits")
        for filt in _filters:
            files.append(f"https://{_s3_bucket}.s3.amazonaws.com/{_s3_prefix}/{mosaic}-{filt}-clear_drc_sci.fits.gz")
            files.append(f"https://{_s3_bucket}.s3.amazonaws.com/{_s3_prefix}/{mosaic}-{filt}-clear_drc_wht.fits.gz")
            files.append(f"https://{_s3_bucket}.s3.amazonaws.com/{_s3_prefix}/{mosaic}-{filt}-clear_drc_exp.fits.gz")

    print(f"Found {len(files)} files to download.")

    def _download_file(url, local_dir):
        """ Helper function to download a single file """
        local_file_path = os.path.join(local_dir, os.path.basename(url))
        response = requests.get(url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        if os.path.exists(local_file_path) and os.path.getsize(local_file_path) == total_size:
            return
        with open(local_file_path, 'wb') as file:
            for data in response.iter_content(chunk_size=10 * 1024):
                file.write(data)

    # Use ThreadPoolExecutor for parallel downloads with tqdm progress bar
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(executor.map(lambda url: _download_file(url, local_dir), files), total=len(files), desc="Downloading files"))


def main(args):

    # Create the local directory if it does not exist
    local_dir = os.path.join(args.local_dir)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # If we are only testing, we only process ngdeep
    if args.tiny:
        _mosaics.clear()
        _mosaics.append("ngdeep-grizli-v7.2")

    # Download all files
    print("Downloading all files...")
    bulk_download(local_dir, max_workers=args.max_workers)
    print("All files downloaded.")

    print("Preparing inverse variance images...")
    maps_to_generate = []
    for mosaic in _mosaics:
        for filter in _filters:
            if os.path.exists(f"{local_dir}/{mosaic}-{filter}-clear_drc_wht_full.fits.gz"):
                continue
            maps_to_generate.append((mosaic, filter))

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        list(tqdm(executor.map(lambda x: build_total_inverse_variance(*x, local_dir), maps_to_generate), total=len(maps_to_generate), desc="Building inverse variance maps"))
    print("All inverse variance maps generated.")

    # Building catalog for all mosaics
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        list(tqdm(executor.map(lambda mosaic: process_mosaic(mosaic, local_dir, args.output_dir), _mosaics), total=len(_mosaics), desc="Processing mosaics"))

    print("All done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Downloads JWST data from DJA from specified survey"
    )

    parser.add_argument(
        "--output_dir", type=str, help="The path to the output directory", default="."
    )
    parser.add_argument(
        "--local_dir",
        type=str,
        help="The path to the temporary download directory",
        default="./jwst_data",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=10,
        help="Number of threads for parallel downloads and processing",
    )
    parser.add_argument(
        "--tiny",
        action="store_true",
        help="If set, only process a small subset of the data (ngdeep)",
        default=False,
    )
    args = parser.parse_args()
    main(args)
