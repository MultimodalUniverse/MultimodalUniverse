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
import healpy as hp
from scipy.optimize import curve_fit
from pathlib import Path
import requests
import tarfile
import requests
import pandas as pd
from typing import List
from waiting import wait
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from functools import partial

FILTERS = ["B", "G", "R", "I"]
FILTER_MAP = {
    'B': '1',
    'G': '2',
    'R': '3',
    'I': '4'
}

def members(tar, strip):
    members = []
    object_ids = [f.path.split('/')[-1].split('.')[-2][:-1] for f in tar.getmembers() if f.path.endswith('.fits')][:2]
    for member in tar.getmembers():
        if member.path.endswith('.fits'):
            member.path = member.path.split('/', strip)[-1]
            if member.path.split('.')[0][:-1] in object_ids:
                members.append(member)
    return members

def get_object_ids(n_objects: int) -> pd.DataFrame:
    sql_query = """SELECT TOP {n_objects:d} * FROM galah_dr3.main_star WHERE sobject_id < 160000000000000 and logg < 2.0"""

    api_url = 'https://datacentral.org.au/api/services/query/'
    qdata = {
        'title' : 'galah_test_query',
        'sql' : sql_query.format(n_objects=n_objects),
        'run_async' : False
        }
    post = requests.post(api_url, data=qdata).json()
    resp = requests.get(post['url']).json()
    return pd.DataFrame(resp['result']['data'], columns=resp['result']['columns'])


def get_objects_table(source_ids: List[int] = None) -> pd.DataFrame:
    if source_ids:
        if not type(source_ids) == list and not all([type(si) == int for si in source_ids]):
            raise ValueError("source_ids must either be None or a list of integers")
        source_ids_str = "(" + ','.join([str(si) for si in source_ids]) + ")"
        sql_query = "SELECT * FROM galah_dr3.main_star WHERE sobject_id IN " + source_ids_str
    else:
        sql_query = "SELECT * FROM galah_dr3.main_star"
    api_url = 'https://datacentral.org.au/api/services/query/'
    qdata = {
        'title' : 'galah_test_query',
        'sql' : sql_query,
        'run_async' : True
        }
    post = requests.post(api_url,data=qdata).json()

    print("Downloading the GALAH DR3 objects table.")
    wait(lambda: requests.get(post['url']).json()['status']=='complete', timeout_seconds=600)
    resp = requests.get(post['url']).json()
    return pd.DataFrame(resp['result']['data'],columns=resp['result']['columns'])


def download_spectrum_file(object_id: int, filter_symbol: str) -> bytes:
    if filter_symbol not in FILTERS:
        raise Exception("Filter must be one of" + ",".join(FILTERS))
    
    url_template = "https://datacentral.org.au/vo/slink/links?ID={sobject_id:d}&DR=galah_dr3&IDX=0&FILT={filter}&RESPONSEFORMAT=fits"
    r = requests.get(url_template.format(sobject_id=object_id, filter=filter_symbol))
    return r.content


def download_resolution_maps() -> List[bytes]:
    url_template = "https://github.com/svenbuder/GALAH_DR3/raw/master/analysis/resolution_maps/ccd{ind:d}_piv.fits"
    return [requests.get(url_template.format(ind=i)).content for i in range(1, 5)]


_PARAMETERS = [
    'ra',
    'dec',
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
]

# These are going to be a 1d array in the example - one per example.
_FLOAT_FEATURES = [
    'object_id',
    'timestamp',
    'spectrum_B_ind_start',
    'spectrum_B_ind_end',
    'spectrum_G_ind_start',
    'spectrum_G_ind_end',
    'spectrum_R_ind_start',
    'spectrum_R_ind_end',
    'spectrum_I_ind_start',
    'spectrum_I_ind_end'
]

# Following https://www.galah-survey.org/dr3/using_the_data/#recommended-flag-values
def selection_fn(catalog):
    mask = catalog['snr_c3_iraf'] > 30            # Reasonable signal-to-noise ratio
    mask &= catalog['flag_sp'] == 0
    mask &= catalog['flag_fe_h'] == 0
    mask &= catalog['survey_name'] == 'galah_main'
    return mask


def get_resolution(ccd_resolution_map_filename):
    try:
        resolution = fits.open(ccd_resolution_map_filename)[0]
    except IndexError:
        raise IOError("The resolution map file is invalid or empty!")
    
    mean_resolution = np.mean(resolution.data, axis=0)
    def _gauss(x, a, x0, sigma):
        return a * np.exp(-(x - x0) ** 2 / (2 * sigma ** 2))
    try:
        popt, _ = curve_fit(_gauss, np.arange(len(mean_resolution)), mean_resolution, p0=[1, 5, 1])
    except:
        popt = [-1, -1, -1]
    return mean_resolution, popt[2]*np.ones(shape=len(mean_resolution), dtype=np.float32)


def process_band_fits(filename, resolution_filename):
    spectrum = {}
    try:
        hdus = fits.open(filename)
        
        # Check if normalized spectrum exists (HDU 4)
        if len(hdus) <= 4:
            return defaultdict(list)  # Skip this sample if no normalized spectrum
        
        # Unnormalized, sky-subtracted spectrum
        flux = hdus[0].data
        sigma = hdus[1].data
        
        # Normalized spectrum
        norm_flux = hdus[4].data
        norm_start_wavelength = hdus[4].header["CRVAL1"]
        norm_dispersion = hdus[4].header["CDELT1"]
        norm_nr_pixels = hdus[4].header["NAXIS1"]
        norm_reference_pixel = hdus[4].header["CRPIX1"]
        if norm_reference_pixel == 0:
            norm_reference_pixel = 1
        
        start_wavelength = hdus[0].header["CRVAL1"]
        dispersion = hdus[0].header["CDELT1"]
        nr_pixels = hdus[0].header["NAXIS1"]
        reference_pixel = hdus[0].header["CRPIX1"]
        if reference_pixel == 0:
            reference_pixel = 1
        
        spectrum['flux'] = flux
        spectrum['lambda'] = ((np.arange(0,nr_pixels)--reference_pixel+1)*dispersion+start_wavelength)
        spectrum['ivar'] = 1 / np.power(flux * sigma, 2)
        
        lsf, lsf_sigma = get_resolution(resolution_filename)
        
        spectrum['norm_flux'] = norm_flux
        spectrum['norm_lambda'] = ((np.arange(0,norm_nr_pixels)--norm_reference_pixel+1)*norm_dispersion+norm_start_wavelength)
        spectrum['norm_ivar'] = 1 / np.power(norm_flux * sigma, 2)
        spectrum['lsf'] = lsf
        spectrum['lsf_sigma'] = lsf_sigma
        spectrum['timestamp'] = hdus[0].header['UTMJD']
        
        return spectrum
    except FileNotFoundError as e:
        return defaultdict(list)
    
def download_tar_file(tar_file, base_url, output_path):
    tar_url = base_url + tar_file
    try:
        response = requests.get(tar_url, stream=True)
        
        temp_tar = os.path.join(output_path, f'temp_{tar_file}')
        with open(temp_tar, "wb") as file:
            for data in response.iter_content(chunk_size=1024*1024):  # Use 1MB chunks instead of 1KB
                file.write(data)
        
        # Extract all files
        with tarfile.open(temp_tar) as tar:
            strip = max([len(n.split('/')) for n in tar.getnames() if n.endswith('.fits')])
            tar.extractall(output_path, members=members(tar, strip))
        
        # Clean up
        os.remove(temp_tar)
        return True
        
    except Exception as e:
        print(f"\nSkipping {tar_file}: {e}")
        return False

def download_galah_data(output_path, tiny=False, max_workers=10):
    if not Path(output_path).exists():
        Path(output_path).mkdir(parents=True)
    
    if tiny:
        # Download a small sample of GALAH data for testing
        catalog_url = "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/131116_com.tar.gz"
        response = requests.get(catalog_url, stream=True)

        # Sizes in bytes.
        total_size = int(response.headers.get("content-length", 0))
        block_size = 1024

        with tqdm(total=total_size, unit="B", unit_scale=True) as progress_bar:
            with open(os.path.join(output_path, 'spectra.tar.gz'), "wb") as file:
                for data in response.iter_content(block_size):
                    progress_bar.update(len(data))
                    file.write(data)

        if total_size != 0 and progress_bar.n != total_size:
            raise RuntimeError("Could not download file")
        
        zipped_file = tarfile.open(os.path.join(output_path, 'spectra.tar.gz'))
        strip = max([len(n.split('/')) for n in zipped_file.getnames() if n.endswith('.fits')])
        zipped_file.extractall(output_path, members=members(zipped_file, strip=strip)[:8])
        zipped_file.close()
        os.remove(zipped_file.name)
        
        filenames = os.listdir(output_path)
        # move the spectra
        object_ids = list(set([int(f.split('.')[0][:-1]) for f in filenames if '.fits' in f]))

        objects_data = get_objects_table(object_ids)
        objects_data.to_csv(os.path.join(output_path, 'objects.csv'), index=False)
    
    else:
        base_url = "https://cloud.datacentral.org.au/teamdata/GALAH/public/GALAH_DR3/spectra/tar_files/"
        
        # Get directory listing
        print("Fetching list of available tar files...")
        response = requests.get(base_url)
        if response.status_code != 200:
            raise Exception(f"Failed to access {base_url}")
            
        # Parse HTML to find all tar.gz files
        soup = BeautifulSoup(response.text, 'html.parser')
        tar_files = [link.get('href') for link in soup.find_all('a') 
                    if link.get('href', '').endswith('_com.tar.gz')]
        
        print(f"Found {len(tar_files)} tar files to download")
        
        # Create partial function with fixed arguments
        download_fn = partial(download_tar_file, base_url=base_url, output_path=output_path)
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(download_fn, tar_file) for tar_file in tar_files]
            for future in tqdm(futures, total=len(tar_files)):
                future.result()
        
        successful = sum(1 for future in futures if future.result())
        print(f"\nSuccessfully downloaded {successful} of {len(tar_files)} tar files")
    
    # Resolution maps
    resolution_maps_path = Path(os.path.join(output_path, "resolution_maps"))
    resolution_maps_path.mkdir(parents=True)
    
    resolution_maps = download_resolution_maps()
    for i, rm in tqdm(enumerate(resolution_maps)):
        open(os.path.join(resolution_maps_path, "ccd{i:d}_piv.fits".format(i=i+1)), "wb").write(rm)
    

def process_healpix_bin(args):
    """Wrapper function to unpack arguments and process a single HEALPix bin"""
    pix_catalog, output_file, galah_data_path = args
    
    print(f"\nProcessing catalog with {len(pix_catalog)} objects")
    
    # Process each object's spectra
    processed_objects = []
    for _, row in pix_catalog.iterrows():
        object_id = row['object_id']
        print(f"Processing object_id: {object_id}")
        
        # Load spectra for all 4 bands
        spectra = {}
        for band in FILTERS:
            # Use numerical suffix instead of letter
            fits_file = os.path.join(galah_data_path, f"{object_id}{FILTER_MAP[band]}.fits")
            resolution_file = os.path.join(galah_data_path, "resolution_maps", f"ccd{FILTERS.index(band)+1}_piv.fits")
            
            if not os.path.exists(fits_file):
                print(f"Missing fits file: {fits_file}")
                continue
            if not os.path.exists(resolution_file):
                print(f"Missing resolution file: {resolution_file}")
                continue
                
            spectra[band] = process_band_fits(fits_file, resolution_file)
            if not spectra[band]:  # if empty dictionary returned
                print(f"Failed to process band {band} for object {object_id}")
        
        # Only include objects with all 4 bands
        if len(spectra) == 4:
            try:
                # Combine spectral data with catalog metadata
                object_data = {
                    'object_id': object_id,
                    'flux_unit': 'normalized',
                    'timestamp': row['timestamp'],
                    'ra': row['ra'],
                    'dec': row['dec'],
                    'teff': row['teff'],
                    'e_teff': row['e_teff'],
                    'logg': row['logg'],
                    'e_logg': row['e_logg'],
                    'fe_h': row['fe_h'],
                    'e_fe_h': row['e_fe_h'],
                    'fe_h_atmo': row['fe_h_atmo'],
                    'vmic': row['vmic'],
                    'vbroad': row['vbroad'],
                    'e_vbroad': row['e_vbroad'],
                    'alpha_fe': row['alpha_fe'],
                    'e_alpha_fe': row['e_alpha_fe'],
                    
                    # Spectral data
                    'spectrum_flux': np.concatenate([spectra[band]['flux'] for band in FILTERS]),
                    'spectrum_ivar': np.concatenate([spectra[band]['ivar'] for band in FILTERS]),
                    'spectrum_lambda': np.concatenate([spectra[band]['lambda'] for band in FILTERS]),
                    'spectrum_norm_flux': np.concatenate([spectra[band]['norm_flux'] for band in FILTERS]),
                    'spectrum_norm_ivar': np.concatenate([spectra[band]['norm_ivar'] for band in FILTERS]),
                    'spectrum_norm_lambda': np.concatenate([spectra[band]['norm_lambda'] for band in FILTERS]),
                    'spectrum_lsf_sigma': np.concatenate([spectra[band]['lsf_sigma'] for band in FILTERS]),
                    
                    # Store indices for each band
                    'B_start': 0,
                    'B_end': len(spectra['B']['flux']),
                    'G_start': len(spectra['B']['flux']),
                    'G_end': len(spectra['B']['flux']) + len(spectra['G']['flux']),
                    'R_start': len(spectra['B']['flux']) + len(spectra['G']['flux']),
                    'R_end': len(spectra['B']['flux']) + len(spectra['G']['flux']) + len(spectra['R']['flux']),
                    'I_start': len(spectra['B']['flux']) + len(spectra['G']['flux']) + len(spectra['R']['flux']),
                    'I_end': len(spectra['B']['flux']) + len(spectra['G']['flux']) + len(spectra['R']['flux']) + len(spectra['I']['flux'])
                }
                processed_objects.append(object_data)
                print(f"Successfully processed object {object_id}")
            except Exception as e:
                print(f"Failed to process object {object_id}: {str(e)}")
    
    # Convert to DataFrame
    df = pd.DataFrame(processed_objects)
    print(f"\nProcessed {len(df)} objects successfully")
    
    # Save to HDF5
    if len(df) > 0:  # Only save if we have data
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            df.to_hdf(output_file, 
                      key='spectra', 
                      mode='w',
                      complevel=5,
                      complib='zlib',
                      format='table')
            print(f"Successfully saved data to {output_file}")
            return True
        except Exception as e:
            print(f"Failed to save HDF5 file: {str(e)}")
            return False
    else:
        print("No data to save")
        return False

def get_healpix_index(ra, dec, nside=32):
    """Convert RA/Dec to HEALPix index."""
    phi = np.radians(ra)
    theta = np.radians(90 - dec)
    return hp.ang2pix(nside, theta, phi)

def extract_metadata_from_fits(fits_file):
    """Extract key metadata from a FITS file header."""
    try:
        with fits.open(fits_file) as hdul:
            header = hdul[0].header
            return {
                'ra': header.get('RA', 0.0),
                'dec': header.get('DEC', 0.0),
                'object_id': int(fits_file.stem[:-1]),  # Remove the CCD number
                'timestamp': header.get('UTMJD', 0.0)
            }
    except Exception as e:
        print(f"Error reading {fits_file}: {e}")
        return None

def main(args):
    # Load the catalog file and apply main cuts
    if not os.path.exists(args.galah_data_path):
        Path(args.galah_data_path).mkdir(parents=True)

    # For full dataset, clear directory first if it only contains tiny data
    if not args.tiny:
        fits_files = list(Path(args.galah_data_path).glob('*.fits'))
        if len(fits_files) <= 8:  # tiny dataset has 8 files
            print("Clearing tiny dataset to download full dataset...")
            for f in fits_files:
                f.unlink()
            
    # Download data if directory is empty
    if not os.listdir(args.galah_data_path) or (not args.tiny and len(fits_files) <= 8):
        print("Starting download of", "tiny dataset..." if args.tiny else "full dataset...")
        download_galah_data(args.galah_data_path, args.tiny)
    else:
        print(f"Found existing full dataset in {args.galah_data_path}")
    
    # Check if we have any .fits files
    fits_files = list(Path(args.galah_data_path).glob('*.fits'))
    if not fits_files:
        print("No .fits files found. Something went wrong with the download.")
        return
    
    print(f"Found {len(fits_files)} .fits files to process")
    
    # Group FITS files by object_id (they come in sets of 4 for B,G,R,I)
    fits_by_object = defaultdict(list)
    for f in fits_files:
        if f.suffix == '.fits' and not 'resolution_maps' in str(f):
            object_id = int(f.stem[:-1])
            fits_by_object[object_id].append(f)
    
    # Only keep objects that have all 4 bands
    complete_objects = {k: v for k, v in fits_by_object.items() if len(v) == 4}
    
    print(f"Found {len(complete_objects)} complete objects with all 4 bands")
    
    # Extract metadata from first band file of each object
    object_metadata = {}
    for obj_id, files in tqdm(complete_objects.items(), desc="Extracting metadata"):
        metadata = extract_metadata_from_fits(files[0])
        if metadata:
            object_metadata[obj_id] = metadata
    
    # Assign HEALPix indices
    ra_dec_data = np.array([[m['ra'], m['dec']] for m in object_metadata.values()])
    healpix_indices = get_healpix_index(ra_dec_data[:, 0], ra_dec_data[:, 1])
    
    # Create output directory structure and prepare processing tasks
    output_base = Path(args.output_path)
    output_base.mkdir(parents=True, exist_ok=True)
    
    # Group objects by HEALPix index
    objects_by_healpix = defaultdict(list)
    for (obj_id, metadata), healpix_idx in zip(object_metadata.items(), healpix_indices):
        objects_by_healpix[healpix_idx].append((obj_id, metadata))
    
    # Process each HEALPix bin
    with Pool(args.num_processes) as pool:
        tasks = []
        for pix, obj_list in objects_by_healpix.items():
            # Get the full object data from GALAH DR3
            object_ids = [obj_id for obj_id, _ in obj_list]
            full_catalog = get_objects_table(object_ids)
            
            # Create catalog DataFrame with all required columns
            pix_catalog = pd.DataFrame([
                {
                    'sobject_id': obj_id,
                    'ra': meta['ra'],
                    'dec': meta['dec'],
                    'timestamp': meta['timestamp']
                }
                for obj_id, meta in obj_list
            ])
            
            # Merge with the full catalog data
            pix_catalog = pd.merge(pix_catalog, full_catalog, on='sobject_id', how='left')
            
            # Rename sobject_id to object_id after merge
            pix_catalog = pix_catalog.rename(columns={'sobject_id': 'object_id'})
            
            # Make sure pix_catalog stays as a DataFrame, don't convert to Table yet
            output_dir = output_base / f"healpix={pix}"
            output_dir.mkdir(exist_ok=True)
            output_file = output_dir / "spectra.hdf5"
            
            tasks.append((
                pix_catalog.copy(),  # Make sure to pass a copy of the DataFrame
                str(output_file),
                args.galah_data_path
            ))
        
        # Process all HEALPix bins in parallel
        results = list(tqdm(
            pool.imap_unordered(process_healpix_bin, tasks),  # Changed from save_in_standard_format to process_healpix_bin
            total=len(tasks),
            desc="Processing HEALPix bins"
        ))
    
    print(f"Successfully processed {sum(results)} of {len(tasks)} HEALPix bins")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts spectra from GALAH')
    parser.add_argument('galah_data_path', type=str, help='Path to the local copy of the GALAH data')
    parser.add_argument('output_path', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    args = parser.parse_args()

    main(args)