
import argparse
import pathlib
from multiprocessing import Pool
import numpy as np

from astropy.io import fits
from astropy.table import Table, vstack
from tqdm import tqdm
import healpy as hp
from astropy.io import fits
import h5py

import pandas as pd
import glob
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


GZ3D_URL = "https://data.sdss.org/sas/dr17/env/MANGA_MORPHOLOGY/galaxyzoo3d/v4_0_0/"
_healpix_nside = 16


def data_selection(table, threshold=0.2):
    gz_bar_fraction = table["gz_bar_votes"] / table["gz_total_classifications"]
    gz_spiral_fraction = table["gz_spiral_votes"] / table["gz_total_classifications"]
    mask = (gz_bar_fraction>=threshold) | (gz_spiral_fraction>=threshold)
    return mask 

def is_link_to_fits_file(link_text, endswith='.fits.gz'):
    if link_text is None:
        return False
    else:
        return link_text.endswith(endswith)

def build_filelist(page_url: str, limit: int = None):
    """ Build a list of relevant files to transfer """
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    hrefs = soup.find_all('a', string=is_link_to_fits_file)
    # Get href attribute from each element as list
    files = []

    n_files = limit or len(hrefs)
    for ref in hrefs:
        files.append(ref['href'])
        if len(files) >= n_files:
            break
    
    return files

def download_all_files(links, base_url, test=False, dir="./data", force=False):
    for link in tqdm(links, desc="Downloading files"):
        try:
            download_file(base_url, link, download_dir=dir, force=force)
        except Exception as e:
            print(f"Error downloading {urljoin(base_url, link)}: {e}")
        if test:
            break

def download_file(url: str, file_name: str, download_dir: str ='./data', chunk_size: int = 8192, force: bool = False):
    """Download a file from a URL to a local directory in chunks (streaming).

    Args:
        url (str): URL of the file to download.
        file_name (str): Name of the file to download.
        download_dir (str, optional): Path to which the data is downloaded. Defaults to './data'.
        chunk_size (int, optional): Chunk size for streaming. Defaults to 8192.

    Returns:
        str: Path to the downloaded file.
    """

    full_url = urljoin(url, file_name)

    local_filename = download_dir / pathlib.Path(file_name)
    if local_filename.exists() and not force:
        return str(local_filename)
    local_filename.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(full_url, stream=True) as r:
        r.raise_for_status()
        with local_filename.open('wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk: # Skip empty chunks
                    f.write(chunk)
    
    return str(local_filename)  # Return the file path as a string


def _read_fits_file(loc):
    with fits.open(loc) as hdul:
        entry = pd.DataFrame(fits.open(loc)[5].data) 
        entry.columns = map(str.lower, entry.columns) # lowercase column names
        entry = Table.from_pandas(entry)

        entry['false_color'] = hdul[0].data.astype(np.uint8)[None, ...]
        entry['center'] = hdul[1].data.astype(np.uint8)[None, ...]
        entry['star'] = hdul[2].data.astype(np.uint8)[None, ...]
        entry['spiral'] = hdul[3].data.astype(np.uint8)[None, ...]
        entry['bar'] = hdul[4].data.astype(np.uint8)[None, ...]
    return entry

def main(args):
    # Download all files
    if args.tiny:
        limit = 5
    elif args.limit:
        limit = args.limit
    else:
        limit = None
    
    # Create the output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Download the data if it doesn't exist
    if args.local_data_path == '':
        files = build_filelist(GZ3D_URL, limit=limit)
        download_all_files(files, GZ3D_URL, dir=args.output_dir, force=False)
        local_data_path = args.output_dir
    else:
        local_data_path = args.local_data_path

    # Read all files in parallel and aggregate the data into a single dataframe
    locs = list(glob.glob(f"{local_data_path}/*.fits.gz"))

    print(f"Found {len(locs)} FITS files")
    with Pool() as pool:
        entries = list(tqdm(pool.imap(_read_fits_file, locs), total=len(locs), desc="Reading FITS files"))
    table = vstack(entries)

    # Data formatting and extra columns
    table['object_id'] = table['mangaid']
    hpix = hp.ang2pix(_healpix_nside, table['ra'], table['dec'], lonlat=True, nest=True)
    table['healpix'] = hpix
    
    # Process the files, i.e. make chosen cuts
    if args.vote_fraction_cut > 0.:
        table = table[data_selection(table, threshold=args.vote_fraction_cut)]

    # Group the data by HEALPix region
    table = table.group_by('healpix')

    # Export the data to HDF5 files by healpix region
    for group in tqdm(table.groups, desc='HEALPix bins'):
        # Create a subdirectory for each HEALPix bin
        hpix = group['healpix'][0]
        hpix_dir = os.path.join(args.output_dir, f'healpix={hpix}')
        os.makedirs(hpix_dir, exist_ok=True)

        # Open the output HDF5 file
        with h5py.File(os.path.join(hpix_dir, '001-of-001.hdf5'), 'w') as hdf:
            for key in group.colnames:
                if group[key].dtype.kind in {'U', 'S'}:
                    data = np.array(group[key], dtype='S').astype('S')
                else:
                    data = group[key]
                hdf.create_dataset(key, data=data)
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts GZ3D data based on SDSS MaNGA files')
    parser.add_argument('--local_data_path', type=str, help='', default='')
    parser.add_argument('-o', '--output_dir', type=str, default='gz3d', help='Path to the output directory')
    parser.add_argument('--tiny', action="store_true", default=False, help='Use a small subset of the data for testing')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of files to process')
    parser.add_argument('--vote_fraction_cut', type=float, default=0.0, help='Selection cut on the vote fraction for spiral and bar galaxies.')
    args = parser.parse_args()

    main(args)
