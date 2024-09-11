
import argparse
import itertools
import pathlib
from multiprocessing import Pool
import numpy as np

from astropy.io import fits
from astropy.table import Table, join
from tqdm import tqdm
import healpy as hp
from astropy.io import fits
from astropy.wcs import WCS
import h5py

import pandas as pd
import glob
import re, os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import gzip


GZ3D_URL = "https://data.sdss.org/sas/dr17/env/MANGA_MORPHOLOGY/galaxyzoo3d/v4_0_0/"
_utf8_filter_type = h5py.string_dtype('utf-8', 5)
_healpix_nside = 16

def build_filelist(page_url: str, download_link: str = '^.fits.gz$', limit: int = None):
    """ Build a list of relevant files to transfer """
    response = requests.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    hrefs = soup.find_all('a', href=True)
    files = []

    n_files = limit or len(hrefs)
    for ref in hrefs[0:n_files]:
        if re.search(download_link, ref['href']):
            files.append(ref['href'])
    
    return files

def download_all_files(links, base_url, test=False, dir="./data"):
    for link in tqdm(links):
        try:
            download_file(base_url, link, local_dir=dir)
        except Exception as e:
            print(f"Error downloading {urljoin(base_url+link)}: {e}")
        if test:
            break

def download_file(url: str, file_name: str, download_dir: str ='./data', chunk_size: int = 8192):
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

    local_dir = pathlib.Path(local_dir)
    local_filename = download_dir / pathlib.Path(file_name)
    local_filename.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(full_url, stream=True) as r:
        r.raise_for_status()
        with local_filename.open('wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk: # Skip empty chunks
                    f.write(chunk)
    # Extract fits from gz
    with gzip.open(local_filename, 'rb') as f_in:
        with open(local_filename.replace('.gz', ''), 'wb') as f_out:
            f_out.write(f_in.read())
    
    return str(local_filename.replace('.gz', ''))  # Return the file path as a string

def create_dataframe_from_files(folder: str, file_name: str = 'reconstructed_gz3d_catalog_new.csv', force: bool = False):
    file_path = pathlib.Path(folder) / file_name
    os.makedirs(folder, exist_ok=True)
    if not force:
        try:
            df = pd.read_csv(file_path)
            return df
        except:
            print(f"Creating new dataframe. File not found at {file_path}")
            pass
    locs = list(glob.glob(f'{folder}*.fits.gz'))
    data = []
    for loc in tqdm(locs):
        manga_metadata_for_galaxy = fits.open(loc)[5].data
        columns = [x.name.lower() for x in manga_metadata_for_galaxy.columns]
        data.append(dict(zip(columns, manga_metadata_for_galaxy[0])))

    df = pd.DataFrame(data=data)
    df['relative_gz3d_fits_loc'] = locs
    df.to_csv(file_path, index=False)
    return df

def data_selection(df_: pd.DataFrame):
    df = df_.copy()
    df = df.dropna()
    df["gz_bar_fraction"] = df["gz_bar_votes"] / df["gz_total_classifications"]
    df["gz_spiral_fraction"] = df["gz_spiral_votes"] / df["gz_total_classifications"]
    df = df[(df['gz_spiral_fraction']>0.2) | (df['gz_bar_fraction']>0.2)]
    return df

def generate_all_hdf5(df, nside=_healpix_nside, output_dir='output', local_data_path_prefix='', limit=None):
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Calculate HEALPix indices
    hpix = hp.ang2pix(nside, df['ra'], df['dec'], lonlat=True)
    df['healpix'] = hpix

    # Group by HEALPix index
    if limit:
        grouped = df.head(limit).groupby('healpix')
    else:
        grouped = df.groupby('healpix')

    for hpix, group in tqdm(grouped, desc='HEALPix bins'):
        # Create a subdirectory for each HEALPix bin
        hpix_dir = os.path.join(output_dir, f'hpix_{hpix}')
        os.makedirs(hpix_dir, exist_ok=True)

        for _, row in group.iterrows():
            fits_file = os.path.join(f'{local_data_path_prefix}fits_gz', row['relative_gz3d_fits_loc'].split('/')[-1])
            output_file = os.path.join(hpix_dir, f"{os.path.basename(fits_file).replace('.fits', '.h5').rstrip('.gz')}")

            # Read FITS file
            with fits.open(fits_file) as hdul:
                with h5py.File(output_file, 'w') as hdf:
                # Assuming the main data is in the primary HDU
                    print(hdul.info())
                    wcs = WCS(hdul[1].header) # TODO: Decide how to encode this
                    hdf.attrs['wcs'] = wcs.to_header_string()
                    hdf.create_dataset('image',       data=hdul[0].data)
                    hdf.create_dataset('center_mask', data=hdul[1].data)
                    hdf.create_dataset('star_mask',   data=hdul[2].data)
                    hdf.create_dataset('spiral_mask', data=hdul[3].data)
                    hdf.create_dataset('bar_mask',    data=hdul[4].data)                    

    print(f"Conversion complete. Files saved in {output_dir}")

def main(args):
    # Download all files
    limit = 5 #if args.tiny else None
    if args.local_data_path != '':
        df = create_dataframe_from_files(args.local_data_path)
    else:
        os.makedirs(args.output_dir, exist_ok=True)
        files = build_filelist(GZ3D_URL, limit=limit)
        download_all_files(files, GZ3D_URL, dir=args.output_dir)
        # Create a dataframe from the files
        df = create_dataframe_from_files(f"{args.output_dir}/gz3d/")

    # Process the files, i.e. make chosen cuts
    df = data_selection(df)

    # Generate HDF5 files
    generate_all_hdf5(df, output_dir=args.output_dir, local_data_path_prefix=args.local_data_path, limit=limit)

 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts GZ3D data based on SDSS MaNGA files')
    parser.add_argument('--local_data_path', type=str, help='', default='')
    parser.add_argument('-o', '--output_dir', type=str, default='data', help='Path to the output directory')
    # parser.add_argument('-n', '--num_processes', type=int, default=8, help='The number of processes to use for parallel processing')
    parser.add_argument('--tiny', action="store_true", help='Use a small subset of the data for testing')
    args = parser.parse_args()

    main(args)
