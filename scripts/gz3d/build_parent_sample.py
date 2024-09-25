
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

def create_dataframe_from_files(folder: str, file_name: str = 'reconstructed_gz3d_catalog.csv'):
    file_path = pathlib.Path(folder) / file_name
    os.makedirs(folder, exist_ok=True)
    locs = list(glob.glob(f"{folder}/*.fits.gz"))
    data = []
    for loc in tqdm(locs, desc="Populating dataframe"):
        manga_metadata_for_galaxy = fits.open(loc)[5].data
        columns = [x.name.lower() for x in manga_metadata_for_galaxy.columns]
        data.append(dict(zip(columns, manga_metadata_for_galaxy[0])))

    df = pd.DataFrame(data=data)
    df['relative_gz3d_fits_loc'] = locs
    df.to_csv(file_path, index=False)
    return df

def data_selection(df_: pd.DataFrame, threshold=0.2):
    df = df_.copy()
    df = df.dropna()
    df["gz_bar_fraction"] = df["gz_bar_votes"] / df["gz_total_classifications"]
    df["gz_spiral_fraction"] = df["gz_spiral_votes"] / df["gz_total_classifications"]
    if threshold > 0:
        df = df[(df['gz_spiral_fraction']>=threshold) | (df['gz_bar_fraction']>=threshold)]
    return df

def generate_all_hdf5(df_, nside=_healpix_nside, output_dir='output', local_data_path_prefix='', limit=None):
    # Create the output directory if it doesn't exist
    df = df_.copy()
    os.makedirs(output_dir, exist_ok=True)

    # Calculate HEALPix indices
    hpix = hp.ang2pix(nside, df['ra'], df['dec'], lonlat=True)
    df['healpix'] = hpix

    # Group by HEALPix index
    if limit:
        grouped = df.head(limit).groupby('healpix')
    else:
        grouped = df.groupby('healpix')

    # First, create the new column with NaN values
    df['relative_gz3d_hdf5_loc'] = pd.NA

    for hpix, group in tqdm(grouped, desc='HEALPix bins'):
        # Create a subdirectory for each HEALPix bin
        hpix_dir = os.path.join(output_dir, f'healpix={hpix}')
        os.makedirs(hpix_dir, exist_ok=True)

        for idx, row in group.iterrows():
            fits_file = row['relative_gz3d_fits_loc']
            hdf5_filename = os.path.basename(fits_file).replace('.fits', '.hdf5').rstrip('.gz')
            output_file = os.path.join(hpix_dir, hdf5_filename)
            
            # Update the DataFrame with the new HDF5 path
            df.at[idx, 'relative_gz3d_hdf5_loc'] = output_file

            # Read FITS file
            with fits.open(fits_file) as hdul:
                with h5py.File(output_file, 'w') as hdf:
                    wcs = WCS(hdul[1].header)
                    # hdf.attrs['wcs'] = wcs.to_header_string() # TODO: Decide how to encode this better.
                    hdf.create_dataset('false_color',  data=hdul[0].data.astype(np.int16), compression="gzip", chunks=True)
                    hdf.create_dataset('center', data=hdul[1].data.astype(np.int8), compression="gzip", chunks=True)
                    hdf.create_dataset('star',   data=hdul[2].data.astype(np.int8), compression="gzip", chunks=True)
                    hdf.create_dataset('spiral', data=hdul[3].data.astype(np.int8), compression="gzip", chunks=True)
                    hdf.create_dataset('bar',    data=hdul[4].data.astype(np.int8), compression="gzip", chunks=True)
                    # Include all metadata from catalog as attributes
                    for col in df.columns:
                        if col in ['relative_gz3d_fits_loc', 'relative_gz3d_hdf5_loc']:
                            continue
                        if df[col].dtype == 'O':
                            hdf.attrs[col] = str(row[col])
                        else:
                            hdf.attrs[col] = row[col]
                    # Rename the mangaid to object_id
                    hdf.attrs['object_id'] = hdf.attrs['mangaid']
                    # Include the scale
                    hdf.attrs['scale'] = abs(wcs.pixel_scale_matrix[0, 0]) # TODO: Check why this is negative? Is it negative in all samples?

    print(f"Conversion complete. Files saved in {output_dir}")
    return df

def main(args):
    # Download all files
    if args.tiny:
        limit = 5
    elif args.limit:
        limit = args.limit
    else:
        limit = None
    
    if args.local_data_path != '':
        df = create_dataframe_from_files(args.local_data_path)
    else:
        os.makedirs(args.output_dir, exist_ok=True)
        files = build_filelist(GZ3D_URL, limit=limit)
        download_all_files(files, GZ3D_URL, dir=args.output_dir, force=False)
        # Create a dataframe from the files
        df = create_dataframe_from_files(args.output_dir) # TODO: Don't create a dataframe ... but rather use the files directly? Maybe difficult for filtering?

    # Process the files, i.e. make chosen cuts
    df = data_selection(df, threshold=args.vote_fraction_cut)

    # Generate HDF5 files
    df = generate_all_hdf5(df, output_dir=args.output_dir, local_data_path_prefix=args.local_data_path, limit=limit)
    df.to_csv(f"{args.output_dir}/reconstructed_gz3d_catalog.csv", index=False)

    # Remove the downloaded files
    if not args.keep_fits:
        for f in df['relative_gz3d_fits_loc'].values:
            os.remove(f)
    
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts GZ3D data based on SDSS MaNGA files')
    parser.add_argument('--local_data_path', type=str, help='', default='')
    parser.add_argument('-o', '--output_dir', type=str, default='gz3d', help='Path to the output directory')
    parser.add_argument('--tiny', action="store_true", default=False, help='Use a small subset of the data for testing')
    parser.add_argument('--keep_fits', action="store_true", default=False, help='Keep the FITS files after conversion')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of files to process')
    parser.add_argument('--vote_fraction_cut', type=float, default=0.0, help='Selection cut on the vote fraction for spiral and bar galaxies.')
    args = parser.parse_args()

    main(args)
