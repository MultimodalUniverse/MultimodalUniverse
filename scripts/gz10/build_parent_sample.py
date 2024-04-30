import h5py
import healpy as hp
import numpy as np
import os
from tqdm import tqdm
import argparse
from multiprocessing import Pool

def process_index(args):
    input_path, output_dir, index, mask, object_ids = args
    
    with h5py.File(input_path, 'r') as file:
        grouped_data = {key: file[key][mask] for key in file.keys()}
    
    grouped_object_ids = object_ids[mask]
    output_subdir = os.path.join(output_dir, f'healpix={index}')

    if not os.path.exists(output_subdir):
        os.makedirs(output_subdir)

    output_path = os.path.join(output_subdir, '001-of-001.h5')
    
    with h5py.File(output_path, 'w') as output_file:
        for key in grouped_data:
            output_file.create_dataset(key, data=grouped_data[key])
        output_file.create_dataset('object_id', data=grouped_object_ids)
        output_file.create_dataset('healpix', data=np.full(grouped_object_ids.shape, index))

def save_in_standard_format(input_path, output_dir):
    with h5py.File(input_path, 'r') as file:
        required_keys = ['ans', 'dec', 'images', 'pxscale', 'ra', 'redshift']
        if not all(key in file.keys() for key in required_keys):
            raise ValueError("Missing one or more required datasets in the HDF5 file")

        ra = file['ra'][:]
        dec = file['dec'][:]
        healpix_indices = hp.ang2pix(16, ra, dec, lonlat=True, nest=True)
        unique_indices = np.unique(healpix_indices)
    
    print(f"Found {len(unique_indices)} unique HEALPix indices")

    object_ids = np.arange(len(ra))
    pool_args = [(input_path, output_dir, index, healpix_indices == index, object_ids)
                 for index in unique_indices]

    with Pool() as pool:
        list(tqdm(pool.imap(process_index, pool_args), total=len(pool_args), desc="Processing HEALPix indices"))

def main(args):
    output_dir = os.path.join(args.output_dir, 'datafiles')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    save_in_standard_format(args.input_path, output_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert an HDF5 file to the standard format for the HEALPix-based dataset")
    parser.add_argument('input_path', type=str, help="Path to the input HDF5 file")
    parser.add_argument('output_dir', type=str, help="Path to the output directory")
    args = parser.parse_args()
    main(args)
