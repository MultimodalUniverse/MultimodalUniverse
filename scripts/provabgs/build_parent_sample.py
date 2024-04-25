from astropy.table import Table
import healpy as hp
import numpy as np
import os
from tqdm import tqdm
import argparse
from multiprocessing import Pool
from jaxtyping import Str
import h5py

def save_in_standard_format(input_path: Str, output_dir: Str):
    """Save the input HDF5 file in the standard format for the HEALPix-based dataset."""
    data = Table.read(input_path)

    # Get the RA and DEC columns
    ra = data['RA']
    dec = data['DEC']

    # Convert the RA and DEC to HEALPix indices and find the unique indices
    healpix_indices = hp.ang2pix(16, ra, dec, lonlat=True, nest=True)
    unique_indices = np.unique(healpix_indices)

    print(f"Found {len(unique_indices)} unique HEALPix indices")

    keys = data.keys()
    keys.remove('RA')
    keys.remove('DEC')

    # Group the data by HEALPix index and save it in the standard format
    for index in tqdm(unique_indices, desc="Processing HEALPix indices"):
        mask = healpix_indices == index
        grouped_data = data[mask]
        output_subdir = os.path.join(output_dir, f'healpix={index}')

        if not os.path.exists(output_subdir):
            os.makedirs(output_subdir)
        
        output_path = os.path.join(output_subdir, '001-of-001.h5')

        with h5py.File(output_path, 'w') as output_file:
            for key in keys:
                output_file.create_dataset(key, data=grouped_data[key])
            output_file.create_dataset('object_id', data=grouped_data['TARGETID'])
            output_file.create_dataset('ra', data=grouped_data['RA'])
            output_file.create_dataset('dec', data=grouped_data['DEC'])
            output_file.create_dataset('healpix', data=np.full(grouped_data['TARGETID'].shape, index))

def main(args):
    """Main function to convert PROVABGS HDF5 file to the standard format for the HEALPix-based dataset."""
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