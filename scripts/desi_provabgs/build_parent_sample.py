from astropy.table import Table
import healpy as hp
import numpy as np
import os
from tqdm import tqdm
import argparse
import h5py

from provabgs import models as Models

provabgs_file = 'https://data.desi.lbl.gov/public/edr/vac/edr/provabgs/v1.0/BGS_ANY_full.provabgs.sv3.v0.hdf5'

_healpix_nside = 16

def download_data(save_path: str):
    """Download the PROVABGS data from the web and save it to the specified directory."""
    # Check if the save path exists
    if not os.path.exists(save_path):
        os.makedirs(save_path)
    
    # Download the PROVABGS file
    local_path = os.path.join(save_path, 'BGS_ANY_full.provabgs.sv3.v0.hdf5')
    if not os.path.exists(local_path):
        print("Downloading PROVABGS data...")
        os.system(f"wget {provabgs_file} -O {local_path}")
        print("Downloaded PROVABGS data successfully!")
    else:
        print("PROVABGS data already exists!")


def _get_best_fit(provabgs: Table):
    """Get the best fit model for each galaxy."""
    m_nmf = Models.NMF(burst=True, emulator=True)

    # Filter out galaxies with no best fit model
    provabgs = provabgs[(provabgs['PROVABGS_LOGMSTAR_BF'] > 0) *
                   (provabgs['MAG_G'] > 0) *
                   (provabgs['MAG_R'] > 0) *
                   (provabgs['MAG_Z'] > 0)]

    # Get the thetas and redshifts for each galaxy
    thetas = provabgs['PROVABGS_THETA_BF'][:, :12]
    zreds = provabgs['Z_HP']

    Z_mw    = [] # Stellar Metallicitiy
    tage_mw = [] # Age
    avg_sfr = [] # Star-Forming Region

    print("Calculating properties using the PROVABGS model...")
    for i in tqdm(range(len(thetas))):
        theta = thetas[i]
        zred = zreds[i]
        
        # Calculate properties using the PROVABGS model
        Z_mw.append(m_nmf.Z_MW(theta, zred=zred))
        tage_mw.append(m_nmf.tage_MW(theta, zred=zred))
        avg_sfr.append(m_nmf.avgSFR(theta, zred=zred))

    # Add the properties to the table
    provabgs['Z_MW'] = np.array(Z_mw)
    provabgs['TAGE_MW'] = np.array(tage_mw)
    provabgs['AVG_SFR'] = np.array(avg_sfr)
    return provabgs


def save_in_standard_format(input_path: str, output_dir: str):
    """Save the input HDF5 file in the standard format for the HEALPix-based dataset."""
    data = Table.read(input_path)

    # Get the best fit model for each galaxy
    data = _get_best_fit(data)

    # Get the RA and DEC columns
    ra = data['RA']
    dec = data['DEC']

    # Convert the RA and DEC to HEALPix indices and find the unique indices
    healpix_indices = hp.ang2pix(_healpix_nside, ra, dec, lonlat=True, nest=True)
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

        # Make HEALPix index subdirectory if it does not exist
        if not os.path.exists(output_subdir):
            os.makedirs(output_subdir)
        
        # AstroPile convention
        output_path = os.path.join(output_subdir, '001-of-001.h5')

        # Save files
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

    # Download the data if it does not already exist
    download_data(args.input_path)

    # Save the data in the standard format
    save_in_standard_format(args.input_path, output_dir)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert an HDF5 file to the standard format for the HEALPix-based dataset")
    parser.add_argument('input_path', type=str, help="Path to the input HDF5 file. If the file does not exist, it will be downloaded.")
    parser.add_argument('output_dir', type=str, help="Path to the output directory")
    args = parser.parse_args()
    main(args)