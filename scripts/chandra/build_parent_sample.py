# Astropile - Rafael Martinez-Galarza
# This code pre-process Chandra X-ray spectra and creates a standard for it
# It requieres Sherpa - X-ray data analysis package that can be installed from:
# https://sherpa.readthedocs.io/en/latest/install.html

import os
import shutil
import logging
import numpy as np
import pyvo as vo
from astropy.table import Table, Column, join, vstack
import glob
from sherpa.astro import ui # CIAO/Sherpa imports
import h5py
import argparse
import healpy as hp
import tqdm
from multiprocessing import Pool
from functools import partial
from tqdm.contrib.concurrent import process_map 

_healpix_nside = 16


def processing_fn(catalog, file):
    
    output = {
        "name": [],        # Target ID
        "obsid": [],       # Observation ID
        "obi": [],         # Observation interval
        "spectrum_ene_lo": [],  # Low end of the energy bin
        "spectrum_ene_hi": [],  # High end of the energy bin
        "spectrum_ene": [],     # Mid point of the energy bin
        "spectrum_flux": [],    # Counts/sec/keV
        "spectrum_flux_err": [] # Error in count value
    }

    # for file in files:
    ui.load_pha(file)               # Load file
    ui.ignore('0.:0.5,8.0:')        # Set energy range
    ui.subtract()                   # Subtract background
    ui.group_counts(5)              # Bin counts in energy axis
    pdata = ui.get_data_plot()      # Get the object with the spectral bins
    output["spectrum_ene_lo"].append(pdata.xlo)   
    output["spectrum_ene_hi"].append(pdata.xhi)
    output["spectrum_ene"].append(pdata.x)
    output["spectrum_flux"].append(pdata.y)
    output["spectrum_flux_err"].append(pdata.yerr)
    if (file.strip().split('/')[-1][0:24].strip().split('_')[2][-4:] == 'e2'):
        src_name = catalog['name'][(catalog['obsid'] == int(file.strip().split('/')[-1][0:27].strip().split('_')[0][-5:])) &
            (catalog['obi'] == int(file.strip().split('/')[-1][0:27].strip().split('_')[1][0:3])) &
            (catalog['region_id'] == int(file.strip().split('/')[-1][0:27].strip().split('_')[3][-4:]))]
    else:
        src_name = catalog['name'][(catalog['obsid'] == int(file.strip().split('/')[-1][0:24].strip().split('_')[0][-5:])) &
            (catalog['obi'] == int(file.strip().split('/')[-1][0:24].strip().split('_')[1][0:3])) &
            (catalog['region_id'] == int(file.strip().split('/')[-1][0:24].strip().split('_')[2][-4:]))]
    output["name"].append(src_name.data[0])
    output["obsid"].append(int(file.strip().split('/')[-1][0:24].strip().split('_')[0][-5:]))
    output["obi"].append(int(file.strip().split('/')[-1][0:24].strip().split('_')[1][0:3]))
        
    # Return the results
    return output

def save_in_standard_format(catalog, output_path=".", chandra_data_path="./output_data/", num_workers=1):
    """ Save the spectra in standard HDF5 format
    """
    os.makedirs(os.path.dirname(chandra_data_path), exist_ok=True)
    
    logger = logging.getLogger('sherpa')
    logger.setLevel(logging.ERROR)

    # Find all files in the directory
    files = glob.glob(chandra_data_path+'/*/*pha*')
    print("Loading {} files...".format(len(files)))
    results = process_map(partial(processing_fn, catalog), files, max_workers=num_workers, chunksize=10)
    print("Finished processing files")
    spectra = {k: [results[i][k][0] for i in range(len(results))] for k in results[0].keys()}

    spectra = Table(spectra)
    catalog = Table(catalog)
    
    catalog['name'] = catalog['name'].astype(str)
    catalog['obsid'] = catalog['obsid'].astype(int)
    catalog['obi'] = catalog['obi'].astype(int)
    spectra['name'] = spectra['name'].astype(str)
    spectra['obsid'] = spectra['obsid'].astype(int)
    spectra['obi'] = spectra['obi'].astype(int)
    # Join on target id with the input catalog
    catalog = join(catalog, spectra, keys=['name','obsid','obi'], join_type='inner')
    
    # Adding a healpix pixel number
    catalog['healpix'] = hp.ang2pix(_healpix_nside, catalog['ra'], catalog['dec'], lonlat=True, nest=True)

    # Group objects by healpix index
    groups = catalog.group_by('healpix')
    print("Outputting data in hdf5")
    for catalog in tqdm.tqdm(groups.groups):
        group_filename = os.path.join(output_path, 'healpix={}/001-of-001.hdf5'.format(catalog['healpix'][0]))
        os.makedirs(os.path.dirname(group_filename), exist_ok=True)
        with h5py.File(group_filename, 'w') as hdf5_file:
            for key in catalog.colnames:
                # Check if the column data type is a string
                if catalog[key].dtype.kind in ['U', 'S']:
                    # Encode Unicode string to byte string
                    encoded_strings = np.char.encode(catalog[key], 'utf-8')
                    hdf5_file.create_dataset(key, data=encoded_strings)
                elif catalog[key].dtype == 'O':  # Column with object dtype, potentially variable-length arrays
                    dt = h5py.special_dtype(vlen=np.dtype('float64'))  # or float32 if more appropriate
                    dataset = hdf5_file.create_dataset(key, (len(catalog[key]),), dtype=dt)
                    for i, item in enumerate(catalog[key]):
                        dataset[i] = item  # Assign the variable-length array directly
                else:
                    # Directly save the column as a dataset for non-string types
                    hdf5_file.create_dataset(key, data=catalog[key])                

def main(args):
    # Load the catalog from the catalog file, process the spectra, and save
    # the HDF5 file
    
    # Open the HDF5 file
    with h5py.File(args.file_path+args.cat_file, 'r') as hdf5_file:
        # Initialize an empty Astropy Table
        cat = Table()
    
        # Iterate over each dataset in the HDF5 file and add it as a column
        for key in hdf5_file.keys():
            data = hdf5_file[key][...]
            cat.add_column(Column(data, name=key)) 


    # Generate HDF5 file
    save_in_standard_format(catalog=cat, output_path=args.output_path, chandra_data_path=args.file_path, num_workers=args.num_workers)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Creates the parent sample of X-ray spectra from Chandra catalog')
    parser.add_argument('--cat_file', type=str, default='catalog.hdf5', help='Catalog file')
    parser.add_argument('--output_path', type=str, default='sample', help='Path to where to store the data')
    parser.add_argument('--file_path', type=str, default='./output_data/', help='Path to spectral files')
    parser.add_argument('--num_workers', type=int, default=1, help='Number of workers to use')

    args = parser.parse_args()

    main(args)

