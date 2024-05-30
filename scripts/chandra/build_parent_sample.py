# Astropile - Rafael Martinez-Galarza
# This code pre-process Chandra X-ray spectra and creates a standard for it
# It requieres Sherpa - X-ray data analysis package that can be installed from:
# https://sherpa.readthedocs.io/en/latest/install.html

import os
import numpy as np
import pyvo as vo
from astropy.table import Table, Column, join
import glob
from sherpa.astro import ui # CIAO/Sherpa imports
import h5py
import argparse


def processing_fn(catalog, chandra_data_path):
    
    output = {
        "name": [],    # Target ID
        "obsid": [],       # Observation ID
        "obi": [],         # Observation interval
        "spectrum_ene_lo": [],  # Low end of the energy bin
        "spectrum_ene_hi": [],  # High end of the energy bin
        "spectrum_ene": [], # Mid point of the energy bin
        "spectrum_flux": [],       # Counts/sec/keV
        "spectrum_flux_err": []        # Error in count value
    }
    ```
    
    # We now use Sherpa to extract the spectrum
    for file in glob.glob(PATH+'/*/*pha*'):
        
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


def save_in_standard_format(args):
    """ Save the spectra in standard HDF5 format
    """
    catalog, output_filename, chandra_data_path = args
    # Create the output directory if it does not exist
    #if not os.path.exists(os.path.dirname(output_filename)):
    #    os.makedirs(os.path.dirname(output_filename))

    # Rename columns to match the standard format
    #catalog['object_id'] = catalog['name']
    #catalog['observation'] = catalog['obsid']
    
    # Process all files
    spectra = Table(processing_fn(catalog, chandra_data_path))
    catalog = Table(catalog)
    
    catalog['name'] = catalog['name'].astype(str)
    catalog['obsid'] = catalog['obsid'].astype(int)
    catalog['obi'] = catalog['obi'].astype(int)
    spectra['name'] = spectra['name'].astype(str)
    spectra['obsid'] = spectra['obsid'].astype(int)
    spectra['obi'] = spectra['obi'].astype(int)
    # Join on target id with the input catalog
    catalog = join(catalog, spectra, keys=['name','obsid','obi'], join_type='inner')
    
    with h5py.File(chandra_data_path+output_filename, 'w') as hdf5_file:
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
    return


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
    save_in_standard_format(catalog=cat, output_filename=args.output_file, chandra_data_path=args.file_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Creates the parent sample of X-ray spectra from Chandra catalog')
    parser.add_argument('cat_file', type=str, default='catalog.hdf5', help='Catalog file')
    parser.add_argument('output_file', type=str, default='parent_sample_xray.hdf5', help='Name of file')
    parser.add_argument('file_path', type=str, default='/Users/juan/science/astropile/output_data/', help='Path to spectral files')
    args = parser.parse_args()

    main(args)

