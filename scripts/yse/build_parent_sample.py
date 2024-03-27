import os
import h5py
import argparse
import numpy as np
import sncosmo
import shutil

def get_str_dtype(value):
    # Get dtype of input value
    str_max_len = int(np.char.str_len(value).max())
    return h5py.string_dtype(encoding='utf-8', length=str_max_len)

def main(args):
    # Retrieve file paths
    file_paths = os.listdir(args.yse_data_path)
    num_examples = len(file_paths)

    # List of fields for the final data file
    field = ['object_id', 'ra', 'dec', 'time', 'flux', 'flux_err', 'band', 'quality_mask', 'z_helio', 'z_phot', 'mwebv', 'host_log_mass', 'spec_class']
    
    # List of keys corresponding to fields of interest above
    key = ['SNID', 'RA', 'DECL', 'MJD', 'FLUXCAL', 'FLUXCALERR', 'FLT', 'FLAG', 'REDSHIFT_FINAL', 'PHOTO_Z', 'MWEBV', 'HOST_LOGMASS', 'SPEC_CLASS']

    # Tells us which fields to pull from the data (e.g. time, flux, band) vs metadata (e.g. redshift, classification)
    partition = ['metadata'] * 3 + ['data'] * 5 + ['metadata'] * 3

    # Place to store lengths of data files for later data padding
    length = []

    # Create dict which will be used to populate hdf5 file
    value = dict(zip(field, ([] for _ in field)))

    for file_path in file_paths:
        # Load data and metadata from snana files using functionality from sncosmo
        metadata, data = sncosmo.read_snana_ascii(args.yse_data_path+file_path, default_tablename='OBS')
        data = data['OBS']

        # Store length of data for later use in padding
        length.append(len(data['MJD']))

        # Populate dict with values from metadata or data (according to partition)
        for f, k, p in zip(field, key, partition):
            p = metadata if p == 'metadata' else data
            value[f].append(p[k])

    # Get length to pad to
    length = np.array(length)
    max_len = np.max(length)
    pad = max_len - length

    # Pad data
    for i in range(num_examples):
        for f, p in zip(field, partition):
            if p == 'data':
                value[f][i] = np.pad(value[f][i], (0, pad[i]), 'constant', constant_values=(0, 0))

    # Create list of dtypes for output file
    #dtype = ['S'+str(np.char.str_len(value['object_id']).max()), np.float32, np.float32, np.float32, np.float32, np.float32, 'S'+str(np.char.str_len(value['band']).max()), np.float32, np.float32, np.float32, np.float32]
    dtype = [get_str_dtype(value['object_id']), np.float32, np.float32, np.float32, np.float32, np.float32, get_str_dtype(value['band']), np.float32, np.float32, np.float32, get_str_dtype(value['spec_class'])]

    # Write to hdf5 file
    os.makedirs(args.output_dir)
    with h5py.File(args.output_dir+'yse_dr1.hdf5', 'w') as hdf5_file:
        for f, d in zip(field, dtype):
            if f == 'quality_mask':
                v = np.zeros((num_examples, max_len), dtype=d)
                # NOTE: could also encode 0x00 and 0 as strings
            else:
                v = np.array(value[f], dtype=d)
            hdf5_file.create_dataset(f, data=v)

    # Remove original data downloaded from Zenodo
    if True:
        shutil.rmtree(args.yse_data_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract YSE data and convert to standard time-series data format.')
    parser.add_argument('yse_data_path', type=str, help='Path to the local copy of the YSE DR1 data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    args = parser.parse_args()

    main(args)

