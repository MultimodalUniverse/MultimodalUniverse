import h5py
import numpy as np
import sncosmo
import os
import argparse
import shutil

def get_str_dtype(arr):
    str_max_len = int(np.char.str_len(arr).max())
    return h5py.string_dtype(encoding='utf-8', length=str_max_len)

def convert_dtype(arr):
    if np.issubdtype(arr.dtype, np.floating):
        dtype = np.float32
    elif np.issubdtype(arr.dtype, np.str_):
        dtype = get_str_dtype(arr)
    else:
        dtype = arr.dtype
    return arr.astype(dtype)

def main(args):
    # Retrieve file paths
    file_dir = args.yse_data_path
    files = os.listdir(file_dir)
    num_examples = len(files)

    """
    ignored_keys = {'PARSNIP_S1', 'PARSNIP_S2', 'PARSNIP_S3', 'SET_ZTF_FP', 'processing', 'SEARCH_PEAKFLT', 'END'}

    example_metadata, example_data = sncosmo.read_snana_ascii(file_paths[0], default_tablename='OBS')
    example_data = example_data['OBS']
    key_metadata = [key for key in example_metadata.keys() if key not in ignored_keys]
    #key_metadata = list(example_metadata.keys())
    key_data = [key for key in example_data.keys() if key not in ignored_keys]
    #key_data = example_data.keys()
    key_all = key_metadata + key_data
    name_conversion = dict(zip(key_all, key_all))
    name_conversion.update({
        'SNID': 'object_id',
        'RA': 'ra',
        'DECL': 'dec',
        'MJD': 'time',
        'FLUXCAL': 'flux',
        'FLUXCALERR': 'flux_err',
        'FLT': 'band',
        'FLAG': 'quality_mask',
    })
    field_data = {name_conversion[key] for key in key_data}
    field_metadata = {name_conversion[key] for key in key_metadata}
    """

    field_data = ['time', 'flux', 'flux_err', 'band', 'quality_mask',]
    key_data = ['MJD', 'FLUXCAL', 'FLUXCALERR', 'FLT', 'FLAG',]

    field_metadata = ['object_id', 'ra', 'dec',  'redshift', 'host_log_mass', 'spec_class']
    key_metadata = ['SNID', 'RA', 'DECL', 'REDSHIFT_FINAL', 'HOST_LOGMASS', 'SPEC_CLASS']

    data = dict(zip(field_data, ([] for _ in field_data)))
    metadata = dict(zip(field_metadata, ([] for _ in field_metadata)))

    for file in files:
        # Load data and metadata from snana files using functionality from sncosmo
        metadata_, data_ = sncosmo.read_snana_ascii(os.path.join(file_dir, file), default_tablename='OBS')
        data_ = data_['OBS']
        for field, key in zip(field_data, key_data):
            data[field].append(data_[key].data)
            # The data are astropy columns wrapping numpy arrays which are accessed via .data
        for field, key in zip(field_metadata, key_metadata):
            metadata[field].append(metadata_[key])

    # Create an array of all bands in the dataset
    all_bands = np.unique(np.concatenate(data['band']))

    # Determine the length of the longest light curve (in a specific band) in the dataset
    max_length = 0
    for i in range(num_examples):
        _, count = np.unique(data['band'][i], return_counts=True)
        max_length = max(max_length, count.max())

    # Remove band from field_data as timeseries will be arranged by band
    field_data.remove('band')
    banded_data = dict(zip(field_data, ([] for _ in field_data)))

    for i in range(num_examples):
        # Create masks to select data from each timeseries by band
        mask = np.expand_dims(all_bands, 1) == data['band'][i]
        for field in field_data:
            d = []  # Stores the timeseries for each band
            for j in range(len(all_bands)):
                d_ = data[field][i][mask[j]]
                d_ = np.pad(d_, (0, max_length - len(d_)), mode='constant', constant_values=np.nan)
                d.append(d_)
            # Append timeseries arranged by band to relevant list of examples
            banded_data[field].append(d)

    # Convert timeseries to numpy arrays
    for field in field_data:
        banded_data[field] = np.array(banded_data[field])
    banded_data['band'] = np.array((all_bands for _ in range(num_examples)))

    # Convert metadata to numpy arrays
    for field in field_metadata:
        metadata[field] = np.array(metadata[field])

    # Save file as hdf5
    with h5py.File(os.path.join(args.output_dir, 'yse.h5'), 'w') as hdf5_file:
        # Save metadata
        for field in field_metadata:
            hdf5_file.create_dataset(field, data=convert_dtype(metadata[field]))
        # Save timeseries
        for field in field_data:
            hdf5_file.create_dataset(field, data=convert_dtype(banded_data[field]))

    # Remove original data downloaded from Zenodo
    if True:
        shutil.rmtree(args.yse_data_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract YSE data and convert to standard time-series data format.')
    parser.add_argument('yse_data_path', type=str, help='Path to the local copy of the YSE DR1 data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    args = parser.parse_args()

    main(args)
