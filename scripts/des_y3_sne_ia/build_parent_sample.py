import argparse
import h5py
import numpy as np
import os
import shutil
import sncosmo
import healpy as hp

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
    file_dir = args.des_data_path
    files = os.listdir(file_dir)
    for file_id,file in enumerate(files):
        # Load data and metadata from snana files using functionality from sncosmo
        metadata_, data_ = sncosmo.read_snana_ascii(os.path.join(file_dir, file), default_tablename='OBS')
        if len(data_['OBS']['BAND'])==0:
            files.pop(file_id)

    num_examples = len(files)

    if args.tiny:
        num_examples = 10
        files = files[:num_examples]

    # Load example data to determine keys in the dataset
    example_metadata, example_data = sncosmo.read_snana_ascii(os.path.join(file_dir, files[0]), default_tablename='OBS')
    example_data = example_data['OBS']
    keys_metadata = list(example_metadata.keys()) 
    keys_data = list(example_data.keys())

    # Set which keys will be ignored when loading/converting/saving the data
    ignored_keys = {
        'END',
        'FIELD',
        'FLAG',
        'MASK_USED',
  
    }

    # Remove ignored keys
    keys_metadata[:] = (key for key in keys_metadata if key not in ignored_keys)
    keys_data[:] = (key for key in keys_data if key not in ignored_keys)
    if 'SNTYPE' not in keys_metadata:
        keys_metadata.append('SNTYPE')
    keys_metadata.append('object_id')

    # Initialize dictionaries to store data and metadata
    data = dict(zip(keys_data, ([] for _ in keys_data)))
    metadata = dict(zip(keys_metadata, ([] for _ in keys_metadata)))

    for file in files:
        # Load data and metadata from snana files using functionality from sncosmo
        metadata_, data_ = sncosmo.read_snana_ascii(os.path.join(file_dir, file), default_tablename='OBS')
        data_ = data_['OBS']
        # Iterate over keys and append data to the corresponding list in data / metadata dicts
        for key in keys_data:
            if key in data_.keys(): data[key].append(data_[key].data)
            else: data[key].append(np.full(0, np.nan))
            # The data are astropy columns wrapping numpy arrays which are accessed via .data
        metadata_['SNTYPE']="Ia"
        metadata_['object_id']='DES_'+str(metadata_['SNID'])
        for key in keys_metadata:
            if key in metadata_.keys(): metadata[key].append(metadata_[key])
            else: metadata[key].append(np.nan)

    # Create an array of all bands in the dataset
    all_bands = np.unique(np.concatenate(data['BAND']))

    # Remove band from field_data as the timeseries will be arranged by band
    keys_data.remove('BAND')


    for i in range(num_examples):
        # For this example, find the band with the most observations
        # and store the number of observations as max_length
        _, count = np.unique(data['BAND'][i], return_counts=True)
        max_length = count.max()

        # Create mask to select data from each timeseries by band
        mask = np.expand_dims(all_bands, 1) == data['BAND'][i]

        for key in keys_data:
            timeseries_all_bands = []  # Stores a particular timeseries (as specified by the key) in all bands
            for j in range(len(all_bands)):
                timeseries_band = data[key][i][mask[j]]  # Select samples from timeseries for a particular band
                timeseries_band = np.pad(  # Pad single band timeseries to max_length
                    timeseries_band,
                    (0, max_length - len(timeseries_band)),
                    mode='constant',
                    constant_values=-99 if key == 'MJD' else 0
                )
                timeseries_all_bands.append(timeseries_band)
            timeseries_all_bands = convert_dtype(np.array(timeseries_all_bands))
            data[key][i] = timeseries_all_bands

    # Convert metadata to numpy arrays and cast to required datatypes
    for key in keys_metadata:
        metadata[key] = convert_dtype(np.array(metadata[key]))
    
    # Add healpix to metadata
    keys_metadata.append('healpix')
    metadata['healpix'] = hp.ang2pix(16, metadata['RA'], metadata['DECL'], lonlat=True, nest=True)

    # Cast bands to required datatype
    all_bands = convert_dtype(all_bands)

    # Establish conversions to standard names
    keys_all = keys_metadata + keys_data
    name_conversion = dict(zip(keys_all, keys_all))
    name_conversion.update({
        'RA': 'ra',
        'DECL': 'dec',
        'MJD': 'time',
        'FLUXCAL': 'flux',
        'FLUXCALERR': 'flux_err',
        'HOST_LOGMASS': 'host_log_mass',
        'SNTYPE': 'spec_class',
    })
    # map 'redshift' depending on which keys are available
    key_options_list = {
        'redshift': ['REDSHIFT_FINAL', 'REDSHIFT_CMB', 'REDSHIFT_HELIO'],
        'host_log_mass': ['HOST_LOGMASS', 'HOSTGAL_LOGMASS']
    }

    # Make output directories labelled by healpix
    unique_healpix = np.unique(metadata['healpix'])
    healpix_num_digits = len(str(hp.nside2npix(16)))
    for healpix in unique_healpix:
        healpix = str(healpix).zfill(healpix_num_digits)
        os.makedirs(os.path.join(args.output_dir, f'healpix={healpix}'), exist_ok=True)

    # Save data as hdf5 grouped into directories by healpix
    for i in range(num_examples):
        healpix = str(metadata['healpix'][i]).zfill(healpix_num_digits)
        object_id = metadata['object_id'][i]
        path = os.path.join(args.output_dir, f'healpix={healpix}', object_id.decode('utf-8')+'.hdf5')
        
        with h5py.File(path, 'w') as hdf5_file:
            # Determine which keys are used for dynamically used metadata
            for final_key, key_options in key_options_list.items():
                key_not_found = True
                for key in key_options:
                    if key in metadata.keys():
                        name_conversion.update({key: final_key})
                        key_not_found = False
                        break
                if key_not_found:
                    raise ValueError(f"No appropriate key found in metadata for object healpix={healpix}/example_{object_id}.hdf5. Accepted options are: {key_options}")

            # Save metadata
            for key in keys_metadata:
                hdf5_file.create_dataset(name_conversion[key], data=metadata[key][i])
            # Save bands

            hdf5_file.create_dataset(
                'bands', data=",".join(
                    list(
                        all_bands.squeeze().astype(str)
                    )
                )
            )
            # Save timeseries
            for key in keys_data:
                hdf5_file.create_dataset(name_conversion[key], data=data[key][i])

    # Remove original data (data has now been reformatted and saved as hdf5)
    if not args.dirty:
        shutil.rmtree(args.des_data_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract des DR1 data and convert to standard time-series data format.')
    parser.add_argument('des_data_path', type=str, help='Path to the local copy of the DES Y3 SN Ia data', default='./des_y3_sne_ia')
    parser.add_argument('output_dir', type=str, help='Path to the output directory', default='./')
    parser.add_argument('--tiny', action="store_true", help='Use a small subset of the data for testing')
    parser.add_argument('--dirty', action="store_true", help='Do not remove the original data')
    args = parser.parse_args()

    main(args)