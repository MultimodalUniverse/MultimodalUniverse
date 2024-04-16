import argparse
import h5py
import numpy as np
import os
import shutil
import sncosmo
import healpy as hp
import pandas as pd

def get_str_dtype(arr):
    str_max_len = int(np.char.str_len(arr).max())
    return h5py.string_dtype(encoding="utf-8", length=str_max_len)


def convert_dtype(arr):
    if np.issubdtype(arr.dtype, np.floating):
        dtype = np.float32
    elif np.issubdtype(arr.dtype, np.str_):
        dtype = get_str_dtype(arr)
    else:
        dtype = arr.dtype
    return arr.astype(dtype)

def cfa_snII_bpf(file_dir, data, metadata, keys_data, keys_metadata, args):
    info = {}
    with open("cfa_info.dat", "r") as f:
        for line in f.readlines():
            info["SN20" + line.split()[0]] = line.split()[1:]
    optical_df = pd.read_csv(
        os.path.join(file_dir, "STDSYSTEM_LC.txt"),
        comment="#",
        sep=r'\s+',
        names=["name", "FLT", "time", "N", "mag", "mag_err"],
    )
    nir_df = pd.read_csv(
        os.path.join(file_dir, "NIR_LC.txt"),
        comment="#",
        sep=r'\s+',
        names=["name", "FLT", "time", "mag", "mag_err"],
    )
    df = pd.concat([optical_df, nir_df])
    unique_names = set(df['name'])
    if args.tiny:
        unique_names = list(unique_names)[:10]
    for sn_name in unique_names:
        mask = np.where(df["name"] == sn_name)
        for key in keys_data:
            data[key].append(np.array(df[key])[mask])
        metadata["name"].append(sn_name)
        metadata["ra"].append(float(info[sn_name][0]))
        metadata["dec"].append(float(info[sn_name][1]))
        metadata['redshift'].append(0)
        # Assuming all are SNe II. There may be unlabeled subtypes.
        metadata['spec_class'].append("SN II")

    num_examples = len(metadata['name'])

    return num_examples, data, metadata

def csp_dr3_bpf(file_dir, data, metadata, keys_data, keys_metadata, args):
    files = os.listdir(file_dir)
    num_examples = len(files) - 2 # tab1.dat and SN_photo.dat
    if args.tiny:
        num_examples=10
        files = files[:10]

    for file in files:
        if file in ('SN_photo.dat', 'tab1.dat'):
            continue
        current_filter = None
        data_ = dict(zip(keys_data, ([] for _ in keys_data)))
        f = open(os.path.join(file_dir, file), "r")
        for i, line in enumerate(f.readlines()):
            if i == 0:
                # Assuming all are SN Ia. There may be unlabeled subtypes.
                metadata['spec_class'].append('SN Ia')
                for key, val in zip(keys_metadata[:-1], line.split()):
                    if key in ('redshift', "ra", "dec"):
                        val = float(val)
                    metadata[key].append(val)
                continue
            if line.startswith("filter"):
                current_filter = line.split()[1]
                continue
            for key, val in zip(keys_data[:-1], line.split()):
                data_[key].append(float(val))
            data_["FLT"].append(current_filter)
        for key in keys_data:
            data[key].append(np.array(data_[key]))
        f.close()
    return num_examples, data, metadata


def snana_bpf(file_dir, data, metadata, keys_data, keys_metadata, args):
    files = os.listdir(file_dir)
    num_examples = len(files)

    if args.tiny:
        num_examples = 10
        files = files[:num_examples]

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
        for key in keys_metadata:
            if key in metadata_.keys(): metadata[key].append(metadata_[key])
            else: metadata[key].append(np.nan)
    return num_examples, data, metadata

SNANA_DATASETS = ('foundation', 'des_yr_sne_ia', 'snls', 'swift_sne_ia', 'ps1_sne_ia')
survey_specific_logic = {
        'cfa_snII': cfa_snII_bpf,
        'csp_dr3': csp_dr3_bpf,
}
for dataset in SNANA_DATASETS:
    survey_specific_logic[dataset] = snana_bpf

def main(args):
    # Retrieve file paths
    file_dir = args.data_path

    if args.dataset not in SNANA_DATASETS:
        keys_data = ["time", "mag", "mag_err", "FLT"]
        keys_metadata = ["name", "redshift", "ra", "dec", 'spec_class']
        data = dict(zip(keys_data, ([] for _ in keys_data)))
        metadata = dict(zip(keys_metadata, ([] for _ in keys_metadata)))
    else:
        # Load example data to determine keys in the dataset
        example_metadata, example_data = sncosmo.read_snana_ascii(os.path.join(file_dir, os.listdir(file_dir)[0]), default_tablename='OBS')
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
        data = dict(zip(keys_data, ([] for _ in keys_data)))
        metadata = dict(zip(keys_metadata, ([] for _ in keys_metadata)))
    num_examples, data, metadata = survey_specific_logic[args.dataset](file_dir, data, metadata, keys_data, keys_metadata, args)

    # Create an array of all bands in the dataset
    all_bands = np.unique(np.concatenate(data['FLT']))

    # Remove band from keys_data as the timeseries will be arranged by band
    keys_data.remove('FLT')

    for i in range(num_examples):
        # For this example, find the band with the most observations
        # and store the number of observations as max_length
        _, count = np.unique(data['FLT'][i], return_counts=True)
        max_length = count.max()

        # Create mask to select data from each timeseries by band
        mask = np.expand_dims(all_bands, 1) == data['FLT'][i]

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

    # Add numeric object_id to metadata (integer for each example in order of reading files)
    keys_metadata.append('object_id')
    metadata['object_id'] = np.arange(1, num_examples + 1)

    # Add healpix to metadata
    keys_metadata.append('healpix')
    if args.dataset in SNANA_DATASETS:
        metadata['healpix'] = hp.ang2pix(16, metadata['RA'], metadata['DECL'], lonlat=True, nest=True)
    else:
        metadata['healpix'] = hp.ang2pix(16, metadata['ra'], metadata['dec'], lonlat=True, nest=True)

    # Cast bands to required datatype
    all_bands = convert_dtype(all_bands)

    # Establish conversions to standard names
    keys_all = keys_metadata + keys_data
    name_conversion = dict(zip(keys_all, keys_all))
    if args.dataset in SNANA_DATASETS:
        name_conversion.update({
            'RA': 'ra',
            'DECL': 'dec',
            'MJD': 'time',
            'FLUXCAL': 'flux',
            'FLUXCALERR': 'flux_err',
        })

    # Make output directories labelled by healpix
    unique_healpix = np.unique(metadata['healpix'])
    healpix_num_digits = len(str(hp.nside2npix(16)))
    for healpix in unique_healpix:
        healpix = str(healpix).zfill(healpix_num_digits)
        os.makedirs(os.path.join(args.output_dir, f'healpix={healpix}'), exist_ok=True)

    # Save data as hdf5 grouped into directories by healpix
    object_id_num_digits = len(str(num_examples))
    for i in range(num_examples):
        healpix = str(metadata['healpix'][i]).zfill(healpix_num_digits)
        object_id = str(metadata['object_id'][i]).zfill(object_id_num_digits)
        path = os.path.join(args.output_dir, f'healpix={healpix}', f'example_{object_id}.hdf5')
        with h5py.File(path, 'w') as hdf5_file:
            # Save metadata
            for key in keys_metadata:
                hdf5_file.create_dataset(name_conversion[key], data=metadata[key][i])
            # Save bands
            hdf5_file.create_dataset('bands', data=all_bands)
            # Save timeseries
            for key in keys_data:
                hdf5_file.create_dataset(name_conversion[key], data=data[key][i])

    # Remove original data (data has now been reformatted and saved as hdf5)
    if not args.dirty:
        shutil.rmtree(args.data_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract data and convert to standard time-series data format.')
    parser.add_argument('data_path', type=str, help='Path to the local copy of the data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('dataset', type=str, help='Dataset to be prepared')
    parser.add_argument('--tiny', action="store_true", help='Use a small subset of the data for testing')
    parser.add_argument('--dirty', action="store_true", help='Do not remove the original data')
    args = parser.parse_args()

    main(args)
