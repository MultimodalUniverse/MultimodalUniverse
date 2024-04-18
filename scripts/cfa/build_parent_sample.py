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


def main(args):
    # Retrieve file paths
    file_dir = args.cfa_data_path

    # Load keys for SNooPy format
    keys_data = ["time", "mag", "mag_err", "FLT"]
    keys_metadata = ["object_id", "redshift", "ra", "dec", 'spec_class']
    data = dict(zip(keys_data, ([] for _ in keys_data)))
    metadata = dict(zip(keys_metadata, ([] for _ in keys_metadata)))

    info = {}
    with open("../../scripts/cfa/info.dat", "r") as f:
        for line in f.readlines():
            info["SN20" + line.split()[0]] = line.split()[1:]
    optical_df = pd.read_csv(
        os.path.join(file_dir, "STDSYSTEM_LC.txt"),
        comment="#",
        sep=r'\s+',
        names=["Name", "FLT", "time", "N", "mag", "mag_err"],
    )
    nir_df = pd.read_csv(
        os.path.join(file_dir, "NIR_LC.txt"),
        comment="#",
        sep=r'\s+',
        names=["Name", "FLT", "time", "mag", "mag_err"],
    )
    df = pd.concat([optical_df, nir_df])
    unique_names = set(df['Name'])
    if args.tiny:
        unique_names = list(unique_names)[:10]
    for sn_name in unique_names:
        mask = np.where(df["Name"] == sn_name)
        for key in keys_data:
            data[key].append(np.array(df[key])[mask])
        metadata["object_id"].append(sn_name)
        metadata["ra"].append(float(info[sn_name][0]))
        metadata["dec"].append(float(info[sn_name][1]))
        metadata['redshift'].append(0)
        # Assuming all are SNe II. There may be unlabeled subtypes.
        metadata['spec_class'].append("SN II")

    num_examples = len(metadata['object_id'])
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
    # keys_metadata.append('object_id')
    # metadata['object_id'] = np.arange(1, num_examples + 1)

    # Add healpix to metadata
    keys_metadata.append('healpix')
    metadata['healpix'] = hp.ang2pix(16, metadata['ra'], metadata['dec'], lonlat=True, nest=True)

    # Cast bands to required datatype
    all_bands = convert_dtype(all_bands)

    # Establish conversions to standard names
    keys_all = keys_metadata + keys_data
    name_conversion = dict(zip(keys_all, keys_all))
    # name_conversion.update({
    #     'RA': 'ra',
    #     'DECL': 'dec',
    #     'MJD': 'time',
    #     'FLUXCAL': 'flux',
    #     'FLUXCALERR': 'flux_err',
    # })

    # Make output directories labelled by healpix
    unique_healpix = np.unique(metadata['healpix'])
    healpix_num_digits = len(str(hp.nside2npix(16)))
    for healpix in unique_healpix:
        healpix = str(healpix).zfill(healpix_num_digits)
        os.makedirs(os.path.join(args.output_dir, f'healpix={healpix}'), exist_ok=True)

    # Save data as hdf5 grouped into directories by healpix
    # object_id_num_digits = len(str(num_examples))
    for i in range(num_examples):
        healpix = str(metadata['healpix'][i]).zfill(healpix_num_digits)
        object_id = metadata['object_id'][i].decode('utf-8') #.zfill(object_id_num_digits)
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
        shutil.rmtree(args.cfa_data_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract CfA data and convert to standard time-series data format.')
    parser.add_argument('cfa_data_path', type=str, help='Path to the local copy of the CfA data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--tiny', action="store_true", help='Use a small subset of the data for testing')
    parser.add_argument('--dirty', action="store_true", help='Do not remove the original data')
    args = parser.parse_args()

    main(args)
