from pathlib import Path
import argparse
import numpy as np
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import h5py
import pandas as pd

def save_in_standard_format(args):
    """ This function iterates through an input metadata/lightcurve data pair and saves the data in a standard format.
    """
    metadata, lcdata, output_filename = args

    # Create the output directory if it does not exist
    if not Path(output_filename).parent.exists():
        Path(output_filename).parent.mkdir(parents=True)

    # find longest light curve
    max_length = lcdata.groupby('object_id').size().max()

    # process this file
    ids = np.unique(lcdata['object_id'])
    for object_id in ids:
        obj_metadata = metadata[metadata['object_id'] == object_id]
        final = lcdata[lcdata['object_id'] == object_id]
        final.drop('detected_bool', axis=1, inplace=True)

        padding_df = pd.DataFrame(np.zeros((max_length - len(final), len(final.columns))), columns=final.columns)
        padding_df['object_id'] = object_id
        final = pd.concat([final, padding_df])
        final['ra'] = obj_metadata['ra'].values[0]
        final['dec'] = obj_metadata['decl'].values[0]
        final['hostgal_specz'] = obj_metadata['hostgal_specz'].values[0]
        final['hostgal_photoz'] = obj_metadata['hostgal_photoz'].values[0]
        final['redshift'] = obj_metadata['true_z'].values[0]
        final['obj_type'] = obj_metadata['true_target'].values[0]

    # Save all columns to disk in HDF5 format
    with h5py.File(output_filename, 'w') as hdf5_file:
        for key in final.colnames:
            hdf5_file.create_dataset(key, data=final[key])
    return 1

def download_plasticc_data(output_path):
    from urllib.request import urlretrieve

    url_base = "https://zenodo.org/records/2539456/files/{}?download=1",
    urls = [
            url_base.format("plasticc_train_metadata.csv.gz"),
            url_base.format("plasticc_train_lightcurves.csv.gz"),
            url_base.format("plasticc_test_metadata.csv.gz"),
            url_base.format("plasticc_test_metadata_01.csv.gz"),
            url_base.format("plasticc_test_metadata_02.csv.gz"),
            url_base.format("plasticc_test_metadata_03.csv.gz"),
            url_base.format("plasticc_test_metadata_04.csv.gz"),
            url_base.format("plasticc_test_metadata_05.csv.gz"),
            url_base.format("plasticc_test_metadata_06.csv.gz"),
            url_base.format("plasticc_test_metadata_07.csv.gz"),
            url_base.format("plasticc_test_metadata_08.csv.gz"),
            url_base.format("plasticc_test_metadata_09.csv.gz"),
            url_base.format("plasticc_test_metadata_10.csv.gz"),
            url_base.format("plasticc_test_metadata_11.csv.gz"),
        ]
    for url in urls:
        filename = url.split('/')[-1].split('?')[0]
        urlretrieve(url, (Path(output_path) / filename).resolve())

def main(args):
    # Load PLAsTiCC data locally if exists, else download from Zenodo
    if Path(args.plasticc_data_path).exists():
        print(f"reading existing plasticc data from {args.plasticc_data_path}")
    else:
        print(f"downloading plasticc data to {args.plasticc_data_path}...")
        Path(args.plasticc_data_path).parent.mkdir(parents=True, exist_ok=True)
        download_plasticc_data(args.plasticc_data_path)
        print("done!")

    # process training data
    metadata = pd.read_csv(Path(args.plasticc_data_path) / "plasticc_train_metadata.csv.gz")
    lcdata = pd.read_csv(Path(args.plasticc_data_path) / "plasticc_train_lightcurves.csv.gz")
    save_in_standard_format((metadata, lcdata, Path(args.output_path) / "plasticc_train.hdf5"))

    # process test data
    map_args = [[metadata, pd.read_csv(Path(args.plasticc_data_path) / f"plasticc_test_lightcurves_{i:02d}.csv.gz"), Path(args.output_path) / f"plasticc_test_{i:02d}.hdf5"] for i in range(1, 12)]

    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args)))

    if sum(results) != len(map_args):
        print("There was an error in the parallel processing, some files may not have been processed correctly")

    print("All done!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts lightcurves and metadata from all PLAsTiCC data')
    parser.add_argument('plasticc_data_path', type=str, help='Path to the local copy of the PLAsTiCC data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    args = parser.parse_args()

    main(args)
