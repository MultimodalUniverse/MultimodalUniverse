from pathlib import Path
import argparse
import numpy as np
from astropy.io import fits
from astropy.table import Table, join
from multiprocessing import Pool
from tqdm import tqdm
import h5py
import pandas as pd
import healpy as hp
import pdb

_healpix_nside = 16

def save_in_standard_format(args):
    """ This function iterates through an input metadata/lightcurve data pair and saves the data in a standard format.
    """
    metadata_path, lcdata_path, output_dir, tiny = args
    output_dir = Path(output_dir)

    metadata = pd.read_csv(metadata_path)
    lcdata = pd.read_csv(lcdata_path)

    # group by healpix
    metadata = metadata[metadata["object_id"].isin(lcdata["object_id"].unique())]
    metadata['healpix'] = hp.ang2pix(_healpix_nside, metadata['ra'].values, metadata['decl'].values, lonlat=True, nest=True)
    metadata = metadata.groupby('healpix')

    fname_split = Path(lcdata_path).name.split('_')
    dataset_type = fname_split[1] # train or test
    # append input file number if test, otherwise append 1 for train
    num = int(fname_split[3].split('.')[0]) if len(fname_split) == 4 else 1

    for i, (name, group) in enumerate(metadata):
        # process healpix 0 only if tiny
        if tiny and i > 0:
            break

        if i % 500 == 0:
            print(f"{dataset_type}:{num} - processing healpix {i}")

        group_filename = output_dir / f"healpix={name}" / f"{dataset_type}_{str(num).zfill(2)}.hdf5"
        if group_filename.exists():
            print(f"{group_filename} already exists, skipping...")
            return 1

        # Create the output directory if it does not exist
        if not group_filename.parent.exists():
            group_filename.parent.mkdir(parents=True)

        # find longest light curve
        lcdata_group = lcdata[lcdata["object_id"].isin(group["object_id"])]
        max_length = lcdata_group.groupby('object_id').size().max()

        objects = []
        for obj_metadata in group.itertuples():
            object_id = obj_metadata.object_id
            object_lcdata = lcdata_group[lcdata_group['object_id'] == object_id]
            # LC data has shape num_bands x 3 x seq_len (3 for mjd, flux, flux_err)
            lc = np.zeros((6, 3, max_length))
            for band in range(5):
                band_data = object_lcdata[object_lcdata['passband'] == band]
                lc[band, 0, :len(band_data)] = band_data['mjd']
                lc[band, 1, :len(band_data)] = band_data['flux']
                lc[band, 2, :len(band_data)] = band_data['flux_err']

            objects.append({
                'object_id': object_id,
                'ra': obj_metadata.ra,
                'dec': obj_metadata.decl,
                'hostgal_specz': obj_metadata.hostgal_specz,
                'hostgal_photoz': obj_metadata.hostgal_photoz,
                'redshift': obj_metadata.true_z,
                'obj_type': obj_metadata.true_target,
                'lightcurve': lc,
            })
        objects = Table({k: [o[k] for o in objects] for k in objects[0].keys()})
        assert len(group) == len(objects), f"Lost objects during preprocessing: {len(objects)} != {len(group)}"

        # Save all columns to disk in HDF5 format
        with h5py.File(group_filename, 'w') as hdf5_file:
            for key in objects.colnames:
                hdf5_file.create_dataset(key, data=objects[key])

    return 1

def download_plasticc_data(output_path, tiny=False):
    from urllib.request import urlretrieve

    if not Path(output_path).exists():
        Path(output_path).mkdir(parents=True)

    url_base = "https://zenodo.org/records/2539456/files/{}?download=1"
    urls = [
            url_base.format("plasticc_train_metadata.csv.gz"),
            url_base.format("plasticc_train_lightcurves.csv.gz"),
            url_base.format("plasticc_test_metadata.csv.gz"),
            url_base.format("plasticc_test_lightcurves_01.csv.gz"),
            url_base.format("plasticc_test_lightcurves_02.csv.gz"),
            url_base.format("plasticc_test_lightcurves_03.csv.gz"),
            url_base.format("plasticc_test_lightcurves_04.csv.gz"),
            url_base.format("plasticc_test_lightcurves_05.csv.gz"),
            url_base.format("plasticc_test_lightcurves_06.csv.gz"),
            url_base.format("plasticc_test_lightcurves_07.csv.gz"),
            url_base.format("plasticc_test_lightcurves_08.csv.gz"),
            url_base.format("plasticc_test_lightcurves_09.csv.gz"),
            url_base.format("plasticc_test_lightcurves_10.csv.gz"),
            url_base.format("plasticc_test_lightcurves_11.csv.gz"),
        ]
    if tiny:
        # only download training data if tiny
        print("Downloading tiny dataset (plasticc train only)")
        urls = urls[:2]

    for url in tqdm(urls):
        filename = url.split('/')[-1].split('?')[0]
        filepath = Path(output_path) / filename
        if filepath.exists():
            print(f"reading existing plasticc data from {filename}")
        else:
            print(f"no {filename} found, downloading ...")
            urlretrieve(url, (Path(output_path) / filename).resolve())
    print("done!")

def main(args):
    # Load PLAsTiCC data locally if exists, else download from Zenodo
    download_plasticc_data(args.plasticc_data_path, args.tiny)

    print("Rewriting training data into standard format...")
    # process training data
    save_in_standard_format((
        Path(args.plasticc_data_path) / "plasticc_train_metadata.csv.gz",
        Path(args.plasticc_data_path) / "plasticc_train_lightcurves.csv.gz",
        args.output_path,
        args.tiny
    ))

    if not args.tiny:
        # process test data
        print("Rewriting test data into standard format...")
        map_args = [[
            Path(args.plasticc_data_path) / "plasticc_test_metadata.csv.gz",
            Path(args.plasticc_data_path) / f"plasticc_test_lightcurves_{i:02d}.csv.gz",
            args.output_path,
            False
        ] for i in range(1, 12)]

        # Run the parallel processing
        with Pool(args.num_processes) as pool:
            results = list(tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args)))

        if sum(results) != len(map_args):
            print("There was an error in the parallel processing, some files may not have been processed correctly")

    # clean up the original data files
    print("Cleaning up original data files...")
    for i in range(1, 12):
        (Path(args.plasticc_data_path) / f"plasticc_test_lightcurves_{i:02d}.csv.gz").unlink(missing_ok=True)
    (Path(args.plasticc_data_path) / "plasticc_train_metadata.csv.gz").unlink(missing_ok=True)
    (Path(args.plasticc_data_path) / "plasticc_train_lightcurves.csv.gz").unlink(missing_ok=True)
    (Path(args.plasticc_data_path) / "plasticc_test_metadata.csv.gz").unlink(missing_ok=True)

    print("All done!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracts lightcurves and metadata from all PLAsTiCC data')
    parser.add_argument('plasticc_data_path', type=str, help='Path to the local copy of the PLAsTiCC data')
    parser.add_argument('output_path', type=str, help='Path to the output directory')
    parser.add_argument('--num_processes', type=int, default=10, help='The number of processes to use for parallel processing')
    parser.add_argument('--tiny', action='store_true', help='Use a tiny subset of the data for testing')
    args = parser.parse_args()

    main(args)
