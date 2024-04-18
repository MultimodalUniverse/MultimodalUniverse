import argparse
import h5py
import numpy as np
import pandas as pd
import os
import shutil
import sncosmo
import healpy as hp
from astropy.table import Table
from astropy.io.misc.hdf5 import write_table_hdf5
from tqdm import tqdm


def main(args):
    # Retrieve file paths
    file_dir = args.btsbot_data_path
    files = os.listdir(file_dir)
    num_examples = len(files)

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    # Hardcoding this because it should never change
    img_file_paths = ['test_triplets_v10_N100_programid1.npy', 'val_triplets_v10_N100_programid1.npy',
                 'train_triplets_v10_N100_programid1.npy']
    meta_file_paths = ['test_cand_v10_N100_programid1.csv', 'val_cand_v10_N100_programid1.csv',
                  'train_cand_v10_N100_programid1.csv']

    # Load meta files first to work out healpix values
    meta_files = []
    for file in meta_file_paths:
        meta_data = pd.read_csv(os.path.join(file_dir, file))
        meta_data['healpix'] = hp.ang2pix(16, meta_data['ra'], meta_data['dec'], lonlat=True, nest=True)
        meta_files.append(meta_data)

    # Load images as array but stored on disk with memmap, otherwise you'll probably run out of memory
    img_files = []
    for img_file_path in img_file_paths:
        img_file = np.load(os.path.join(file_dir, img_file_path), mmap_mode='r')
        img_files.append(img_file)

    # Work out all unique healpix
    all_healpix = []
    for file in meta_files:
        all_healpix = all_healpix + list(file['healpix'].values)

    unique_healpix = np.sort(np.unique(all_healpix))

    healpix_num_digits = len(str(hp.nside2npix(16)))
    # Loop over individual healpix values, can't think of a better way to do this which won't take loads of memory
    for healpix in tqdm(unique_healpix):
        hp_metas, hp_imgs = [], []
        for img_file, meta_file in zip(img_files, meta_files):
            hp_meta = meta_file[meta_file.healpix == healpix]
            hp_img = img_file[hp_meta.index, ...]
            hp_metas.append(hp_meta)
            hp_imgs.append(hp_img)
        hp_meta_all = pd.concat(hp_metas)
        hp_img_all = np.concatenate(hp_imgs)

        healpix = str(healpix).zfill(healpix_num_digits)
        os.makedirs(os.path.join(args.output_dir, f'healpix={healpix}'), exist_ok=True)

        hp_meta_all.rename(columns={'candid': 'object_id', 'objectId': 'OBJECT_ID_'}, inplace=True)
        hp_table = Table.from_pandas(hp_meta_all)
        hp_table['image_triplet'] = hp_img_all

        """
        write_table_hdf5(hp_table, os.path.join(args.output_dir, f'healpix={healpix}', '001-of-001.hdf5'),
                         path='table', serialize_meta=True)
        """
        with h5py.File(os.path.join(args.output_dir, f'healpix={healpix}', '001-of-001.hdf5'), 'w') as hdf5_file:
            for key in hp_table.colnames:
                dtype = hp_table[key].dtype
                if np.issubdtype(dtype, np.str_):
                    str_max_len = int(str(dtype)[2:])
                    dtype = h5py.string_dtype(encoding='utf-8', length=str_max_len)
                    hdf5_file.create_dataset(key, data=hp_table[key].astype(dtype))
                else:
                    hdf5_file.create_dataset(key, data=hp_table[key])

    # Remove original data (data has now been reformatted and saved as hdf5)
    if not args.dirty:
        shutil.rmtree(args.btsbot_data_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract BTSbot data and save into healpix files')
    parser.add_argument('btsbot_data_path', type=str, help='Path to the local copy of the BTSbot data',
                        default='./data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory', default='./')
    parser.add_argument('--dirty', action="store_true", help='Do not remove the original data')
    args = parser.parse_args()

    main(args)
