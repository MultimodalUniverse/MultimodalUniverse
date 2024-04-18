import argparse
import numpy as np
import pandas as pd
import os
import shutil


def main(args):
    # Retrieve file paths
    file_dir = args.btsbot_data_path

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)
    os.mkdir(os.path.join(args.output_dir, 'images'))
    os.mkdir(os.path.join(args.output_dir, 'meta'))

    # Hardcoding this because it should never change
    img_file_paths = ['test_triplets_v10_N100_programid1.npy', 'val_triplets_v10_N100_programid1.npy',
                 'train_triplets_v10_N100_programid1.npy']
    meta_file_paths = ['test_cand_v10_N100_programid1.csv', 'val_cand_v10_N100_programid1.csv',
                  'train_cand_v10_N100_programid1.csv']

    # Load meta files first to work out healpix values
    meta_files = []
    for file in meta_file_paths:
        meta_data = pd.read_csv(os.path.join(file_dir, file))
        meta_files.append(meta_data)

    # Load images as array but stored on disk with memmap, otherwise you'll probably run out of memory
    img_files = []
    for img_file_path in img_file_paths:
        img_file = np.load(os.path.join(file_dir, img_file_path), mmap_mode='r')
        img_files.append(img_file)

    # Save tiny versions
    num_examples = 3
    for i in range(3):
        img_file = img_files[i][:num_examples, ...]
        np.save(os.path.join(args.output_dir, 'images', img_file_paths[i]), img_file)
    shutil.make_archive(os.path.join(args.output_dir, 'images_v10'), format='zip',
                        root_dir=os.path.join(args.output_dir, 'images'), base_dir='./')
    shutil.rmtree(os.path.join(args.output_dir, 'images'))

    for i in range(3):
        meta_file = meta_files[i].head(num_examples)
        meta_file.to_csv(os.path.join(args.output_dir, 'meta', meta_file_paths[i]), index=False)
    shutil.make_archive(os.path.join(args.output_dir, 'meta_v10'), format='zip',
                        root_dir=os.path.join(args.output_dir, 'meta'), base_dir='./')
    shutil.rmtree(os.path.join(args.output_dir, 'meta'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make tiny version of BTSbot download for test.sh, full download '
                                                 'is 30GB and will take too long for testing')
    parser.add_argument('btsbot_data_path', type=str, help='Path to the local copy of the BTSbot data',
                        default='./data')
    parser.add_argument('output_dir', type=str, help='Path to the output directory', default='./data_tiny')
    args = parser.parse_args()

    main(args)
