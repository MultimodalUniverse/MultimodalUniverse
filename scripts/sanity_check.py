import h5py
import argparse
from glob import glob
import tqdm

def main(args):
    path = f'{args.sample_path}/*/*/*.hdf5'
    files = glob(path)
    failed_files = []
    for file in tqdm.tqdm(files):
        try:
            with h5py.File(file, 'r') as f:
                f['object_id'][:]
        except Exception as e:
            failed_files.append((file, e))
            print(f'Error in file {file}: {e}')
    print(f'Failed to load {len(failed_files)} files out of {len(files)}')
    print(failed_files)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Tests whether all hdf5 can be read correctly for a given sample.')
    parser.add_argument('sample_path', type=str, help='Path of the sample')
    args = parser.parse_args()
    main(args)
