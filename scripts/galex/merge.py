import argparse
import h5py
import os
from tqdm.contrib.concurrent import process_map
import numpy as np

def merge_one_dir(dir):
    out_path = os.path.join(dir, "001-of-001.hdf5")
    if os.path.exists(out_path):
        os.remove(out_path)
    fnames = [os.path.join(dir, f) for f in os.listdir(dir)]
    if len(fnames) == 1:
        os.rename(fnames[0], out_path)
    else:
        with h5py.File(fnames[0], "r") as f2:
            all_keys = list(f2.keys())
        with h5py.File(out_path, "w") as fout:
            for k in all_keys:
                temp_arr = []
                for i, fname in enumerate(fnames):
                    with h5py.File(fname, "r") as f:
                        temp_arr.append(f[k][:])
                temp_arr = np.concatenate(temp_arr, axis=0)
                fout.create_dataset(k, data=temp_arr)
        

def main(args):
    dirs = [os.path.join(args.input_dir, d) for d in os.listdir(args.input_dir) if os.path.isdir(os.path.join(args.input_dir, d)) and "healpix" in d]
    process_map(merge_one_dir, dirs, max_workers=os.cpu_count())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge multiple HDF5 files into one.")
    parser.add_argument("--input_dir", type=str, help="Path to directory containing HDF5 files.")
    args = parser.parse_args()
    main(args)
