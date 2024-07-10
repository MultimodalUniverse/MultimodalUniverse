import h5py
import numpy as np
import os
import argparse
from tqdm.contrib.concurrent import process_map

def transfer_extras(source_file_and_xp_file):
    source_file, xp_file = source_file_and_xp_file
    with h5py.File(xp_file, "r+") as f:
        with h5py.File(source_file, "r") as g:
            xp_id = f["source_id"][:]
            source_id = g["source_id"][:]
            _, xp_ix, source_ix = np.intersect1d(xp_id, source_id, assume_unique=True, return_indices=True)
            assert len(source_ix) == len(xp_id), "Source IDs do not match"
            assert np.all(np.diff(xp_ix) == 1), "XP IDs are not row matched"

            for key in ["ra", "dec"]:
                f.create_dataset(
                    key, data=g[key][:][source_ix]
                )


def main(args):
    source_files = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.startswith("GaiaSource") and f.endswith(".hdf5")
    ]
    xp_files = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.startswith("XpContinuousMeanSpectrum") and f.endswith(".hdf5")
    ]

    assert len(source_files) == len(
        xp_files
    ), "Number of source files and coefficient files do not match"
    source_files.sort()
    xp_files.sort()

    process_map(transfer_extras, zip(source_files, xp_files), max_workers=args.num_workers, chunksize=1, desc="Adding extra info to XP files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge Gaia data")
    parser.add_argument(
        "--input_dir", type=str, help="file containing split gaia data files"
    )
    parser.add_argument(
        "--num_workers",
        type=int,
        default=os.cpu_count(),
        help="number of workers to use for parallel processing",
    )
    args = parser.parse_args()
    main(args)
