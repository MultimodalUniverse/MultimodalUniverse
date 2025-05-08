import h5py
import numpy as np
import os
import argparse
from tqdm.contrib.concurrent import process_map

def transfer_extras(source_file_and_dest_file):
    source_file, dest_file = source_file_and_dest_file
    with h5py.File(dest_file, "r+") as f:
        with h5py.File(source_file, "r") as g:
            dest_id = f["source_id"][:]
            source_id = g["source_id"][:]
            _, dest_ix, source_ix = np.intersect1d(dest_id, source_id, assume_unique=True, return_indices=True)
            assert len(source_ix) == len(dest_id), "Source IDs do not match"
            assert np.all(np.diff(dest_ix) == 1), "Destination IDs are not row matched"

            for key in ["ra", "dec"]:
                if key not in f.keys():
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
    ap_files = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.startswith("AstrophysicalParameters") and f.endswith(".hdf5")
    ]

    assert len(source_files) == len(
        xp_files
    ), "Number of source files and coefficient files do not match"
    assert len(source_files) == len(
        ap_files
    ), "Number of source files and astrophysical parameter files do not match"
    source_files.sort()
    xp_files.sort()
    ap_files.sort()

    process_map(transfer_extras, zip(source_files, xp_files), max_workers=args.num_workers, chunksize=1, desc="Adding extra info to XP files", total=len(source_files))
    process_map(transfer_extras, zip(source_files, ap_files), max_workers=args.num_workers, chunksize=1, desc="Adding extra info to AP files", total=len(source_files))


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
