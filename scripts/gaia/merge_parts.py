import h5py
import numpy as np
import os
import argparse
from tqdm.auto import tqdm


def main(args):
    source_files = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.startswith("GaiaSource") and f.endswith(".hdf5")
    ]
    coeff_files = [
        os.path.join(args.input_dir, f)
        for f in os.listdir(args.input_dir)
        if f.startswith("XpContinuousMeanSpectrum") and f.endswith(".hdf5")
    ]

    assert len(source_files) == len(
        coeff_files
    ), "Number of source files and coefficient files do not match"
    source_files.sort()
    coeff_files.sort()

    n_files = len(source_files)
    n_rows = 0

    for i in range(n_files):
        with h5py.File(source_files[i], "r") as f:
            n_rows += f["source_id"].shape[0]

    out = h5py.File(args.output_file, "w")

    for i in tqdm(range(n_files)):
        with h5py.File(source_files[i], "r") as f:
            batch_size = f["source_id"].shape[0]
            if i == 0:
                for k in f.keys():
                    out.create_dataset(
                        k,
                        shape=(n_rows,) + f[k].shape[1:],
                        dtype=f[k].dtype,
                        maxshape=(None,) + f[k].shape[1:],
                    )
            for k in f.keys():
                out[k][i * batch_size : (i + 1) * batch_size] = f[k][:]
        with h5py.File(coeff_files[i], "r") as f:
            batch_size = f["source_id"].shape[0]
            if i == 0:
                out.create_dataset(
                    "coeff", shape=(n_rows, 110), dtype=np.float32, maxshape=(None, 110)
                )
                out.create_dataset(
                    "coeff_error",
                    shape=(n_rows, 110),
                    dtype=np.float32,
                    maxshape=(None, 110),
                )
            out["coeff"][i * batch_size : (i + 1) * batch_size] = np.concatenate(
                (f["bp_coefficients"][:], f["rp_coefficients"][:]), axis=-1
            )
            out["coeff_error"][i * batch_size : (i + 1) * batch_size] = np.concatenate(
                (f["bp_coefficient_errors"][:], f["rp_coefficient_errors"][:]), axis=-1
            )

    out.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge Gaia data")
    parser.add_argument(
        "--input_dir", type=str, help="file containing split gaia data files"
    )
    parser.add_argument("--output_file", type=str, help="output file")
    args = parser.parse_args()
    main(args)
