import argparse
import multiprocessing
import os

import h5py
import pyarrow.parquet as pq
from tqdm.auto import tqdm


def parquetfrag_to_hdf5(frag, filename):
    table = frag.to_table()
    with h5py.File(filename, "w") as f:
        for col in table.column_names:
            f.create_dataset(name=col, data=table[col].to_numpy(), compression=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    pqd = pq.ParquetDataset(args.data_dir)

    filenames = []
    for f in pqd.files:
        filedir = os.path.join(args.output_dir, "/".join(f.split("/")[1:-1]))
        os.makedirs(filedir, exist_ok=True)
        filename = os.path.join(filedir, "001-of-001.hdf5")
        filenames.append(filename)

    pool = multiprocessing.Pool(8)
    results = []
    for frag, filename in tqdm(zip(pqd.fragments, filenames), total=len(filenames)):
        results.append(pool.apply_async(parquetfrag_to_hdf5, args=(frag, filename)))
    for r in tqdm(results):
        r.get()
    pool.close()
    pool.join()
