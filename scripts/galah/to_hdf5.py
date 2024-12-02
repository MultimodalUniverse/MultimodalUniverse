import argparse
import multiprocessing
import os

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm.auto import tqdm


def parquetfrag_to_hdf5(frags, filename):
    table = pa.concat_tables([frag.to_table() for frag in frags])
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with h5py.File(filename, "w") as f:
        for col in table.column_names:
            d = np.stack(table[col].to_numpy())
            try:
                if isinstance(type(d[0]), bytes):
                    d = d.astype(str)
                f.create_dataset(name=col, data=d)
            except Exception as e:
                print(f"Error with column {col}, dtype {d.dtype}: {e}")
                raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()

    pqd = pq.ParquetDataset(args.data_dir)

    filenames = []
    for f in pqd.files:
        filedir = os.path.join(
            args.output_dir, os.path.dirname(f.replace(args.data_dir, "").lstrip("/"))
        )
        os.makedirs(filedir, exist_ok=True)
        filename = os.path.join(filedir, "001-of-001.hdf5")
        filenames.append(filename)

    map_args = {filename: [] for filename in filenames}
    for i, filename in enumerate(filenames):
        map_args[filename].append(i)

    pool = multiprocessing.Pool(os.cpu_count())
    results = []
    for filename, idxs in map_args.items():
        results.append(
            pool.apply_async(
                parquetfrag_to_hdf5, args=([pqd.fragments[i] for i in idxs], filename)
            )
        )
    for r in tqdm(results):
        r.get()
    pool.close()
    pool.join()
