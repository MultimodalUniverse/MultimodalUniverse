import os

import healpy as hp
import pyarrow.parquet as pq
from tqdm.contrib.concurrent import process_map


def ang2pix(ra, dec):
    return hp.ang2pix(nside=args.nside, theta=ra, phi=dec, lonlat=True, nest=True)


def process_fragment(fragment):
    table = fragment.to_table()
    hpix = ang2pix(table["ra"], table["dec"])
    table = table.append_column("healpix", [hpix])
    table = table.append_column("object_id", table["cntr"])
    pq.write_to_dataset(table, args.output_dir, partition_cols=["healpix"])
    return 1


def main(args):
    ds = pq.ParquetDataset(args.input_dir)
    tables = process_map(
        process_fragment, ds.fragments, max_workers=os.cpu_count(), chunksize=1
    )
    assert all(tables), "Some tables failed to process"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_dir",
        type=str,
        help="Path to the local data directory",
    )
    parser.add_argument("--output_dir", type=str, help="Path to the output directory")
    parser.add_argument("--nside", type=int, help="nside for healpix")
    args = parser.parse_args()

    main(args)
