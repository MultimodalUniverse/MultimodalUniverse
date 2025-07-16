from astropy.table import Table
import numpy as np
import argparse
import healpy as hp
import h5py
import os
import multiprocessing
from tqdm.auto import tqdm
import uuid

REMAP = dict(
    objid="object_id",
)

def ang2pix(ra, dec):
    return hp.ang2pix(nside=args.nside, theta=ra, phi=dec, lonlat=True, nest=True)

def process_one_group(group, subset, output_dir):
    write_dir = os.path.join(output_dir, f"healpix={group}")
    os.makedirs(write_dir, exist_ok=True)
    write_path = os.path.join(write_dir, f"{uuid.uuid4()}.hdf5")
    with h5py.File(write_path, 'w') as f:
        for col in subset.colnames:
            if col in REMAP:
                f.create_dataset(REMAP[col], data=subset[col])
            else:
                f.create_dataset(col, data=subset[col])


def process_one_file(fname):
    t = Table.read(fname)
    t.rename_columns(t.colnames, list(map(lambda x: x.lower(), t.colnames)))
    healpix = ang2pix(t['ra'], t['dec'])
    t['healpix'] = healpix
    t_grouped = t.group_by("healpix")
    groups = t_grouped.groups.keys['healpix'].data

    with multiprocessing.Pool(os.cpu_count()) as pool:
        pool.starmap(process_one_group, tqdm([(g, t, args.output_dir) for (g, t) in zip(groups, t_grouped.groups)], total=len(groups), desc=f'{fname} | {len(t)} rows', leave=False))


def main(args):
    files = [os.path.join(args.input_dir, f) for f in os.listdir(args.input_dir) if f.endswith('.fits.gz')]
    if args.tiny:
        files = files[:1]

    for f in tqdm(files):
        process_one_file(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str, help="Directory containing input files")
    parser.add_argument("--output_dir", type=str, help="Directory to write output files")
    parser.add_argument("--nside", type=int, help="Healpix nside", default=16)
    parser.add_argument("--tiny", action="store_true", help="Use a tiny subset of the data")
    args = parser.parse_args()
    main(args)
