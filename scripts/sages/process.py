import os
from astropy.table import Table
import healpy as hp
import numpy as np
import argparse
import h5py
import multiprocessing as mp
from tqdm.auto import tqdm

REMAP = dict(
    SAGE_ID="object_id",
    RA="ra",
    DEC="dec",
)

def ang2pix(ra, dec):
    return hp.ang2pix(nside=args.nside, theta=ra, phi=dec, lonlat=True, nest=True)

def save_one_group(t, group, output_dir):
    t_group = t[t['healpix'] == group]
    write_dir = os.path.join(output_dir, f"healpix={group}")
    os.makedirs(write_dir, exist_ok=True)
    write_path = os.path.join(write_dir, "001-of-001.hdf5")
    with h5py.File(write_path, "w") as f:
        for col in t_group.colnames:
            if col in REMAP:
                f.create_dataset(REMAP[col], data=t_group[col])
            else:
                f.create_dataset(col, data=t_group[col])
    


def main(args):
    t = Table.read(os.path.join(args.input_dir, "dr1-uv.fits"))
    if args.tiny:
        t = t[:100]
    mask = (t['MAG_U'] > -999) & (t['MAG_V'] > -999) & (t['FLAG_U'] == 0) & (t['FLAG_V'] == 0)
    t = t[mask]
    healpix = ang2pix(t['RA'], t['DEC'])
    t['healpix'] = healpix

    hp_groups = np.unique(healpix)

    with mp.Pool() as pool:
        pool.starmap(save_one_group, tqdm([(t, group, args.output_dir) for group in hp_groups]))



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str, help="Input dir")
    parser.add_argument("--output_dir", type=str, help="Output dir")
    parser.add_argument("--nside", type=int, help="healpix nside", default=16)
    parser.add_argument("--tiny", action="store_true", default=False, help="Run with small subset of data for testing")
    args = parser.parse_args()
    main(args)
