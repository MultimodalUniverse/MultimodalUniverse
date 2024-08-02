import healpy as hp
import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import h5py
from tqdm.contrib.concurrent import process_map
import os
import glob
from functools import partial


def ang2pix(ra, dec, nside):
    return hp.ang2pix(nside=nside, theta=ra, phi=dec, lonlat=True, nest=True)


def hdf5_to_parquet(hdf5_file, output_dir, nside):
    with h5py.File(hdf5_file, 'r') as f:
        table = pa.Table.from_pydict({k: list(f[k][:]) for k in f.keys()})
        table = table.append_column('healpix', [ang2pix(table['ra'], table['dec'], nside)])
        pq.write_to_dataset(table, output_dir, partition_cols=['healpix'])

def main(args):
    source_files = sorted(glob.glob(os.path.join(args.input_dir, 'GaiaSource*.hdf5')))
    xp_files = sorted(glob.glob(os.path.join(args.input_dir, 'XpContinuousMeanSpectrum*.hdf5')))
    rvs_files = sorted(glob.glob(os.path.join(args.input_dir, 'RvsMeanSpectrum*.hdf5')))
    # AstrophysicalParameters
    ap_files = sorted(glob.glob(os.path.join(args.input_dir, 'AstrophysicalParameters*.hdf5')))

    source_dir = os.path.join(args.output_dir, "dr3_source")
    xp_dir = os.path.join(args.output_dir, "dr3_xp")
    rvs_dir = os.path.join(args.output_dir, "dr3_rvs")
    ap_dir = os.path.join(args.output_dir, "dr3_ap")

    os.makedirs(source_dir, exist_ok=True)
    os.makedirs(xp_dir, exist_ok=True)
    os.makedirs(rvs_dir, exist_ok=True)
    os.makedirs(ap_dir, exist_ok=True)

    hdf5_to_parquet_partial = partial(hdf5_to_parquet, nside=args.nside)

    process_map(partial(hdf5_to_parquet_partial, output_dir=source_dir), source_files, max_workers=args.num_workers, chunksize=1, desc="Building GaiaSource")
    process_map(partial(hdf5_to_parquet_partial, output_dir=xp_dir), xp_files, max_workers=args.num_workers, chunksize=1, desc="Building XP")
    process_map(partial(hdf5_to_parquet_partial, output_dir=rvs_dir), rvs_files, max_workers=args.num_workers, chunksize=1, desc="Building RVS")
    process_map(partial(hdf5_to_parquet_partial, output_dir=ap_dir), ap_files, max_workers=args.num_workers, chunksize=1, desc="Building AP")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert HDF5 files to Parquet')
    parser.add_argument('--input_dir', type=str, help='Directory with input HDF5 files')
    parser.add_argument('--output_dir', type=str, help='Output directory')
    parser.add_argument('--nside', type=int, help='Healpix nside', default=16)
    parser.add_argument('--num_workers', type=int, help='Number of workers', default=os.cpu_count())
    args = parser.parse_args()
    main(args)
