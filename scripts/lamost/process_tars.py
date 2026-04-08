#!/usr/bin/env python3
"""
LAMOST Tar-to-HDF5 Processor (Phase 1)

Reads spectra directly from tar.gz archives (produced by download_data.py),
matches them against a LAMOST catalog, and writes one intermediate HDF5
per tar.gz into <output_dir>/_intermediate/.

Each intermediate HDF5 contains:
  - spectrum_flux:        (N, max_length) float32
  - spectrum_wavelength:  (N, max_length) float32
  - _catalog_indices:     (N,) int64  — row indices into the full catalog

Already-processed tars are skipped, making this script resume-safe.
Supports multi-node parallelism via --rank / --world_size, which shards
the list of tar.gz files across workers.

Example usage:
  # Single node:
  python process_tars.py catalog.fits /path/to/tars /path/to/output

  # Multi-node (e.g. 4 workers):
  python process_tars.py catalog.fits /path/to/tars /path/to/output --rank 0 --world_size 4
  python process_tars.py catalog.fits /path/to/tars /path/to/output --rank 1 --world_size 4
  ...
"""

import argparse
import gzip
import io
import os
import tarfile
from glob import glob

import h5py
import numpy as np
from astropy.io import fits
from astropy.table import Table
from tqdm import tqdm

# LAMOST wavelength coverage: 3700-9000 Å
max_length = 3908


def _parse_fits_bytes(raw):
    """Parse FITS data from raw bytes, returning (flux, wavelength) arrays."""
    with fits.open(io.BytesIO(raw)) as hdulist:
        binaryext = hdulist[1].data
        x = binaryext["FLUX"].astype(np.float32)
        wv = binaryext["WAVELENGTH"].astype(np.float32)

        if x.shape[1] > max_length:
            x = x[:, :max_length]
        elif x.shape[1] < max_length:
            x = np.pad(
                x,
                ((0, 0), (0, max_length - x.shape[1])),
                mode="constant",
                constant_values=0,
            )

        if wv.shape[1] > max_length:
            wv = wv[:, :max_length]
        elif wv.shape[1] < max_length:
            wv = np.pad(
                wv,
                ((0, 0), (0, max_length - wv.shape[1])),
                mode="constant",
                constant_values=0,
            )

    return x, wv


def _obsid_from_member(member_name):
    """Extract obsid string from a tar member path like 'dir/12345.fits.gz'."""
    basename = os.path.basename(member_name)
    if basename.endswith(".fits.gz"):
        return basename[: -len(".fits.gz")]
    elif basename.endswith(".fits"):
        return basename[: -len(".fits")]
    return None


def process_tar_to_hdf5(tar_path, obsid_to_idx, output_path):
    """
    Stream through a tar.gz, extract spectra matching the catalog, and write
    an intermediate HDF5.

    Args:
        tar_path: Path to the tar.gz archive.
        obsid_to_idx: dict mapping obsid (str) -> catalog row index.
        output_path: Where to write the intermediate HDF5.

    Returns:
        Number of spectra successfully written.
    """
    found_obsids = []
    found_flux = []
    found_wv = []

    try:
        with tarfile.open(tar_path, "r:gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                obsid = _obsid_from_member(member.name)
                if obsid is None or obsid not in obsid_to_idx:
                    continue
                try:
                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    raw = f.read()
                    if member.name.endswith(".gz"):
                        raw = gzip.decompress(raw)
                    flux, wv = _parse_fits_bytes(raw)
                    found_obsids.append(obsid)
                    found_flux.append(flux)
                    found_wv.append(wv)
                except Exception as e:
                    print(f"  Error reading {obsid} from {os.path.basename(tar_path)}: {e}")
    except Exception as e:
        print(f"Error opening {tar_path}: {e}")
        return 0

    if not found_obsids:
        return 0

    row_indices = [obsid_to_idx[oid] for oid in found_obsids]

    with h5py.File(output_path, "w") as hf:
        hf.create_dataset("spectrum_flux", data=np.vstack(found_flux))
        hf.create_dataset("spectrum_wavelength", data=np.vstack(found_wv))
        hf.create_dataset("_catalog_indices", data=np.array(row_indices, dtype=np.int64))

    return len(found_obsids)


def main():
    parser = argparse.ArgumentParser(
        description="Process LAMOST tar.gz archives into intermediate HDF5 files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Writes one HDF5 per tar.gz into <output_dir>/_intermediate/. Already-processed
tars are skipped (resume-safe). Use --rank/--world_size to shard across nodes.

Example:
  python process_tars.py dr11_v2.0_LRS_stellar.fits ./lamost_tars ./output
  python process_tars.py dr11_v2.0_LRS_stellar.fits ./lamost_tars ./output --rank 0 --world_size 4
        """,
    )
    parser.add_argument(
        "catalog_path", type=str, help="Path to LAMOST catalog file (FITS format)",
    )
    parser.add_argument(
        "lamost_data_path", type=str,
        help="Path to directory containing LAMOST tar.gz files (from download_data.py)",
    )
    parser.add_argument(
        "output_dir", type=str,
        help="Output directory (intermediates written to <output_dir>/_intermediate/)",
    )
    parser.add_argument(
        "--tiny", action="store_true",
        help="Process only the first 50 catalog rows for testing",
    )
    parser.add_argument(
        "--rank", type=int, default=0,
        help="Worker rank for multi-node processing (default: 0)",
    )
    parser.add_argument(
        "--world_size", type=int, default=1,
        help="Total number of workers for multi-node processing (default: 1)",
    )

    args = parser.parse_args()

    if args.rank < 0 or args.rank >= args.world_size:
        parser.error(
            f"--rank must be in [0, {args.world_size - 1}] for --world_size={args.world_size}"
        )

    print(f"Loading catalog from {args.catalog_path}")
    try:
        catalog = Table.read(args.catalog_path, hdu=1)
    except Exception as e:
        print(f"Error reading catalog: {e}")
        return

    if args.tiny:
        catalog = catalog[:50]
        print(f"Using tiny subset of {len(catalog)} objects")

    if len(catalog) == 0:
        print("No objects found in the catalog.")
        return

    obsid_to_idx = {str(row["obsid"]): i for i, row in enumerate(catalog)}

    tar_files = sorted(glob(os.path.join(args.lamost_data_path, "*.tar.gz")))

    if args.world_size > 1:
        total = len(tar_files)
        tar_files = tar_files[args.rank :: args.world_size]
        print(f"[rank {args.rank}/{args.world_size}] Processing {len(tar_files)} of {total} tar files")
    else:
        print(f"Found {len(tar_files)} tar.gz files")

    intermediate_dir = os.path.join(args.output_dir, "_intermediate")
    os.makedirs(intermediate_dir, exist_ok=True)

    total_spectra = 0
    total_files = 0

    for tar_path in tqdm(tar_files, desc="Processing tar files"):
        date_stem = os.path.basename(tar_path).replace(".tar.gz", "")
        out_hdf5 = os.path.join(intermediate_dir, f"{date_stem}.hdf5")

        if os.path.exists(out_hdf5):
            with h5py.File(out_hdf5, "r") as hf:
                total_spectra += len(hf["_catalog_indices"])
            total_files += 1
            continue

        n = process_tar_to_hdf5(tar_path, obsid_to_idx, out_hdf5)
        if n > 0:
            total_spectra += n
            total_files += 1

    print(f"Done: {total_spectra} spectra across {total_files} intermediate files")


if __name__ == "__main__":
    main()
