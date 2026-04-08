#!/usr/bin/env python3
"""
LAMOST Parent Sample Builder (Phase 2)

Reads the intermediate HDF5 files produced by process_tars.py, computes
healpix indices, and reshuffles spectra into the final healpix-grouped
directory structure:
    <output_dir>/<catalog_name>/healpix=<N>/001-of-001.hdf5

This should be run once after all process_tars.py workers have finished.

Example usage:
  python build_parent_sample.py catalog.fits /path/to/output
"""

import argparse
import os
from glob import glob

import h5py
import healpy as hp
import numpy as np
from astropy.table import Table, hstack
from tqdm import tqdm

_healpix_nside = 16


def main(args):
    catalog_path = args.catalog_path
    catalog_name = catalog_path.replace(".fits", "").lower().replace(".", "")

    print(f"Loading catalog from {catalog_path}")
    try:
        catalog = Table.read(catalog_path, hdu=1)
    except Exception as e:
        print(f"Error reading catalog: {e}")
        return

    if args.tiny:
        catalog = catalog[:50]
        print(f"Using tiny subset of {len(catalog)} objects")

    if len(catalog) == 0:
        print("No objects found in the catalog.")
        return

    if "object_id" not in catalog.colnames:
        catalog["object_id"] = catalog["obsid"]
    if "radial_velocity" not in catalog.colnames and "rv" in catalog.colnames:
        catalog["radial_velocity"] = catalog["rv"]
    catalog["restframe"] = np.ones(len(catalog), dtype=bool)

    # Discover intermediate files
    intermediate_dir = os.path.join(args.output_dir, "_intermediate")
    intermediate_files = sorted(glob(os.path.join(intermediate_dir, "*.hdf5")))

    if not intermediate_files:
        print(f"No intermediate HDF5 files found in {intermediate_dir}")
        print("Run process_tars.py first.")
        return

    print(f"Found {len(intermediate_files)} intermediate files")

    # Read all intermediates
    all_catalog_indices = []
    all_flux = []
    all_wv = []

    for hdf5_path in tqdm(intermediate_files, desc="Reading intermediate files"):
        with h5py.File(hdf5_path, "r") as hf:
            all_catalog_indices.append(hf["_catalog_indices"][:])
            all_flux.append(hf["spectrum_flux"][:])
            all_wv.append(hf["spectrum_wavelength"][:])

    all_catalog_indices = np.concatenate(all_catalog_indices)
    all_flux = np.concatenate(all_flux)
    all_wv = np.concatenate(all_wv)

    print(f"Loaded {len(all_catalog_indices)} spectra from intermediate files")

    matched_catalog = catalog[all_catalog_indices]
    matched_catalog["healpix"] = hp.ang2pix(
        _healpix_nside, matched_catalog["ra"], matched_catalog["dec"],
        lonlat=True, nest=True,
    )

    order = np.argsort(matched_catalog["healpix"])
    matched_catalog = matched_catalog[order]
    all_flux = all_flux[order]
    all_wv = all_wv[order]

    grouped = matched_catalog.group_by("healpix")
    group_keys = grouped.groups.keys["healpix"]
    group_indices = grouped.groups.indices

    total_saved = 0
    for i in tqdm(range(len(group_keys)), desc="Writing healpix groups"):
        hp_idx = group_keys[i]
        start = group_indices[i]
        end = group_indices[i + 1]

        cat_slice = matched_catalog[start:end]
        spectra_table = Table({
            "spectrum_flux": all_flux[start:end],
            "spectrum_wavelength": all_wv[start:end],
        })
        combined = hstack([cat_slice, spectra_table])

        output_filename = os.path.join(
            args.output_dir,
            f"{catalog_name}/healpix={hp_idx}/001-of-001.hdf5",
        )
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)

        with h5py.File(output_filename, "w") as hdf5_file:
            for key in combined.colnames:
                hdf5_file.create_dataset(key.lower(), data=combined[key])

        total_saved += end - start

    print(f"Saved {total_saved} spectra across {len(group_keys)} healpix groups")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build healpix-grouped HDF5 parent sample from intermediate files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Reads intermediate HDF5 files from <output_dir>/_intermediate/ (produced by
process_tars.py) and writes final healpix-grouped HDF5 files to
<output_dir>/<catalog_name>/healpix=<N>/001-of-001.hdf5.

Run this once after all process_tars.py workers have completed.

Example:
  python build_parent_sample.py dr11_v2.0_LRS_stellar.fits ./output
  python build_parent_sample.py dr11_v2.0_MRS_stellar.fits ./output --tiny
        """,
    )
    parser.add_argument(
        "catalog_path", type=str, help="Path to LAMOST catalog file (FITS format)",
    )
    parser.add_argument(
        "output_dir", type=str,
        help="Output directory (reads from <output_dir>/_intermediate/, writes healpix groups)",
    )
    parser.add_argument(
        "--tiny", action="store_true",
        help="Process only the first 50 catalog rows (must match what process_tars.py used)",
    )

    args = parser.parse_args()
    main(args)
