import argparse
import os
from multiprocessing import Pool

import h5py
import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.table import Table, hstack
from tqdm import tqdm

_healpix_nside = 16

# LAMOST wavelength coverage: 3700-9000 Å
blue_start = 3841
blue_end = 5800
red_start = 5800
red_end = 8798
max_length = 3908


def processing_fn(obsid, base_path):
    """Read LAMOST spectrum file, downloading if necessary"""
    filename = f"{base_path}/{obsid}.fits"
    try:
        with fits.open(filename) as hdulist:
            binaryext = hdulist[1].data
            header = hdulist[0].header
            x = binaryext["FLUX"].astype(np.float32)  # shape: (1, seq_len)
            wv = binaryext["WAVELENGTH"].astype(np.float32)  # shape: (1, seq_len)

            # Handle truncation/padding for shape (1, seq_len)
            if x.shape[1] > max_length:
                x = x[:, :max_length]
            elif x.shape[1] < max_length:
                x = np.pad(
                    x,
                    ((0, 0), (0, max_length - x.shape[1])),
                    mode="constant",
                    constant_values=0,
                )

            # Handle wavelength array the same way
            if wv.shape[1] > max_length:
                wv = wv[:, :max_length]
            elif wv.shape[1] < max_length:
                wv = np.pad(
                    wv,
                    ((0, 0), (0, max_length - wv.shape[1])),
                    mode="constant",
                    constant_values=0,
                )

    except Exception as e:
        print(f"Error processing {obsid}: {e}")
        return None

    return {
        "spectrum_flux": x,
        "spectrum_wavelength": wv,
    }


def save_in_standard_format(args):
    """Save processed spectra in standard HDF5 format"""
    catalog, output_filename, base_path = args

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)

    try:
        # Add required standard columns while preserving all existing catalog columns
        # Add object_id (keeping obsid as well)
        if "object_id" not in catalog.colnames:
            catalog["object_id"] = catalog["obsid"]

        # Add radial_velocity (keeping rv as well if it exists)
        if "radial_velocity" not in catalog.colnames and "rv" in catalog.colnames:
            catalog["radial_velocity"] = catalog["rv"]

        # Add restframe column
        catalog["restframe"] = np.ones(len(catalog), dtype=bool)

        # Process all files
        results = []
        for row in catalog:  # This iterates over Row objects
            result = processing_fn(row["obsid"], base_path)
            if result is not None:
                results.append(result)
            else:
                print(f"Failed to process spectrum for {row['obsid']}")

        if not results:
            print(f"No valid spectra found for {output_filename}")
            return 0

        print(f"Successfully processed {len(results)} spectra")
        print("Keys in first result:", results[0].keys())

        # Aggregate all spectra into an astropy table
        # Since each result has arrays of shape (1, seq_len), we can directly stack them
        spectra = Table(
            {k: np.vstack([d[k] for d in results]) for k in results[0].keys()}
        )

        n_rows = len(spectra)
        n_cols = len(spectra.colnames)
        print(f"Spectra table: {n_rows} rows, {n_cols} columns")
        print("Spectra columns:", spectra.colnames)
        print("Flux shape:", spectra["spectrum_flux"].shape)
        print("Wave shape:", spectra["spectrum_wavelength"].shape)

        # Join catalog and spectra data
        # Make sure we have the same number of rows
        if len(catalog) != len(spectra):
            print(
                f"Warning: catalog has {len(catalog)} rows but spectra has {len(spectra)} rows"
            )
            # Truncate catalog to match spectra if some failed
            catalog = catalog[: len(spectra)]

        catalog = hstack([catalog, spectra])
        print(
            f"Combined catalog and spectra: {len(catalog)} rows, {len(catalog.colnames)} columns"
        )

        # Save to HDF5
        with h5py.File(output_filename, "w") as hdf5_file:
            for key in catalog.colnames:
                hdf5_file.create_dataset(key.lower(), data=catalog[key])

        print(f"Saved {len(catalog)} objects to {output_filename}")
        return 1

    except Exception as e:
        print(f"Error processing {output_filename}: {e}")
        import traceback

        traceback.print_exc()
        return 0


def main(args):
    """Main processing function"""
    catalog_path = args.catalog_path

    catalog_name = (
        catalog_path.replace(".fits", "").lower().replace(".", "")
    )  # hf doesn't like dots in data file path

    # Load catalog
    print(f"Loading catalog from {catalog_path}")
    try:
        catalog = Table.read(catalog_path, hdu=1)
    except Exception as e:
        print(f"Error reading catalog: {e}")
        print("Please check the catalog file format and path")
        return

    # Apply tiny subset if requested
    if args.tiny:
        catalog = catalog[:50]
        print(f"Using tiny subset of {len(catalog)} objects")

    if len(catalog) == 0:
        print("No objects found in the catalog.")
        return

    # Calculate healpix indices
    print("Calculating healpix indices...")
    catalog["healpix"] = hp.ang2pix(
        _healpix_nside, catalog["ra"], catalog["dec"], lonlat=True, nest=True
    )

    # Group by healpix
    catalog_grouped = catalog.group_by("healpix")

    # Prepare arguments for parallel processing
    map_args = []
    for group in catalog_grouped.groups:
        output_filename = os.path.join(
            args.output_dir,
            f"{catalog_name}/healpix={group['healpix'][0]}/001-of-001.hdf5",
        )
        map_args.append((group, output_filename, args.lamost_data_path))

    print(f"Processing {len(map_args)} healpix groups...")

    # Process in parallel
    with Pool(args.num_processes) as pool:
        results = list(
            tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args))
        )

    successful = sum(results)
    print(f"Successfully processed {successful}/{len(map_args)} healpix groups")

    if successful < len(map_args):
        print("Some files failed to process. Check error messages above.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and process LAMOST spectra to create parent sample"
    )
    parser.add_argument(
        "catalog_path", type=str, help="Path to LAMOST catalog file (FITS format)"
    )
    parser.add_argument(
        "lamost_data_path", type=str, help="Path to store/find LAMOST data"
    )
    parser.add_argument(
        "output_dir", type=str, help="Output directory for processed data"
    )

    parser.add_argument(
        "--num_processes",
        type=int,
        default=os.cpu_count(),
        help="Number of parallel processes",
    )
    parser.add_argument(
        "--tiny", action="store_true", help="Process only a small subset for testing"
    )
    parser.add_argument(
        "--skip_check",
        action="store_true",
        help="Skip checking if spectrum files exist",
    )

    args = parser.parse_args()
    main(args)
