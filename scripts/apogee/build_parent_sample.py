import argparse
import os
import urllib
from functools import partial
from multiprocessing import Pool

import h5py
import healpy as hp
import numpy as np
from astropy.io import fits
from astropy.table import Table, hstack, join
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

_healpix_nside = 16

# APOGEE shares a global wavelength grid
lam = 10.0 ** np.arange(
    4.179, 4.179 + 8575 * 6.0 * 10.0**-6.0, 6.0 * 10.0**-6.0
).astype(np.float32)

# detector edges
blue_start = 246
blue_end = 3274
green_start = 3585
green_end = 6080
red_start = 6344
red_end = 8335
lam_cropped = lam[np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]]


def check_file_exists(x, base_path) -> bool:
    existing_mask, apogee_id, field, telescope, filename = x
    # print(
    #     f"apogee_id: {apogee_id}, field: {field}, telescope: {telescope}, filename: {filename}"
    # )
    if not existing_mask or filename is None:
        return False
    result = visit_spectra(base_path, field, telescope, filename)
    if result == 1:
        return False
    result = combined_spectra(base_path, field, apogee_id, telescope)
    if result == 1:
        return False
    return True


def selection_fn(base_path, catalog, check_exists=True):
    # Only use the spectrum from APO 2.5m and LCO 2.5m
    mask = (catalog["TELESCOPE"] == "apo25m") | (catalog["TELESCOPE"] == "lco25m")
    # known no file entries
    mask &= ~catalog["FILE"].mask
    # exclude low SNR
    mask &= catalog["SNR"] > 30

    # duplicated APOGEE_ID are causing trouble, simply take the unique IDs now
    _, idx = np.unique(catalog["APOGEE_ID"], return_index=True)
    _unique_mask = np.zeros(len(catalog), dtype=bool)
    _unique_mask[idx] = True
    mask &= _unique_mask

    if check_exists:
        # check if there is a cached file
        if os.path.exists("exists_mask_cached.npy"):
            exists_mask = np.load("exists_mask_cached.npy")
        else:
            exists_mask = process_map(
                partial(check_file_exists, base_path=base_path),
                list(
                    zip(
                        mask,
                        catalog["APOGEE_ID"],
                        catalog["FIELD"],
                        catalog["TELESCOPE"],
                        catalog["FILE"],
                    )
                ),
                desc="Checking files",
                chunksize=10,
            )
            exists_mask = np.array(exists_mask).astype(bool)
            np.save("exists_mask_cached.npy", exists_mask)

        mask = mask & exists_mask

    return mask


def download_allstar(base_path):
    fullfoldername = os.path.join(base_path, "spectro/aspcap/dr17/synspec_rev1/")
    # Check if directory exists
    if not os.path.exists(fullfoldername):
        os.makedirs(fullfoldername)
    filename = "allStar-dr17-synspec_rev1.fits"
    fullfilename = os.path.join(fullfoldername, filename)
    url = f"https://data.sdss.org/sas/dr17/apogee/spectro/aspcap/dr17/synspec_rev1/{filename}"
    urllib.request.urlretrieve(url, fullfilename)


def combined_spectra(base_path, field, apogee, telescope):
    aspcap_code = "synspec_rev1"
    str1 = f"https://data.sdss.org/sas/dr17/apogee/spectro/aspcap/dr17/{aspcap_code}/{telescope}/{field}/"

    filename = f"aspcapStar-dr17-{apogee}.fits"
    urlstr = str1 + filename

    # check folder existence
    fullfoldername = os.path.join(
        base_path,
        f"spectro/aspcap/dr17/{aspcap_code}/{telescope}",
        str(f"{field}"),
    )
    if not os.path.exists(fullfoldername):
        os.makedirs(fullfoldername)

    fullfilename = os.path.join(fullfoldername, filename)

    if os.path.exists(fullfilename):
        return fullfilename
    else:
        try:
            urllib.request.urlretrieve(urlstr, fullfilename)
            return 0
        except urllib.error.HTTPError as emsg:
            print(
                f"failed in combined spectra on following: base_path: {base_path}, field: {field}, apogee: {apogee}, telescope: {telescope}, filename: {filename}"
            )
            return 1  # error code


def visit_spectra(
    base_path,
    field,
    telescope,
    filename,
):
    str1 = f"https://data.sdss.org/sas/dr17/apogee/spectro/redux/dr17/stars/{telescope}/{field}/"
    try:
        urlstr = str1 + filename
    except Exception as e:
        raise ValueError(f"Error in constructing URL: {e}, {str1}, {filename}")

    fullfoldername = os.path.join(
        base_path,
        f"spectro/redux/dr17/stars/{telescope}/",
        str(f"{field}"),
    )

    if not os.path.exists(fullfoldername):
        os.makedirs(fullfoldername)

    fullfilename = os.path.join(fullfoldername, filename)
    if os.path.exists(fullfilename):
        # print(f"{fullfilename} exists, skipping")
        return fullfilename
    else:
        # print(f"{fullfilename} doesnt exist, downloading from {urlstr}")
        try:
            urllib.request.urlretrieve(urlstr, fullfilename)
            return 0
        except urllib.error.HTTPError as emsg:
            return 1  # error code


def processing_fn(raw_filename, continuum_filename):
    """Parallel processing function reading all requested spectra from one plate."""

    # Load the visit spectra file
    hdus = fits.open(raw_filename)
    raw_flux = hdus[1].data[0]
    raw_ivar = 1 / hdus[2].data[0] ** 2
    mask_spec = hdus[2].data[0]
    # if NaN, assume no mask since huggingface data does not support NaN in integer array
    mask_spec[np.isnan(mask_spec)] = 0

    # Load the combined spectra file
    hdus = fits.open(continuum_filename)
    continuum_flux = hdus[1].data
    continuum_ivar = 1 / hdus[2].data ** 2

    # very rough estimate
    # https://www.sdss4.org/dr17/irspec/spectra/
    lsf_sigma = np.ones_like(raw_flux)
    lsf_sigma[:blue_end] *= 0.326
    lsf_sigma[blue_end:green_end] *= 0.283
    lsf_sigma[green_end:] *= 0.236

    # Return the results
    return {
        "spectrum_lambda": lam_cropped,
        "spectrum_flux": raw_flux,
        "spectrum_ivar": raw_ivar,
        # pixel level bitmask
        # see https://www.sdss4.org/dr17/irspec/apogee-bitmasks/#APOGEE_PIXMASK:APOGEEbitmaskforindividualpixelsinaspectrum
        "spectrum_lsf_sigma": lsf_sigma,
        "spectrum_bitmask": mask_spec,
        "spectrum_pseudo_continuum_flux": continuum_flux,
        "spectrum_pseudo_continuum_ivar": continuum_ivar,
    }


def save_in_standard_format(args):
    """
    This function takes care of iterating through the different input files
    corresponding to this healpix index, and exporting the data in standard format.
    """
    catalog, output_filename, apogee_data_path = args

    # Create the output directory if it does not exist
    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

    try:
        # Rename columns to match the standard format
        catalog["object_id"] = catalog["APOGEE_ID"]
        catalog["radial_velocity"] = catalog["VHELIO_AVG"]
        catalog["restframe"] = np.ones(len(catalog), dtype=bool)

        # Preparing the arguments for the parallel processing
        # Process all files
        results = []
        for i in catalog:
            telescope = i["TELESCOPE"]
            field = i["FIELD"]
            filename = i["FILE"]
            apogee_id = i["APOGEE_ID"]
            results.append(
                processing_fn(
                    visit_spectra(apogee_data_path, field, telescope, filename),
                    combined_spectra(apogee_data_path, field, apogee_id, telescope),
                )
            )

        # Aggregate all spectra into an astropy table
        spectra = Table(
            {k: np.vstack([d[k] for d in results]) for k in results[0].keys()}
        )

        # crop apogee spectra
        spectra["spectrum_flux"] = spectra["spectrum_flux"][
            :, np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]
        ]
        spectra["spectrum_ivar"] = spectra["spectrum_ivar"][
            :, np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]
        ]
        spectra["spectrum_bitmask"] = spectra["spectrum_bitmask"][
            :, np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]
        ]
        spectra["spectrum_lsf_sigma"] = spectra["spectrum_lsf_sigma"][
            :, np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]
        ]
        spectra["spectrum_pseudo_continuum_flux"] = spectra[
            "spectrum_pseudo_continuum_flux"
        ][:, np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]]
        spectra["spectrum_pseudo_continuum_ivar"] = spectra[
            "spectrum_pseudo_continuum_ivar"
        ][:, np.r_[blue_start:blue_end, green_start:green_end, red_start:red_end]]
        # Join on target id with the input catalog
        # catalog = join(catalog, spectra, keys="object_id", join_type="inner")
        catalog = hstack([catalog, spectra])

        # Save all columns to disk in HDF5 format
        with h5py.File(output_filename, "w") as hdf5_file:
            for key in catalog.colnames:
                hdf5_file.create_dataset(key.lower(), data=catalog[key])
        return 1

    except Exception as e:
        print(f"Error processing {output_filename}: {e}")
        print(
            f"catalog: {catalog} \n output_filename: {output_filename} \n apogee_data_path: {apogee_data_path}"
        )
        return 0


def main(args):
    # Load the catalog file and apply main cuts
    path_to_read = os.path.join(
        os.getcwd(),
        args.apogee_data_path,
        "spectro/aspcap/dr17/synspec_rev1",
        "allStar-dr17-synspec_rev1.fits",
    )
    if not os.path.exists(path_to_read):
        print("Downloading allStar catalog file...")
        download_allstar(os.path.join(os.getcwd(), args.apogee_data_path))
    print("Reading allStar catalog file...")
    catalog = Table.read(path_to_read, hdu=1)

    # if only tiny then build the with only a few stars
    if args.tiny:
        catalog = catalog[:50]

    print("Checking and downloading spectra files...")
    catalog = catalog[selection_fn(args.apogee_data_path, catalog, not args.skip_check)]

    print("Calculating healpix index...")
    # Add healpix index to the catalog
    catalog["healpix"] = hp.ang2pix(
        _healpix_nside,
        catalog["RA"].filled(),
        catalog["DEC"].filled(),
        lonlat=True,
        nest=True,
    )
    catalog = catalog.group_by(["healpix"])

    # Preparing the arguments for the parallel processing
    map_args = []

    for group in catalog.groups:
        # Create a filename for the group
        group_filename = os.path.join(
            args.output_dir,
            "apogee/healpix={}/001-of-001.hdf5".format(group["healpix"][0]),
        )
        map_args.append((group, group_filename, args.apogee_data_path))

    print("Processing data...")
    # Run the parallel processing
    with Pool(args.num_processes) as pool:
        results = list(
            tqdm(pool.imap(save_in_standard_format, map_args), total=len(map_args))
        )

    if sum(results) != len(map_args):
        print(
            "There was an error in the parallel processing, some files may not have been processed correctly"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download (if not existed already) and Extracts spectra from all SDSS spectra"
    )
    parser.add_argument(
        "--apogee_data_path",
        type=str,
        help="Path to the local copy of the APOGEE data",
    )
    parser.add_argument("--output_dir", type=str, help="Path to the output directory")
    parser.add_argument(
        "--skip_check", action="store_true", help="Skip file checks", default=False
    )
    parser.add_argument(
        "--num_processes",
        type=int,
        default=os.cpu_count(),
        help="The number of processes to use for parallel processing",
    )
    parser.add_argument(
        "--tiny",
        action="store_true",
        help="Use a tiny subset of the data for testing",
    )
    args = parser.parse_args()

    main(args)
