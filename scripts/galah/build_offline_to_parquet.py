import argparse
import itertools
import multiprocessing as mp
import os
import tarfile

import h5py
import healpy as hp
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from astropy.io import fits
from scipy.optimize import curve_fit
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map

BANDS = ["BLUE", "GREEN", "RED", "NIR"]

GLOBAL_TAR = None
GLOBAL_CATALOG = None
GLOBAL_VAC = None
GLOBAL_RESOLUTION_MAPS = None
GLOBAL_HEALPIX = None


def ang2pix(ra, dec, nside):
    return hp.ang2pix(nside=nside, theta=ra, phi=dec, lonlat=True, nest=True)


def get_resolution(ccd_resolution_map_filename):
    with fits.open(ccd_resolution_map_filename) as hdul:
        resolution = hdul[0].data

    mean_resolution = np.mean(resolution, axis=0)

    def _gauss(x, a, x0, sigma):
        return a * np.exp(-((x - x0) ** 2) / (2 * sigma**2))

    try:
        popt, _ = curve_fit(
            _gauss, np.arange(len(mean_resolution)), mean_resolution, p0=[1, 5, 1]
        )
    except Exception as e:
        popt = [-1, -1, -1]

    return mean_resolution, popt[2] * np.ones(
        shape=len(mean_resolution), dtype=np.float32
    )


def process_band_fits(hdu0_header, hdu0_data, hdu1_data, hdu4_header, hdu4_data):
    try:
        spectrum = {}

        # Unnormalized, sky-substracted spectrum
        flux = hdu0_data
        sigma = hdu1_data

        # Normalized spectrum
        norm_flux = hdu4_data

        start_wavelength = hdu0_header["CRVAL1"]
        dispersion = hdu0_header["CDELT1"]
        nr_pixels = hdu0_header["NAXIS1"]
        reference_pixel = hdu0_header["CRPIX1"]
        if reference_pixel == 0:
            reference_pixel = 1

        spectrum["flux"] = flux
        spectrum["lambda"] = (
            np.arange(0, nr_pixels) - -reference_pixel + 1
        ) * dispersion + start_wavelength
        spectrum["ivar"] = 1 / np.power(flux * sigma, 2)

        # # TODO: UPDATE
        # # PLACEHOLDER VALUE!
        # # Get an averaged estimated Gaussian line spread function
        # # TODO: Actually properly estimate the line spread function of each spectrum
        # lsf, lsf_sigma = get_resolution(resolution_filename)

        norm_start_wavelength = hdu4_header["CRVAL1"]
        norm_dispersion = hdu4_header["CDELT1"]
        norm_nr_pixels = hdu4_header["NAXIS1"]
        norm_reference_pixel = hdu4_header["CRPIX1"]
        if norm_reference_pixel == 0:
            norm_reference_pixel = 1
        spectrum["norm_flux"] = norm_flux
        spectrum["norm_lambda"] = (
            np.arange(0, norm_nr_pixels) - -norm_reference_pixel + 1
        ) * norm_dispersion + norm_start_wavelength
        spectrum["norm_ivar"] = 1 / np.power(norm_flux * sigma, 2)
        # spectrum["lsf"] = lsf
        # spectrum["lsf_sigma"] = lsf_sigma
        spectrum["timestamp"] = hdu0_header["UTMJD"]

        return spectrum

    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return None


def read_batch(
    batch: list[tarfile.TarInfo],
):
    sobject_id = int(batch[0].name.split("/")[-1][:-6])
    fits_files = []
    global GLOBAL_TAR
    with tarfile.open(GLOBAL_TAR, "r:gz") as tar:
        for b in batch:
            with fits.open(tar.extractfile(b)) as f:
                fits_files.append(
                    [
                        f[0].header.copy(),
                        f[0].data.copy(),
                        f[1].data.copy(),
                        f[4].header.copy(),
                        f[4].data.copy(),
                    ]
                )
    return sobject_id, fits_files


def process_batch(
    sobject_id_and_fits_files,
):
    sobject_id, fits_files = sobject_id_and_fits_files

    cat_idx = np.where(GLOBAL_CATALOG["sobject_id"] == int(sobject_id))[0]
    if cat_idx.size == 0:
        return None

    cat_row = GLOBAL_CATALOG[cat_idx]
    vac_row = GLOBAL_VAC[cat_idx]  # row matched with catalog

    spectra = {b: process_band_fits(*f) for b, f in zip(BANDS, fits_files)}
    for band in BANDS:
        spectra[band]["lsf"], spectra[band]["lsf_sigma"] = GLOBAL_RESOLUTION_MAPS[band]

    len_B = len(spectra["BLUE"]["lambda"])
    len_G = len(spectra["GREEN"]["lambda"])
    len_R = len(spectra["RED"]["lambda"])
    len_I = len(spectra["NIR"]["lambda"])

    # Return the results
    output = {
        "object_id": sobject_id,
        "timestamp": np.mean([spectra[band]["timestamp"] for band in BANDS]).astype(
            np.float32
        ),
        "spectrum_B_ind_start": 0,
        "spectrum_B_ind_end": len_B - 1,
        "spectrum_G_ind_start": len_B,
        "spectrum_G_ind_end": len_B + len_G - 1,
        "spectrum_R_ind_start": len_B + len_G,
        "spectrum_R_ind_end": len_B + len_G + len_R - 1,
        "spectrum_I_ind_start": len_B + len_G + len_R,
        "spectrum_I_ind_end": len_B + len_G + len_R + len_I - 1,
    }

    spectrum_keys = [
        "lambda",
        "flux",
        "ivar",
        "lsf",
        "lsf_sigma",
        "norm_lambda",
        "norm_flux",
        "norm_ivar",
    ]

    for k in spectrum_keys:
        output[f"spectrum_{k}"] = (
            np.concatenate([spectra[band][k] for band in BANDS])
            .flatten()
            .astype(np.float32)
        )

    vac_keys = {
        "log_lum": "log_lum_bstep",
        "stellar_mass": "m_act_bstep",
        "age": "age_bstep",
        "distance": "distance_bstep",
    }

    cat_keys = {
        "teff": "teff",
        "logg": "logg",
        "fe_h": "fe_h",
        "vbroad": "vbroad",
        "alpha_fe": "alpha_fe",
        "rv": "rv_galah",
        "ebv": "ebv",
        "ra": "ra_dr2",
        "dec": "dec_dr2",
    }

    for k, v in vac_keys.items():
        output[k] = vac_row[v][0].astype(np.float32)

    for k, v in cat_keys.items():
        output[k] = cat_row[v][0].astype(np.float32)

    output["healpix"] = GLOBAL_HEALPIX[cat_idx][0].astype(np.int32)

    return output


def join_batched_spectra(spectra, output_dir):
    # Pad all spectra to the same length
    max_length = max([len(s["spectrum_flux"]) for s in spectra])

    for i in tqdm(range(len(spectra))):
        for k in [
            "spectrum_flux",
            "spectrum_ivar",
            "spectrum_lsf",
            "spectrum_lsf_sigma",
            "spectrum_norm_flux",
            "spectrum_norm_ivar",
        ]:
            spectra[i][k] = np.pad(
                spectra[i][k], (0, max_length - len(spectra[i][k])), mode="constant"
            )
        for k in ["spectrum_lambda", "spectrum_norm_lambda"]:
            spectra[i][k] = np.pad(
                spectra[i][k],
                (0, max_length - len(spectra[i][k])),
                mode="constant",
                constant_values=-1,
            )

    with h5py.File(os.path.join(output_dir, "001-of-001.hdf5"), "a") as f:
        for k in spectra[0].keys():
            if k not in f.keys():
                if isinstance(spectra[0][k], np.ndarray):
                    shape = (len(spectra), *spectra[0][k].shape)
                    maxshape = (None, *spectra[0][k].shape)
                    dtype = spectra[0][k].dtype
                else:
                    shape = (len(spectra),)
                    maxshape = (None,)
                    dtype = type(spectra[0][k])
                f.create_dataset(k, shape=shape, maxshape=maxshape, dtype=dtype)
            else:
                f[k].resize(f[k].shape[0] + len(spectra), axis=0)

            for i, s in enumerate(spectra):
                f[k][-len(spectra) + i] = s[k]

    return len(spectra)

    # # merge all spectra into parquet table
    # table = pa.Table.from_pylist(spectra)
    # pq.write_to_dataset(table, output_dir, partition_cols=["healpix"])
    # num_processed = len(spectra)
    # del table
    # del spectra
    # return num_processed


def batched_it(iterable, n, drop_last=False, sort_key=None):
    "Batch data into iterators of length n. Drops last batch if it is smaller."
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return

        out = list(itertools.chain((first_el,), chunk_it))
        if sort_key is not None:
            out = sorted(out, key=sort_key)

        if drop_last and len(out) < n:
            return
        else:
            yield out


def read_and_process_batch(batch):
    return process_batch(read_batch(batch))


def main(args):
    # get catalog

    with fits.open(args.allstar_file) as hdul:
        catalog = hdul[1].data.copy()
    with fits.open(args.vac_file) as hdul:
        vac = hdul[1].data.copy()

    # make cuts
    catalog = catalog[
        (catalog["flag_sp"] == 0)  # spectra are fine
        # & (catalog["flag_fe_h"] == 0) # metallicity is fine
    ]

    # keep overlap between catalog and vac, row match
    _, idx1, idx2 = np.intersect1d(
        catalog["sobject_id"],
        vac["sobject_id"],
        assume_unique=True,
        return_indices=True,
    )
    global GLOBAL_CATALOG
    global GLOBAL_VAC
    global GLOBAL_HEALPIX
    global GLOBAL_RESOLUTION_MAPS

    GLOBAL_CATALOG = np.array(catalog[idx1])
    GLOBAL_VAC = np.array(vac[idx2])
    GLOBAL_HEALPIX = ang2pix(catalog["ra_dr2"], catalog["dec_dr2"], nside=args.nside)

    # get resolution maps
    GLOBAL_RESOLUTION_MAPS = {
        band: get_resolution(f"{args.resolution_map_dir}/ccd{ccd}_piv.fits")
        for band, ccd in zip(BANDS, [1, 2, 3, 4])
    }

    # open tarball
    global GLOBAL_TAR
    GLOBAL_TAR = args.spectra_tarball
    tar = tarfile.open(args.spectra_tarball, "r:gz")
    batched_tar = batched_it(
        tar, 4, drop_last=True, sort_key=lambda x: x.name
    )  # chunks of 4 for 4 bands
    chunked_batches = batched_it(
        batched_tar, args.chunk_size, drop_last=False
    )  # chunks of objects

    print(f"processing with {args.num_workers} workers")

    os.makedirs(args.output_dir, exist_ok=True)

    pbar = tqdm(desc="Processing spectra")

    with mp.Pool(args.num_workers) as pool:
        for chunked_batch in chunked_batches:
            spectra = pool.imap_unordered(
                read_and_process_batch,
                tqdm(chunked_batch, leave=False),
            )

            spectra = list(
                filter(lambda x: x is not None, spectra)
            )  # remove None values (probably cut from catalog)

            num_processed = join_batched_spectra(spectra, args.output_dir)

            del spectra

            pbar.update(num_processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spectra_tarball", type=str, required=True)
    parser.add_argument("--allstar_file", type=str, required=True)
    parser.add_argument("--vac_file", type=str, required=True)
    parser.add_argument("--resolution_map_dir", type=str, required=True)
    parser.add_argument("--tiny", action="store_true", default=False)
    parser.add_argument("--nside", type=int, default=16)
    parser.add_argument("--num_workers", type=int, default=os.cpu_count())
    parser.add_argument("--chunk_size", type=int, default=1000)
    parser.add_argument("--output_dir", type=str, required=True)
    args = parser.parse_args()
    main(args)
