import argparse
import multiprocessing as mp
import os
from functools import partial

import h5py
import healpy as hp
import numpy as np
from astropy.io import fits
from scipy.optimize import curve_fit
from tqdm.auto import tqdm

BANDS = ["BLUE", "GREEN", "RED", "NIR"]

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


def process_band_fits(filename):
    try:
        spectrum = {}

        hdu = fits.open(filename)

        # Unnormalized, sky-substracted spectrum
        flux = hdu[0].data
        sigma = hdu[1].data

        # Normalized spectrum
        norm_flux = hdu[4].data

        start_wavelength = hdu[0].header["CRVAL1"]
        dispersion = hdu[0].header["CDELT1"]
        nr_pixels = hdu[0].header["NAXIS1"]
        reference_pixel = hdu[0].header["CRPIX1"]
        if reference_pixel == 0:
            reference_pixel = 1

        spectrum["flux"] = flux
        spectrum["lambda"] = (
            np.arange(0, nr_pixels) - -reference_pixel + 1
        ) * dispersion + start_wavelength
        spectrum["ivar"] = 1 / np.power(flux * sigma, 2)

        norm_start_wavelength = hdu[4].header["CRVAL1"]
        norm_dispersion = hdu[4].header["CDELT1"]
        norm_nr_pixels = hdu[4].header["NAXIS1"]
        norm_reference_pixel = hdu[4].header["CRPIX1"]
        if norm_reference_pixel == 0:
            norm_reference_pixel = 1
        spectrum["norm_flux"] = norm_flux
        spectrum["norm_lambda"] = (
            np.arange(0, norm_nr_pixels) - -norm_reference_pixel + 1
        ) * norm_dispersion + norm_start_wavelength
        spectrum["norm_ivar"] = 1 / np.power(norm_flux * sigma, 2)
        spectrum["timestamp"] = hdu[0].header["UTMJD"]

        return spectrum

    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return None


def process_object(
    cat_idx,
    data_dir,
):
    cat_row = GLOBAL_CATALOG[cat_idx]
    vac_row = GLOBAL_VAC[cat_idx]  # row matched with catalog

    sobject_id = cat_row["sobject_id"]

    fits_files = [
        os.path.join(data_dir, f"{sobject_id}{band}.fits") for band in [1, 2, 3, 4]
    ]
    spectra = {b: process_band_fits(f) for b, f in zip(BANDS, fits_files)}

    for s in spectra.values():
        if s is None:
            return None

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
        "m_act": "m_act_bstep",
        "age": "age_bstep",
        "distance": "distance_bstep",
        "radius": "radius_bstep",
        "e_log_lum": "e_log_lum_bstep",
        "e_m_act": "e_m_act_bstep",
        "e_age": "e_age_bstep",
        "e_distance": "e_distance_bstep",
        "e_radius": "e_radius_bstep",
    }

    cat_keys = {
        k: k
        for k in [
            "teff",
            "logg",
            "fe_h",
            "vbroad",
            "alpha_fe",
            "vmic",
            "e_teff",
            "e_logg",
            "e_fe_h",
            "e_vbroad",
            "e_alpha_fe",
            "ebv",
            "Li_fe",
            "C_fe",
            "O_fe",
            "Na_fe",
            "Mg_fe",
            "Al_fe",
            "Si_fe",
            "K_fe",
            "Ca_fe",
            "Sc_fe",
            "Sc2_fe",
            "Ti_fe",
            "Ti2_fe",
            "V_fe",
            "Cr_fe",
            "Cr2_fe",
            "Mn_fe",
            "Co_fe",
            "Ni_fe",
            "Cu_fe",
            "Zn_fe",
            "Rb_fe",
            "Sr_fe",
            "Y_fe",
            "Zr_fe",
            "Mo_fe",
            "Ru_fe",
            "Ba_fe",
            "La_fe",
            "Ce_fe",
            "Nd_fe",
            "Sm_fe",
            "Eu_fe",
            "Li_fe",
            "C_fe",
            "O_fe",
            "Na_fe",
            "Mg_fe",
            "Al_fe",
            "Si_fe",
            "K_fe",
            "Ca_fe",
            "Sc_fe",
            "Sc2_fe",
            "Ti_fe",
            "Ti2_fe",
            "V_fe",
            "Cr_fe",
            "Cr2_fe",
            "Mn_fe",
            "Co_fe",
            "Ni_fe",
            "Cu_fe",
            "Zn_fe",
            "Rb_fe",
            "Sr_fe",
            "Y_fe",
            "Zr_fe",
            "Mo_fe",
            "Ru_fe",
            "Ba_fe",
            "La_fe",
            "Ce_fe",
            "Nd_fe",
            "Sm_fe",
            "Eu_fe",
            "e_Li_fe",
            "e_C_fe",
            "e_O_fe",
            "e_Na_fe",
            "e_Mg_fe",
            "e_Al_fe",
            "e_Si_fe",
            "e_K_fe",
            "e_Ca_fe",
            "e_Sc_fe",
            "e_Sc2_fe",
            "e_Ti_fe",
            "e_Ti2_fe",
            "e_V_fe",
            "e_Cr_fe",
            "e_Cr2_fe",
            "e_Mn_fe",
            "e_Co_fe",
            "e_Ni_fe",
            "e_Cu_fe",
            "e_Zn_fe",
            "e_Rb_fe",
            "e_Sr_fe",
            "e_Y_fe",
            "e_Zr_fe",
            "e_Mo_fe",
            "e_Ru_fe",
            "e_Ba_fe",
            "e_La_fe",
            "e_Ce_fe",
            "e_Nd_fe",
            "e_Sm_fe",
            "e_Eu_fe",
            "e_Li_fe",
            "e_C_fe",
            "e_O_fe",
            "e_Na_fe",
            "e_Mg_fe",
            "e_Al_fe",
            "e_Si_fe",
            "e_K_fe",
            "e_Ca_fe",
            "e_Sc_fe",
            "e_Sc2_fe",
            "e_Ti_fe",
            "e_Ti2_fe",
            "e_V_fe",
            "e_Cr_fe",
            "e_Cr2_fe",
            "e_Mn_fe",
            "e_Co_fe",
            "e_Ni_fe",
            "e_Cu_fe",
            "e_Zn_fe",
            "e_Rb_fe",
            "e_Sr_fe",
            "e_Y_fe",
            "e_Zr_fe",
            "e_Mo_fe",
            "e_Ru_fe",
            "e_Ba_fe",
            "e_La_fe",
            "e_Ce_fe",
            "e_Nd_fe",
            "e_Sm_fe",
            "e_Eu_fe",
            "flag_fe_h",
            "flag_Li_fe",
            "flag_C_fe",
            "flag_O_fe",
            "flag_Na_fe",
            "flag_Mg_fe",
            "flag_Al_fe",
            "flag_Si_fe",
            "flag_K_fe",
            "flag_Ca_fe",
            "flag_Sc_fe",
            "flag_Sc2_fe",
            "flag_Ti_fe",
            "flag_Ti2_fe",
            "flag_V_fe",
            "flag_Cr_fe",
            "flag_Cr2_fe",
            "flag_Mn_fe",
            "flag_Co_fe",
            "flag_Ni_fe",
            "flag_Cu_fe",
            "flag_Zn_fe",
            "flag_Rb_fe",
            "flag_Sr_fe",
            "flag_Y_fe",
            "flag_Zr_fe",
            "flag_Mo_fe",
            "flag_Ru_fe",
            "flag_Ba_fe",
            "flag_La_fe",
            "flag_Ce_fe",
            "flag_Nd_fe",
            "flag_Sm_fe",
            "flag_Eu_fe",
            "flag_Li_fe",
            "flag_C_fe",
            "flag_O_fe",
            "flag_Na_fe",
            "flag_Mg_fe",
            "flag_Al_fe",
            "flag_Si_fe",
            "flag_K_fe",
            "flag_Ca_fe",
            "flag_Sc_fe",
            "flag_Sc2_fe",
            "flag_Ti_fe",
            "flag_Ti2_fe",
            "flag_V_fe",
            "flag_Cr_fe",
            "flag_Cr2_fe",
            "flag_Mn_fe",
            "flag_Co_fe",
            "flag_Ni_fe",
            "flag_Cu_fe",
            "flag_Zn_fe",
            "flag_Rb_fe",
            "flag_Sr_fe",
            "flag_Y_fe",
            "flag_Zr_fe",
            "flag_Mo_fe",
            "flag_Ru_fe",
            "flag_Ba_fe",
            "flag_La_fe",
            "flag_Ce_fe",
            "flag_Nd_fe",
            "flag_Sm_fe",
            "flag_Eu_fe",
            "snr_c1_iraf",
            "snr_c2_iraf",
            "snr_c3_iraf",
            "snr_c4_iraf",
            "flag_sp",
        ]
    }

    cat_keys.update(
        {
            "ra": "ra_dr2",
            "dec": "dec_dr2",
            "rv": "rv_galah",
            "e_rv": "e_rv_galah",
        }
    )

    for k, v in vac_keys.items():
        output[k] = vac_row[v]

    for k, v in cat_keys.items():
        output[k] = cat_row[v]

    output["healpix"] = GLOBAL_HEALPIX[cat_idx]

    return output


def collate_and_chunk(list_of_dicts, chunks):
    output = {}
    for k in list_of_dicts[0].keys():
        output[k] = np.array_split(np.stack([d[k] for d in list_of_dicts]), chunks)
    return output


def join_batched_spectra(spectra, output_dir, max_rows_per_file):
    # Pad all spectra to the same length
    max_length = max([len(s["spectrum_flux"]) for s in spectra])
    num_processed = len(spectra)

    for i in range(len(spectra)):
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

    os.makedirs(output_dir, exist_ok=True)
    num_chunks = int(np.ceil(num_processed / max_rows_per_file))

    spectra_chunked = collate_and_chunk(spectra, num_chunks)

    for i in range(num_chunks):
        with h5py.File(
            os.path.join(output_dir, f"{i+1:03d}-of-{num_chunks:03d}.hdf5"), "a"
        ) as f:
            for k in spectra_chunked.keys():
                f.create_dataset(k, data=spectra_chunked[k][i])

    return num_processed


def process_and_write_batched_spectra(
    cat_idxs_and_output_dir, data_dir, verbose, max_rows_per_file
):
    cat_idxs, output_dir = cat_idxs_and_output_dir
    if verbose:
        print(f"worker {mp.current_process().pid} processing {len(cat_idxs)} objects")
    spectra = [process_object(i, data_dir) for i in cat_idxs]
    spectra = list(filter(lambda x: x is not None, spectra))
    if verbose:
        print(f"worker {mp.current_process().pid} writing {len(cat_idxs)} objects")
    num_proc = join_batched_spectra(spectra, output_dir, max_rows_per_file)
    return num_proc


def main(args):
    # get catalog

    with fits.open(args.allstar_file) as hdul:
        catalog = hdul[1].data.copy()
    with fits.open(args.vac_file) as hdul:
        vac = hdul[1].data.copy()

    # # make cuts
    # catalog = catalog[
    #     (catalog["flag_sp"] == 0)  # spectra are fine
    # ]

    # remove objects with (partially) missing spectra
    if args.missing_id_file is not None:
        missing_ids = np.loadtxt(args.missing_id_file, skiprows=1, delimiter=",")[
            :, 0
        ].astype(int)

        catalog = catalog[~np.isin(catalog["sobject_id"], missing_ids)]

    if args.tiny:
        filter_fn = np.vectorize(lambda x: str(x).startswith("151224"))
        filter_ix = np.where(filter_fn(catalog["sobject_id"]))[0]
        catalog = catalog[filter_ix]

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

    os.makedirs(args.output_dir, exist_ok=True)

    total_to_process = catalog["sobject_id"].size

    healpix_row_mappings = {
        hp: np.where(GLOBAL_HEALPIX == hp)[0] for hp in np.unique(GLOBAL_HEALPIX)
    }

    print(
        f"split into {len(healpix_row_mappings)} healpix chunks, processing with {args.num_workers} workers"
    )

    map_args = [
        (idxs, os.path.join(args.output_dir, f"healpix={hp}"))
        for hp, idxs in healpix_row_mappings.items()
    ]
    map_args = sorted(map_args, key=lambda x: len(x[0]))

    pbar = tqdm(total=total_to_process)

    with mp.Pool(args.num_workers) as pool:
        for num_proc in pool.imap_unordered(
            # split into chunks
            partial(
                process_and_write_batched_spectra,
                data_dir=args.data_dir,
                verbose=args.verbose,
                max_rows_per_file=args.max_rows_per_file,
            ),
            map_args,
        ):
            pbar.update(num_proc)

    print("done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, required=True)
    parser.add_argument("--allstar_file", type=str, required=True)
    parser.add_argument("--vac_file", type=str, required=True)
    parser.add_argument("--missing_id_file", type=str, required=False)
    parser.add_argument("--resolution_map_dir", type=str, required=True)
    parser.add_argument("--tiny", action="store_true", default=False)
    parser.add_argument("--nside", type=int, default=16)
    parser.add_argument("--num_workers", type=int, default=os.cpu_count())
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--max_rows_per_file", type=int, default=999_999_999)
    args = parser.parse_args()
    main(args)
