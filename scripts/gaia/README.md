# Gaia DR3

This folder contains a dataset based on the Gaia DR3 catalog. The dataset is a subset of the full catalog, containing only stars which have BP/RP low-resolution spectra available. For this dataset, we prepare astrometry, photometry, photometrically-estimated parameters, and low-resolution spectra in the form of spectral coefficients.

For more information on Gaia data, see https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html.

## General

There are the variables `ra`, `dec`, `object_id` (Gaia `source_id`), and `healpix` for consistency with the rest of the datasets. Note that `ra` and `dec` are duplicated from the astrometry section.

## Astrometry

This includes:

- `ra` and `dec` in degrees
- `parallax` in mas
- `pmra` and `pmdec` in mas/yr
- `ra_error`, `dec_error`, `parallax_error`, `pmra_error`, and `pmdec_error` in mas
- `ra_dec_corr`, `ra_parallax_corr`, `ra_pmra_corr`, `ra_pmdec_corr`, `dec_parallax_corr`, `dec_pmra_corr`, `dec_pmdec_corr`, `parallax_pmra_corr`, `parallax_pmdec_corr`, and `pmra_pmdec_corr` as correlation coefficients

## Photometry

This includes:

- `phot_g_mean_mag`, `phot_bp_mean_mag`, and `phot_rp_mean_mag` in mag
- `phot_g_mean_flux`, `phot_bp_mean_flux`, and `phot_rp_mean_flux` in e-/s
- `phot_g_mean_flux_error`, `phot_bp_mean_flux_error`, and `phot_rp_mean_flux_error` in e-/s
- `phot_bp_rp_excess_factor` in mag
- `bp_rp`, `bp_g`, `g_rp` in mag

## Photometrically-estimated parameters

These are stellar parameters derived from the low-resolution BP/RP spectra, apparent G magnitude and parallax. See more details here: https://gea.esac.esa.int/archive/documentation/GDR3/Data_analysis/chap_cu8par/sec_cu8par_apsis/ssec_cu8par_apsis_gspphot.html.

This includes:

- `ag_gspphot`, `azero_gspphot`, `distance_gspphot`, `ebpminrp_gspphot`, `logg_gspphot`, `mh_gspphot`, `teff_gspphot`, as well as all lower and upper confidence intervals (16% and 84%) for these parameters (e.g. `ag_gspphot_lower`, `ag_gspphot_upper`)

## Spectral coefficients

These are the coefficients of the low-resolution BP/RP spectra. This includes:

- `coeff`, which is a length 110 array, with the first 55 elements corresponding to the BP coefficients and the last 55 elements corresponding to the RP coefficients
- `coeff_error`, which is the standard deviation on the coefficients

If you want the spectrum in wavelength/flux space, you can use GaiaXPy to convert.

## Radial velocities

This includes:

- `radial_velocity` in km/s
- `radial_velocity_error` in km/s
- `rv_template_teff`, `rv_template_logg`, `rv_template_fe_h` which are the stellar parameters of the template used to derive the radial velocity

## Corrections

There are a few variables that are useful for corrections:

- `ecl_lat`, `ecl_lon`, `nu_eff_used_in_astrometry`, `pseudocolor`, and `astrometric_params_solved` for zero point corrections
- `rv_template_teff`, `grvs_mag` for radial velocity corrections

## Flags

There are some flags for quality control:

- `ruwe` is the renormalized unit weight error, where you can typically remove sources with ruwe > 1.4 if you want to select single stars

## Data Download

The data are available from Globus at the following link: [Gaia DR3 Dataset](https://app.globus.org/file-manager/collections/90f5713a-e74d-11ec-9bd2-2d2219dcc1fa/overview).

The current parent sample is built from the Gaia Source Catalog and the XP coefficients, and `build_parent_sample.py` healpix-ifies it. To create the actual parent sample, you need to download the source catalogs and XP coefficients, then join it all together into one HDF5 file.
