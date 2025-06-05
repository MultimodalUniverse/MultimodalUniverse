# Gaia DR3

This folder contains datasets based on the Gaia DR3 catalogs. There are three datasets available: Gaia Source, Gaia RVS mean spectra, and Gaia BP/RP spectral coefficients.

For more information on Gaia data, see https://gea.esac.esa.int/archive/documentation/GDR3/Gaia_archive/chap_datamodel/sec_dm_main_source_catalogue/ssec_dm_gaia_source.html.

## Data Preparation

The data can be downloaded from Globus here: https://app.globus.org/file-manager/collections/90f5713a-e74d-11ec-9bd2-2d2219dcc1fa/overview. There is also a `download_parts.py` script that can be used to download the data in parts. The data are stored in split HDF5 files. I would highly recommend downloading `aria2c` and using the `--aria2` flag in the Python script if you are downloading the entire dataset. **Note that the full dataset is very large (about 4.6TB).**

RA and Dec are then added to the XP coefficient files with `add_extras.py`, all the files are partitioned according to healpix and saved in parquet files with `to_parquet.py`, and then the parquet files are converted to HDF5 with `to_hdf5.py`.

As an example of the full preparation, you can see the `test.sh` script.

## Dataset

There are the variables `ra`, `dec`, `object_id` (Gaia `source_id`), and `healpix` for consistency with the rest of the datasets. Note that `ra` and `dec` are duplicated from the astrometry section.

See the data models here for the full data columns:
- [Source](https://gaia.aip.de/metadata/gaiadr3/gaia_source/)
- [RVS](https://gaia.aip.de/metadata/gaiadr3/rvs_mean_spectrum/)
- [XP](https://gaia.aip.de/metadata/gaiadr3/xp_continuous_mean_spectrum/)
