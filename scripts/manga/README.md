# MaNGA Dataset

The [SDSS-IV MaNGA](https://www.sdss4.org/surveys/manga/) survey is a wide-field optical IFU galaxy survey of ~10,000 galaxies.  This dataset is using the final [DR17](https://www.sdss4.org/dr17/manga/) release.  MaNGA data products consist of reconstructed [datacubes](https://www.sdss4.org/dr17/manga/manga-data/data-model/#CUBEFiles) as well as derived [analysis maps](https://www.sdss4.org/dr17/manga/manga-data/data-model/#DataAnalysisPipelineOutput), among others.


## Data Preparation

All data are downloaded from the [SDSS Science Archive Server (SAS)](https://data.sdss.org/sas/dr17/manga/)

The following scripts can be used to download the following MaNGA data:

- the DRP [summary catalog](https://www.sdss4.org/dr17/manga/manga-data/catalogs/#DRPALLFile) file
- the DAP [summary catalog](https://www.sdss4.org/dr17/manga/manga-data/catalogs/#DAPALLFile) file
- the DRP [LOGCUBE](https://www.sdss4.org/dr17/manga/manga-data/data-model/#CUBEFiles) files
- the DAP [MAPS HYB10-MILESHC-MASTARSSP](https://www.sdss4.org/dr17/manga/manga-data/data-model/#MAPSFiles)

It should consist of approximately ~11273 IFU cubes, and ~10735 DAP maps files, totalling ~1.7 TB.

### Globus Transfer

You can use the `globus_transfer` Python script to download the data using the Globus software. It is necessary either to use an institutional Globus endpoint or to create a personal one following the instructions here: https://www.globus.org/globus-connect-personal.

This script should behave the same as the SDSS globus_transfer script.

Assuming you have created an endpoint with a given UUID you can submit a Globus transfer with the following script
```bash
python globus_transfer.py --destination_endpoint_id [endpoint_UUID] --destination_path [endpoint_download_path] --client_id [globus_client_id]
```
This will submit a series of transfer requests, which you can track from the globus website.

By default, this will only download the IFU data cubes and the DRPall summary catalog file, i.e. `--product cubes`.  To also download the maps data, you need to set `--product maps`.  To download everything, run both
```bash
# download cubes
python globus_transfer.py --destination_endpoint_id [endpoint_UUID] --destination_path [endpoint_download_path] --client_id [globus_client_id] -p cubes
# download maps
python globus_transfer.py --destination_endpoint_id [endpoint_UUID] --destination_path [endpoint_download_path] --client_id [globus_client_id] -p maps
```

### sdss-access

Alternatively, you can use the `access_tranfer` Python script to download the data with the [sdss-access](https://sdss-access.readthedocs.io/en/latest/) Python package, which uses parallelized `rsync` streams to download the data. This tool organizes the output download directory structure to mirror the official SDSS SAS.

To download all summary files, cubes and maps, run:
```bash
python access_transfer.py --destination_path .
```

### Dataset Creation

To create the dataset, use the `build_parent_sample.py` script. You only need to specify the input data directory, `manga_data_path`, the output data directory, `output_dir`, and the number of cpu processes to use.  `manga_data_path` should be the same as the `destination_path` from the download scripts.

For example, run:
```bash
python build_parent_sample.py --manga_data_path . --output_dir out --num_processes 1;
```

This script processes the data in batch groups organized by healpix id. It aggregates information from both the datacubes and maps files for a given plate-IFU target, and writes a single HDF5 file per healpix id, with each plate-IFU object a new group within the HDF5 file.

 During processing all IFU cubes, and maps have been resized to 96 x 96, with zero-padded elements added around the edges of the data.

## Dataset Structure

See the `demo_manga.ipynb` Jupyter notebook for an example of how to load and interact with the dataset.  This notebook loads the manga dataaset, gets an entry, inspects the structure of the data, and provides examples of how to plot individual spaxels, images and maps.

See [Feature Datamodel](feature_datamodel) for a complete description of the dataset features.


```python
from datasets import load_dataset

manga = load_dataset('manga.py', trust_remote_code=True, split='train', streaming=True)
manga = iter(manga.with_format('numpy'))

ii = next(manga)
```

### Feature Datamodel

The dataset structure is organized with the following features:

- object_id: the MaNGA plate-IFU
- ra: the Right Ascension of the target
- dec: the Declination of the target
- healpix: the healpix id
- z: the target redshift
- spaxel_size: the size of spaxel (spatial pixel)
- spaxel_size_units: the units of the spaxel size
- spaxels: a list of spaxels, with properties
- images: a list of reconstructed images, with properties
- maps: a list of derived maps, with properties

Each `spaxel` item has the following features:

- flux: the flux spectrum array in the spaxel
- ivar: the inverse variance array in the spaxel
- mask: the pixel quality mask array in the spaxel
- lsf: the line-spread-function array in the spaxel
- lambda: the wavelength array, same for all spaxels
- x: the x-array index spaxel coordinate
- y: the y-array index spaxel coordinate
- spaxel_idx: the 1d array index of the np.raveled spaxel
- flux_units: the physical units of the flux array
- lambda_units: the physical units of the wavelength array
- skycoo_x: the sky x offset in RA, in arcsec from galaxy center
- skycoo_y: the sky y offset in DEC, in arcsec from galaxy center
- ellcoo_r: the elliptical polar coordinate radial offset, from galaxy center, R in arcsec
- ellcoo_rre: the elliptical polar coordinate radial offset, from galaxy center, as R/Re
- ellcoo_rkpc: the elliptical polar coordinate radial offset, from galaxy center, R in h-1 kpc
- ellcoo_theta: the elliptical polar coordinate angle offset, from galaxy center, theta in degrees
- skycoo_units: the physical units of skycoo_x/y
- ellcoo_r_units: the physical units of ellcoo_r
- ellcoo_rre_units: the physical units of ellcoo_rre
- ellcoo_rkpc_units: the physical units of ellcoo_rkpc
- ellcoo_theta_units: the physical units of ellcoo_theta


Each `images` item has the following features:

- filter: the image band or filter, e.g. griz
- array: the 2d array pixel values
- array_units: the physical units of the data
- psf: the reconstructed PSF pixel values
- psf_units: the physical units of the data
- scale: the image pixel scale
- scale_units: the units of the image scale

Each `maps` item has the following features:

- group: the original MaNGA DAP extension, e.g. `emline_gflux`
- label: the full map label, made from the HDU extension name + channel, e.g. `emline_gflux_ha_6564`.
- array: the 2d array of pixel values
- ivar: the 2d array of inverse variance values
- mask: the 2d array of pixel quality mask values
- array_units: the physical units of the map data
