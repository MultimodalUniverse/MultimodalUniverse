# Kepler Dataset Collection

This folder contains the scripts and queries used to build the Kepler photometric parent sample, based on 
a set of selection criteria for the Kepler DR25 light curves.

## Sample selection

In the current version of the dataset, we retrieve all long cadence optical spectra from the Kepler mission. There are currently no specific cuts applied, but these could be specified in the download_data.py file.

## Data preparation

### Downloading data through MAST/Astroquery

The first step to data preparation is to download all relevant data to a local machine through MAST/lightkurve. By default it will currently download the long cadence data.

You can download the data through the following script
```bash
python download_data.py [--tiny] [path to catalog with KICs]
```
e.g. `python mast_s3_transfer.py --tiny ./kepler_catalog_dr25.csv/`
by defaulte it will save the fits files in a directory called './fits_data'.

The total number of files downloaded should be around 197,000 per

### Light curve extraction

Once the Kepler data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to data catalog] [path to Kepler data] [output directory] --num_processes [1] [--tiny];
```
e.g. `python build_parent_sample.py ./all_kepler_samples ./fits_data/ ./tess_data_hdf5/ --num_processes 1 --tiny`

the data catalog should ocnsists of the following columns:
    <br>
    **KID** - the kepler identifier number
    <br>
    **data_file_path** - a list of paths for a specific sample. becasue Kepler is divided into quarters, this might be a list of different quarter files. if the          fits files are a concatenations of all quarters, this should be a list with one element.
    <br>
    **qs** - a list with the specific quarters used in this sample. This argument is currently not being used in the data processing and can be an empty list if          the this information is missing

### Documentation

- Kepler Data: https://archive.stsci.edu/missions-and-data/kepler
