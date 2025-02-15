# Kepler Dataset Collection

This folder contains the scripts and queries used to build the Kepler photometric parent sample, based on 
a set of selection criteria for the Kepler DR25 light curves.

## Sample selection

In the current version of the dataset, we retrieve all long cadence optical spectra from the Kepler mission. There are currently no specific cuts applied, but these could be specified in the download_data.py file.

## Data preparation

### Downloading data through MAST/Astroquery

The first step to data preparation is to download all relevant data to a local machine through MAST/lightkurve. By default it will currently download the long cadence data.

It is recommended to download the raw fits files directly from the MAST website at https://archive.stsci.edu/missions-and-data/kepler/kepler-bulk-downloads

Alternatively, you can download the data through the following script
```bash
python download_data.py [--tiny] [path to catalog] [output path]
```
e.g. `python mast_s3_transfer.py --tiny ./kepler_catalog_dr25.csv/ ./fits_data` 

the catalog in the argument should consists of KID/KIC identifiers. This can be, for example, the Kepler input catalog which can be downloaded from https://archive.stsci.edu/pub/kepler/catalogs/ 

note that this script will only save the pdcsap flux (and not the sapflux).

The total number of files downloaded should be around 197,000.

### Light curve extraction

Once the Kepler data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to data catalog] [path to Kepler data] [output directory] --num_processes [1] [--tiny];
```
e.g. `python build_parent_sample.py ./all_kepler_samples ./fits_data/ ./tess_data_hdf5/ --num_processes 1 --tiny`

The data catalog should consists of at least the following columns:
    <br>
    **KID** - the kepler identifier number
    <br>
    **data_file_path** - a list of paths for a specific sample. becasue Kepler is divided into quarters, this might be a list of different file paths, one for each quarter. if your data is not seperated into quarters (or all the quarters have been conatenated into one file), this should be a list with one element - the path of the single file.
    <br>
### Documentation

- Kepler Data: https://archive.stsci.edu/missions-and-data/kepler
