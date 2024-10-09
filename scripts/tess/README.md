# TESS Dataset Collection

This folder contains the scripts and queries used to build the TESS photometric parent sample, based on 
a set of selection criteria for the TESS-SPOC light curves.

## Sample selection

In the current version of the dataset, we retrieve all optical spectra from the TESS SPOC (Science Processing Operations Center) pipeline for sector 64. There are currently no specific cuts applied, but these could be specified in the mast_transfer.py file.

## Data preparation

### Downloading data through MAST/Astroquery

The first step to data preparation is to download all relevant data to a local machine through MAST. By default it will currently download one sector of data.

You can download the data through the following script
```bash
python mast_transfer.py [--tiny] [path on your endpoint to download TESS data]
```
e.g. `python mast_transfer.py --tiny ./tess_data/`
This will submit a series of transfer requests.

The total number of files downloaded should be around 160,000 per sector.

### Light curve extraction

Once the TESS data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to TESS data] [output directory] --num_processes [1] [--tiny];
```
e.g. `python build_parent_sample.py ./tess_data/ ./tess_data_hdf5/ --num_processes 1 --tiny`

### Documentation

- TESS SPOC Data: https://archive.stsci.edu/hlsp/tess-spoc