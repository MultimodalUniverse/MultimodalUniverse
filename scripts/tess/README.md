# TESS Dataset Collection

This folder contains the scripts and queries used to build the TESS photometric parent sample, based on 
a set of selection criteria for the TESS-SPOC light curves.

## Sample selection

In the current version of the dataset, we retrieve all optical spectra from the TESS SPOC (Science Processing Operations Center) pipeline for sector 64. There are currently no specific cuts applied, but these could be specified in the mast_s3_transfer.py file.

## Data preparation

### Downloading data through Globus

The first step to data preparation is to download all relevant data to a local machine through MAST. By default it will currently download one sector of data

Assuming you have created an endpoint with a given UUID you can submit a Globus transfer with the following script
```bash
python mast_s3_transfer.py [--tiny] [path on your endpoint to download data]
```
e.g. `python mast_s3_transfer.py  --tiny ./tess_tiny/`
This will submit a series of transfer requests.

The total number of files downloaded should be around 150,000 per sector, for a total download size of about XXXX TB.

### Light curve extraction

Once the TESS data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to TESS data] [output directory]
```
e.g. `python build_parent_sample.py ./tess_tiny/ /home/flanusse/AstroPile/tess`

### Documentation

- TESS SPOC Data: https://archive.stsci.edu/hlsp/tess-spoc


