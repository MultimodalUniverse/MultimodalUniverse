# SDSS Dataset Collection

This folder contains the scripts and queries used to build the SDSS spectroscopic parent sample, based on 
all optical spectra at the time of dr18.

## Sample selection

In the current version of the dataset, we retrieve all optical spectra from SDSS, SEGUE, BOSS, eBOSS. Only the following
cuts are applied:
```
    - SPECPRIMARY = 1            # Avoid duplicated spectra for the same object
    - PLATEQUALITY = 'good'      # Only good plates
    - TARGETTYPE = 'science'     # Only science targets (no sky, calibration, etc.)
```
As a result, our sample contains quasars, galaxies, stars, and objects for which the pipeline may have struggled
to classify.

## Data preparation

### Downloading data through Globus

The first step to data preparation is to download through globus all relevant data to a local machine. It is necessary either to use an institutional Globus endpoint or to create a personal one following the instructions here: https://www.globus.org/globus-connect-personal.

Assuming you have created an endpoint with a given UUID you can submit a Globus transfer with the following script
```bash
python globus_transfer.py [your endpoint UUID] [path on your endpoint to download data]
```
e.g. `python globus_transfer.py c3dc2ae2-74c6-11e8-93bb-0a6d4e044368 /mnt/ceph/users/flanusse/SDSS/`
This will submit a series of transfer requests, which you can track from the globus website.

The total number of files downloaded should be around 5 millions, for a total download size of about XXXX TB.

### Spectra extraction

Once the SDSS data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to SDSS data] [output directory]
```
e.g. `python build_parent_sample.py /mnt/ceph/users/flanusse/SDSS /home/flanusse/AstroPile/sdss`

### Documentation

- SDSS datamodel https://data.sdss.org/datamodel/
- Bulk data access page: https://www.sdss.org/dr18/data_access/bulk/


