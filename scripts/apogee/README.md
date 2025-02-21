# SDSS APOGEE Dataset Collection

This folder contains the scripts and queries used to build the SDSS APOGEE/Milky Way Mapper spectroscopic parent sample, based on
all infrared spectra at the time of DR17.

## Sample selection

In the current version of the dataset, we retrieve near infrared spectra from SDSS APOGEE. Only the following
cuts are applied:

```
    - TELESCOPE = "apo25m" or "lco25m"
```

## Data preparation

### Downloading data

The data can be downloaded from Globus using the included ./download.sh script. You will need to put your Globus endpoint in the file. The spectrum extraction script will also try to (slowly) download any missing files.

The scripts will get all the `apStar/asStar*.fits` files (visit spectra) for the APO25M/LCO25M telescopes, as well as the `aspcapStar*.fits` files (pseudo-continuum normalized spectra) for both telescopes.

### Spectra extraction

You can create the parent sample by running the following script:

```bash
python build_parent_sample.py --apogee_data_path=[path to SDSS data] --output_dir=[output directory]
```

e.g. `python build_parent_sample.py --apogee_data_path=/mnt/data_overflow/sdss/apogee/dr17/apogee --output_dir /mnt/data_overflow/MultimodalUniverse/apogee` --num_processes 40

### Documentation

- APOGEE datamodel https://data.sdss.org/datamodel/
- Bulk data access page: https://www.sdss.org/dr18/data_access/bulk/
