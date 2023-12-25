# DESI Dataset Collection

We gather in this folder all the scripts and queries used to build the DESI spectroscopic parent sample.

## Sample selection

In the current version of the dataset, we select all the objects from the one percent survey, i.e. sv3 from the 
DESI Early Data Release applying only the following cuts:
```
  - SURVEY = 'sv3'         # Only use data from the one percent survey
  - SV_PRIMARY is true     # Only use the primary spectrum for each object
  - OBJTYPE = 'TGT'        # Only use targets (ignore sky and others)
  - COADD_FIBERSTATUS = 0  # Only use fibers with good status
```

## Data preparation

### Downloading data through Globus

The first step to data preparation is to download through globus all relevant data to a local machine. It is necessary either to use an institutional Globus endpoint or to create a personal one following the instructions here: https://www.globus.org/globus-connect-personal.

Assuming you have created an endpoint with a given UUID you can submit a Globus transfer with the following script
```bash
python globus_transfer.py [your endpoint UUID] [path on your endpoint to download data]
```
This will submit a transfer request, which you can track from the globus website.

The total size of the downloaded data is ~ 2 TB.

### Spectra extraction

**Step I: Install DESI dependencies**: The first step is to install some DESI specific packages to help with the handling of spectra. Just run
the following lines:
```bash
pip install fitsio speclite numba git+https://github.com/desihub/desiutil.git
pip install git+https://github.com/desihub/desispec.git git+https://github.com/desihub/desitarget.git git+https://github.com/desihub/desimodel.git
```

**Step II: Run the processing script**: Once the DESI has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to DESI data] [output directory]
```
This is essentially limited by the speed of io, it takes around 15 mins, and requires around 100 GB of RAM (I know...  feel free to submit a PR for a more memory efficient implementation ;-) ).

This process will generate the following files:
- `desi_spectra.hdf5`: The parent sample spectra file in HDF5 format.
- `desi_catalog.fits`: The parent sample FITS catalog file, in same order as the spectra file.

## Documnentation

- [DESI data model for zpix-survey-program](https://desidatamodel.readthedocs.io/en/latest/DESI_SPECTRO_REDUX/SPECPROD/zcatalog/zpix-SURVEY-PROGRAM.html#hdu1)
