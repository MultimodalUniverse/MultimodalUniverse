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

Once the DESI has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to DESI data] [output directory]
```
