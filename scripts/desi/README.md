# DESI Dataset Collection

We gather in this folder all the scripts and queries used to build the DESI spectroscopic parent sample.

## Sample selection

In the current version of the dataset, we select all the objects from the main sample of
DESI DR1 applying only the following cuts:

```txt
SURVEY = "main"        # Only use data from the main survey
SV_PRIMARY = True      # Only use the primary spectrum for each object
OBJTYPE = "TGT"        # Only use targets (ignore sky and others)
COADD_FIBERSTATUS = 0  # Only use fibers with good status
```

## Data preparation

### Downloading data through Globus

[Globus](https://docs.globus.org/) is a file-downloading utility. It is used to schedule large downloads like the raw data of the DESI dataset.

The first step to data preparation is thus to install Globus. It is necessary either to use an institutional Globus endpoint or to create a personal one following the instructions here: <https://www.globus.org/globus-connect-personal>.
To create a personal one you will need to download and uncompress the archive. Then, on the machine you want to perform the download, you will have to start the Globus service:

```bash
./globusconnectpersonal -start -debug -restrict-paths <path_where_to_download_the_data>
```

Assuming you have created an endpoint with a given UUID you can submit a Globus transfer with the following script

```bash
python globus_transfer.py [your endpoint UUID] [path on your endpoint to download data]
```

This will ask you to authenticate before starting the transfer. You can track the status of the transfer on Globus website.

The total size of the downloaded data is ~ 10 TB.

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

This is essentially limited by the speed of io, it takes around 15 mins, and requires around 10 GB of RAM per parallel process.

This process will generate export the dataset in standard format at in the output directory.

## Documentation

- [DESI data model for zpix-survey-program](https://desidatamodel.readthedocs.io/en/latest/DESI_SPECTRO_REDUX/SPECPROD/zcatalog/zpix-SURVEY-PROGRAM.html#hdu1)
