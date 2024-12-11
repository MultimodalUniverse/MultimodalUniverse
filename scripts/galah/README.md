# GALAH Dataset Collection

This folder contains the scripts used to build the GALAH DR3 spectroscopic sample.

Note here that there are no cuts on the data, apart from removing objects for which spectra are (partially) missing. Refer to the GALAH DR3 best practices document for more information on how to use the data.

## Data preparation

The data preparation process is divided into two steps:

1. Downloading the data from the GALAH DR3 website
2. Preprocessing the spectra tarballs
3. Processing the data

The download script, `download.py`, will take care of downloading the main catalog, VACs, spectra tarballs, and some auxiliary files. Note that this will take ~385GB for the full dataset.

The spectra then need to be preprocessed using the `prepare.sh` script, which will put things into a nice format for processing. This should be done on a filesystem that can handle ~2M files in a single directory.

Finally, the `build_parent_sample.py` script will process the data and convert it into format that is consistent with The Multimodal Universe's other datasets.

## Documentation

- GALAH DR3 overview https://www.galah-survey.org/dr3/overview/
- GALAH DR3 best practices https://www.galah-survey.org/dr3/using_the_data/
