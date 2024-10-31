# TESS Dataset Collection

This folder contains the scripts and queries used to build the TESS photometric parent sample, based on 
a set of selection criteria for the TESS-SPOC light curves.

## Sample selection

In the current version of the dataset, we retrieve all lightcurves from the TESS SPOC (Science Processing Operations Center) pipeline for sectors 56, 58, 60, 63, 64, 67, and 69. There are currently no specific cuts applied.

## Data preparation

### Downloading data

The first step to data preparation is to download all relevant data to a local machine. The easiest way is to download the following scripts from the [official TESS site](https://archive.stsci.edu/hlsp/tess-spoc):
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0056_tess_v1_dl-lc.sh
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0058_tess_v1_dl-lc.sh
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0060_tess_v1_dl-lc.sh
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0063_tess_v1_dl-lc.sh
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0064_tess_v1_dl-lc.sh
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0067_tess_v1_dl-lc.sh
- https://archive.stsci.edu/hlsps/tess-spoc/download_scripts/hlsp_tess-spoc_tess_phot_s0069_tess_v1_dl-lc.sh

These scripts will each download one sector of data into the directory you run the script from. These scripts can take quite a few hours so we'd recommend running them in a screen or tmux session. The total number of files downloaded should be around 160,000 per sector.

### Light curve extraction

The above scripts will download each sector's data into a directory called `s00xx`, where `xx` is the sector number. All the lightcurves will be inside the `s00xx/target/0000` subdirectory. Once the data has been downloaded, you can create the parent sample by running the following script for each sector:
```bash
python build_parent_sample.py [path to TESS data] [output directory] --num_processes [1] [--tiny];
```
e.g. `python build_parent_sample.py data/path/s00xx/target/0000 ./tess_s00xx_hdf5/ --num_processes 1 --tiny`

### Merge the sectors

Finally, to put all the lightcurves together into a single directory, run the following script:
```bash
python merge_directories.py [path to one sector] [path to another sector] ...
```
e.g. `python merge_directories.py tess_s0056_hdf5 tess_s0058_hdf tess_s0060_hdf tess_s0063_hdf tess_s0064_hdf tess_s0067_hdf tess_s0069_hdf`

