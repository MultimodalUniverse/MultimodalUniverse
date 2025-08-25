# LAMOST Dataset Collection

This folder contains the scripts and queries used to build the LAMOST spectroscopic parent sample, based on optical spectra from the Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST).

## Sample selection

In the current version of the dataset, we do not apply any quality cuts. This would ensure maximum flexibility. Applying quality cuts can be done pn the local catalog.

## Data preparation
<!-- The first step is downloading the desired LAMOST catalog. This step cannot be done automatically and therefore should be done manually prior to the dataset creation. Downloading LAMOST catalogs can be done from the LAMOST website. for example: -->
<!-- https://www.lamost.org/dr10/v2.0/catalogue would lead you to the version 2 of data release 10. There you can choose the desired catalog (for example Stellar parameters for A,F,G,K stars) and download the fits file (important - download fits and not csv file. The data generation script assume the catalog is in fits format). -->
<br>

The first step is to download the desired LAMOST catalog, along with the individual spectra from the catalog. 
These can both be done automatically using provided download script.
You will specify the desired catalog, along with the output directory and maximum number of samples to download.
For example:
<br>
```python
python download_data.py --catalog_name LRS_stellar --version dr10_v2.0 --output ./lrs_stellar --max_rows 2000
```
<br>
Next, you can call build_parent_sample (with the catalog path, fits files path and an output folder path) and create a dataset. See test.sh for example for these steps.


### Documentation

- LAMOST official website: http://www.lamost.org/
- LAMOST survey overview: https://ui.adsabs.harvard.edu/abs/2012RAA....12.1197C/abstract
