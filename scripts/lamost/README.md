# LAMOST Dataset Collection

This folder contains the scripts and queries used to build the LAMOST spectroscopic parent sample, based on optical spectra from the Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST).

## Sample selection

In the current version of the dataset, we do not apply any quality cuts. This would ensure maximum flexibility. Applying quality cuts can be done pn the local catalog.

## Data preparation
The first step is to download the desired LAMOST catalog, along with the individual spectra from the catalog. 
These can both be done automatically using provided download script, `download_data.py`.
You will specify the desired catalog, along with the output directory and maximum number of samples to download:
```python
python3 download_data.py --catalog_name LRS_cv --release dr10_v2.0 -o ./lrs_cv_spectra --max_rows 50 --n_workers 8 --delay 1.0 --timeout 30 --retries 3
```

Next, you can call `build_parent_sample.py` (with the catalog path, fits files path and an output folder path) to create an MMU-compatible dataset:
```python
python3 build_parent_sample.py dr10_v2.0_LRS_cv.fits ./lrs_cv_spectra/ .
```

Now, this dataset can be loaded with `datasets.load_dataset`!

A full example can be found in `test.sh`.


### Documentation

- LAMOST official website: http://www.lamost.org/
- LAMOST survey overview: https://ui.adsabs.harvard.edu/abs/2012RAA....12.1197C/abstract
