# GALEX

This is an implementation of an MMU-compatible version of the [Bianchi+ 2017](https://ui.adsabs.harvard.edu/abs/2017ApJS..230...24B/abstract) GUVcat_AIS catalog of GALEX UV sources.

This catalog is a clean version of the comprehensive sky survey in far-UV (FUV, λ$_{eff}$ ∼ 1528 Å), and near-UV (NUV, λ$_{eff}$ ∼ 2310 Å) and provides measurements for 82,992,086 sources.

## Data preparation

Run the following scripts in order:

- `download.py`: downloads the catalog
- `process.py`: processes the catalog into an MMU-ready format
- `merge.py`: merges files within a healpix into a single file
- `cleanup.py`: cleans up directories after a merge

All scripts have a `--help` flag that provides more information on how to use them.
