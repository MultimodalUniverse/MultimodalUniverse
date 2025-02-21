# PROVABGS Dataset Collection

This folder contains the scripts and queries used to build the PROVABGS catalog based on Hahn et al., 2022. (https://arxiv.org/abs/2202.01809)

## Data preparation

The important script to use for downloading and processing the PROVABGS catalog is here:

```
python build_parent_sample.py [location to save PROVABGS file] [output directory]
```

This script will automatically download the PROVABGS catalog from DESI if it has not already been downloaded. Then, the script will call
```
_get_best_fit
```
This function uses the PROVABGS Bayesian SED modeling of galaxy spectra to calculate the best-fit parameters of the physical properties of the galaxies in the PROVABGS catalog. This returns a best-fit estimation of each galaxy's:

- Z_mw: The Mass-Weighted Metallicity
- Tage_mw: The Mass-Weighted Stellar Age
- SFR: The Average SFR 

Additionally, the PROVABGS catalog already provides estimates of galaxy redshift (Z) and of galaxy stellar mass (PROVABGS_LOGMSTAR).

Once processed, the data are split into folders according to HEALPIX region, and saved according to the MultimodalUniverse convention.

## Documentation

- PROVABGS main page: https://changhoonhahn.github.io/provabgs/current/
- Github for PROVABGS: https://github.com/changhoonhahn/provabgs
- Hahn et al., 2022 Paper: https://arxiv.org/abs/2202.01809
- Data: https://data.desi.lbl.gov/public/edr/vac/edr/provabgs/v1.0/BGS_ANY_full.provabgs.sv3.v0.hdf5

