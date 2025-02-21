# VIPERS Data Collection

This folder contains the scripts and queries used to build the "VIMOS Public Extragalactic Redshift Survey" (VIPERS) spectroscopic parent sample, based on 
all optical spectra available in the VIPERS Final Public Release.

## Sample selection

In the current version of the dataset, we retrieve all optical spectra from the VIPERS Final Public Data Release from 2016 (PDR-2). This includes 90,000 galaxies with red magnitude I(AB) brighter than 22.5 over an overall area of nearly 24 square degrees.

## Data preparation

### Downloading the Data and Spectra Extraction

The VIPERS data can be downloaded and correctly extracted using the following script:
```bash
python build_parent_sample.py [output directory]
```
e.g. `python build_parent_sample.py /home/lparker/MultimodalUniverse/vipers`

This will automatically download the data to the output directory, and then save the data using the MultimodalUniverse convention in that same directory. The default HEALPIX set up uses `NSIDE=16`, however this can be manually changed with the `--nside` flag. 

### Units

The VIPERS spectra flux is natively stored in units of erg cm**(-2) s**(-1) angstrom**(-1), and correspondingly the noise is stored in erg cm**(-2) s**(-1) angstrom**(-1) and the wavelength in angstroms. To be consistent with the other spectra datasets, we normalize the spectra fluxes to 10**(-17) erg cm**(-2) s**(-1) angstrom**(-1). Correspondingly, we convert the spectra noise to inverse variance, also in units 10**(-17) erg cm**(-2) s**(-1) angstrom**(-1).

### Documentation

- VIPERS homepage: http://vipers.inaf.it/
- VIPERS documentation: http://vipers.inaf.it/data/pdr2/catalogs/PDR2_SPECTRO_TABLES.html

