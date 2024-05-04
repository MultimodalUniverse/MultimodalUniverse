# Supernova Legacy Survey (SNLS) Dataset Collection

We gather in this folder all the scripts and queries used to build the SNLS parent sample.

## Data preparation

### Downloading Data from GitHub

The first step to data preparation is to download all relevant data from the PantheonPlusSH0ES GitHub to a local machine. The data will be downloaded by running the following script:
```bash
python download_data.py [path to directory where data will be downloaded]
```
e.g. `python download_data.py /mnt/ceph/users/flanusse/data/`

The total number of files downloaded should be around 239 light curve files, for a total download size of about 2.3 MB.

### Light Curve Extraction

Once the SNLS data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to SNLS Data] [output directory]
```
e.g. `python build_parent_sample.py /mnt/ceph/users/flanusse/data/snls/ /home/flanusse/AstroPile/snls/`


## HuggingFace Dataset Format
Each example contains:

  - `object_id`: Dataset specific identifier
  - `obj_type`: Spectral class
  - `ra`, `dec`: right ascension, declination
  - `redshift`: redshift of target. : As decided by ordered availability of: `['REDSHIFT_FINAL', 'REDSHIFT_CMB', 'REDSHIFT_HELIO']`
  - `host_log_mass`: host mass. As decided by ordered availability of: `['HOST_LOGMASS', 'HOSTGAL_LOGMASS']` 
  - `lightcurve`: object with:
        - `band`: band of observation, arr(string),
        - `time`: timestamp of observation in MJD, arr(float)
        - `flux`: flux value of observation, arr(float),
        - `flux_err`: flux uncertainty of observation, arr(float)

## Documentation

- SNLS Website: https://irfu.cea.fr/en/Phocea/Vie_des_labos/Ast/ast_technique.php?id_ast=2289
- SNLS SN Ia Paper: Guy et al. (2010) [A&A](https://www.aanda.org/articles/aa/pdf/2010/15/aa14468-10.pdf)
- PantheonPlusSH0ES Data: https://github.com/PantheonPlusSH0ES/DataRelease/tree/main/Pantheon%2B_Data/1_DATA/photometry
- PantheonPlusSH0ES Paper: Scolnic et al. (2022) [ApJ](https://iopscience.iop.org/article/10.3847/1538-4357/ac8b7a/pdf)