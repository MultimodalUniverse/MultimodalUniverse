# PROVABGS Dataset Collection

This folder contains the scripts and queries used to build the PROVABGS catalog based on Hahn et al., 2022
https://changhoonhahn.github.io/provabgs/current/

## Data preparation

### Downloading data

The first step is to download the PROVABGS HDF5 file. It should be available soon here:
https://changhoonhahn.github.io/provabgs/current/

### Spectra extraction

Once the SDSS data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to PROVABGS hdf5] [output directory]
```
e.g. `python build_parent_sample.py /mnt/ceph/users/lparker/provabgs.hdf5 /home/flanusse/AstroPile/provabgs`

### Documentation

- PROVABGS main page: https://changhoonhahn.github.io/provabgs/current/
- Github for PROVABGS: https://github.com/changhoonhahn/provabgs
- Hahn et al., 2022 Paper: https://arxiv.org/abs/2202.01809

