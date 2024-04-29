# Young Supernova Experiment Data Release 1 (YSE DR1) Dataset Collection

We gather in this folder all the scripts and queries used to build the YSE DR1 parent sample.

## Data preparation

### Downloading Data from Zenodo

The first step to data preparation is to download from Zenodo all relevant data to a local machine. The data will be downloaded by running the following script:
```bash
python download_data.py [path to directory where data will be downloaded]
```
e.g. `python download_data.py /mnt/ceph/users/flanusse/data/`

The total number of files downloaded should be around 2,000 light curve files, for a total download size of about 3 MB.

### Light Curve Extraction

Once the YSE DR1 data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to YSE DR1 data] [output directory]
```
e.g. `python build_parent_sample.py /mnt/ceph/users/flanusse/data/yse_dr1_zenodo/ /home/flanusse/AstroPile/yse_dr1/`

## Documentation

- Young Supernova Experiment Survey Website: https://yse.ucsc.edu/
- YSE DR1 Paper: Aleo P.D., et al., 2023, [ApJS](https://iopscience.iop.org/article/10.3847/1538-4365/acbfba), [266, 9](https://ui.adsabs.harvard.edu/abs/2023ApJS..266....9A/abstract)
- YSE DR1 Bulk Data Access Page: https://zenodo.org/records/7317476
