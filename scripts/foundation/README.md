# Foundation Data Release 1 (Foundation DR1) Collection

We gather in this folder all the scripts and queries used to build thE Foundation DR1 parent sample.

## Data preparation

### Downloading Data from GitHub

The first step to data preparation is to download all relevant data from the PantheonPlusSH0ES GitHub to a local machine. The data will be downloaded by running the following script:
```bash
python download_data.py [path to directory where data will be downloaded]
```
e.g. `python download_data.py /mnt/ceph/users/flanusse/data/`

The total number of files downloaded should be around 180 light curve files, for a total download size of about 0.8 MB.

### Light Curve Extraction

Once the Foundation DR1 data has been downloaded, you can create the parent sample by running the following script:
```bash
python build_parent_sample.py [path to Foundation DR1 Data] [output directory]
```
e.g. `python build_parent_sample.py /mnt/ceph/users/flanusse/data/foundation_dr1/ /home/flanusse/AstroPile/foundation_dr1/`


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

- Foundation DR1 Papers: Foley et al. (2018) [MNRAS](https://watermark.silverchair.com/stx3136.pdf?token=AQECAHi208BE49Ooan9kkhW_Ercy7Dm3ZL_9Cf3qfKAc485ysgAAA08wggNLBgkqhkiG9w0BBwagggM8MIIDOAIBADCCAzEGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMCgZ7JXmAx046_kVfAgEQgIIDAhM_bvBbJLQo00AnsUBMr5UNpLhfW7Ff4LHbZJF8pI7BcOznaxL2f8OMA0sS5-AEGt_AXVHp4RV7zu6dx5jojgqxwYKGSyZ3xMY7bDuMahu9qGWpRyDz0TYZ4IHXEuNatfSi0-pIZ0gJqaYwowH-978GYMJ_569uEh_FPKeuUb98O-HTGAnJeZslcG1bH-hTPoHzN0vcAR57vMRieoIk4l2It-Q-BadDddl804MLlrs5G8PlqhZCY4sAEzlHb7t82D2WzAl41DYcmjHuFpd321K2yVa-13Um5QbAnCp9TEZtSatR1GVtSGBmPb2J-VAt6s9rUsLtqfrRYYtR-g1gqygeS9JtjKAtXXWpcAmkyIkyHAJEH_aV-kD-7HrEa1SGhWXrvZVF9mf-MyH47nRARKHk4xH_Fa97AwpsbYQ5zXJ8q0uAS_Lqy7FAmp4iOo_RqpxMuWgxY82ZP9ERHT_YiJ40rXBt0EZxbccCsDZlY4lS7eeIBtuvkzvssO1lO7fhGc9UGB-789FwRlLwGhIUsk-ZdrfT7u18KoSaKcAhXiIsr-3prTgG8agISiqX2sVxbCGp4oi2h0BpLdDa5bSn-begeSNZrO5oEa1fX9Gks7SLlVhNDsIMr5tL_5Ebcw_01S_GZDU1UnpHtPnF-fYhtpQW-c-l2MsmbokBnP6VWiFFv8vctAMTXr7y62BGzXmK58oCO1LGKcCfx0N4JwsVj3l6WeoKIc2ZPNZM4avpltwJdVp3g52ink3yexw_S6539UAALJdAFuwEMZKVYhaxCX8EFb0ox0rEbY2gQKceEyN4ItVUWIWaMiDdQXNLgDCs-ug3U8w5dEBJqpH9Yh7PwcI7a2oeYhdXZTesCBEGtaVAmDyhNkwZGFYEyxjLCLwhLesHOchp42mkFDEqTgprrSTrcPEd5tOLHUC4soKJlnpf6-79GSk8m6LXin0OSCvDU2FW_3E0PzCI-nw0CP_VNJx6spCpC5x9hBc2nPvBpQog9znXBXBfWHKKjH_6axEgma-v) Jones et al. (2019) [ApJ](https://iopscience.iop.org/article/10.3847/1538-4357/ab2bec/pdf)
- PantheonPlusSH0ES Data: https://github.com/PantheonPlusSH0ES/DataRelease/tree/main/Pantheon%2B_Data/1_DATA/photometry
- PantheonPlusSH0ES Paper: Scolnic et al. (2022) [ApJ](https://iopscience.iop.org/article/10.3847/1538-4357/ac8b7a/pdf) 

