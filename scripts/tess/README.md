
# Dataset builders for the TESS FFI pipelines

This folder contains scripts used to build lightcurve datasets from a number of pipelines including: TGLC developed by [Han & Brandt 2023](https://iopscience.iop.org/article/10.3847/1538-3881/acaaa7), QLP ([Huang et al. 2020](https://arxiv.org/abs/2011.06459)) and SPOC ([Caldwell et al. 2020](https://ui.adsabs.harvard.edu/abs/2020RNAAS...4..201C/abstract)). Note, currently there are speed-ups in download times with distributed downloading implemented in this version.

The methods used to extract light curves in each pipeline vary signficantly. For detailed reference of their implementations see the linked papers above. At a high level, the SPOC pipeline include targets within 100 pc that are brighter than $H <10$. These targets have been traditionally used for exoplanet searches with de-trended light curves available for this purpose; pre-search data conditioning (PDC) is used to remove long term trends using co-trending basis vectors. Simple aperture photometry is also available and is recommended for applications focused on stellar variability.

The QLP pipeline focuses on similar data products as SPOC providing simple aperture photometry measurements. Detrending of stellar lightcurves using a spline-based method is also produced in the data products offering a different methodology to SPOC.

The TGLC pipeline uses GAIA DR3 as priors to extract targets from TESS full-frame images that are on the order of 2.5 (TESS) magntiudes dimmer than those from SPOC/QLP and demonstrates enhanced deblending capabilities. These increases in processing have increased the number of availble lightcurves from TESS by at least an order of magnitude.

## Data preparation 
To download a full sector and store it in the standardised format, simply call:

```
python build_parent_sample.py --pipeline ['qlp', 'tglc', 'spoc'] -s [sector_number] --data_path './data' 
--hdf5_output_path './data/' 
--fits_output_path './data/fits' --n_processes 4 --tiny
```

The ```-s``` flag determines which sector to download (**i.e. sector 23**). The following sectors are available for the current pipelines (as of 10/02/2025; see links for updates):
- [TGLC](https://archive.stsci.edu/hlsp/tglc): 1-52 ()
- [SPOC](https://archive.stsci.edu/hlsp/tess-spoc): 1-83
- [QLP](https://archive.stsci.edu/hlsp/qlp): 1-83


The following flags: ```--data_path```, ``` --hdf5_output_path``` and ```--fits_output_path ``` each specify where the root data path, where the standardised hdf5 files are saved and where the fits files from MAST are kept. ```--n_processes``` allows the downloads and processing of the data to be distributed across cores. ```--tiny``` is a boolean flag which can be used for testing, which currently uses 100 samples. This can be changed by modifying ```_TINY_SIZE``` in ```build_parent_sample.py```.


### Finer control over the data processing
For more control over the data processing, it is possible to make use of methods in the ```TESS_Downloader``` child classes. 
The following sections will demonstrate this functionality for the TGLC downloader but classes are available for SPOC (```SPOC_Downloader```) and QLP (```QLP_Downloader```).

### Downloading the data 
The first step is to download the TGLC data from [MAST](https://archive.stsci.edu/hlsp/tglc). For example for the TGLC pipeline, this can be done for an entire sector by first building a catalog of the available : 

```
from tglc import TGLC_Downloader

tglc_downloader = TGLC_Downloader(
        sector = sector_num, 
        data_path = tglc_data_path, 
        hdf5_output_dir = hdf5_output_path,
        fits_dir = fits_output_path,
        n_processes = n_processes
)

tiny = True # Or False for a full sector download
save_catalog = True # Flag whether to save the catalog to memory or not.

catalog = tglc_downloader.create_sector_catalog(save_catalog = save_catalog, tiny = tiny) 

tglc_downloader.batched_download(
    catalog = catalog,
    tiny = tiny
)
```

Instantiate the ```TGLC_Download``` class and then create a catalog of all desired objects. By specifying save_catalog to be true, the catalog can be kept locally for use. The ```tiny``` flag can be set for testing. The  ```batched_download``` method can then be called for downloading the fits files from MAST. Large queries are split into batches of 5000 and for the objects in each batch are processed in parallel. 

### Standardizing the data format 
Secondly, the data is formatted into style suitable for the Multimodal Universe. The raw fits files are converted into hdf5 with the following structure for TGLC (although this can be easily modified to include more or less features from TGLC):

```
example = {
    'lightcurve' : {
        'time': arr_like,
        'psf_flux': arr_like,
        'aper_flux':  arr_like,
        'tess_flags':  arr_like,
        'tglc_flags':  arr_like
    }, 
    'RA':  float,
    'DEC':  float,
    'TIC_ID': float,
    'gaiadr3_id': float,
    'psf_flux_err': float,
    'aper_flux_err': float
}
```

To process and save the data in the hdf5 format, the following code can be called:

```
tglc_downloader.convert_fits_to_standard_format(catalog)
```

where catalog is an astropy table that contains all of the entries from the sector or your tiny sample.

### Using the dataset

Using the ```datasets``` library, the dataset can be loaded as follows:

```
from datasets import load_dataset
from datasets.data_files import DataFilesPatternsDict

fp = "./tglc.py"
data_files = DataFilesPatternsDict.from_patterns({"train": ["./tiny_tglc/TGLC/healpix=*/*.hdf5"]}) # For data_path = ./tiny_tglc
tglc = load_dataset(fp, data_files = data_files, trust_remote_code=True)
dset = tglc.with_format('numpy')['train']
```

For a more comprehensive demonstration of each of the pipelines including plotting light curves, see the example notebook ```examples.ipynb```.

### Testing 

The ```test``` folder contains a series of tests for each pipeline and each respective component of the download and processing procedure. By installing pytest the download code can be checked for all piplines on the tiny sample of sector 23 by running the following on the command line:

```
cd tests
python test.py --pipeline [all, spoc, tglc, qlp] 
```