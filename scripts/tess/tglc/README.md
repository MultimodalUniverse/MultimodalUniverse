
# TGLC Dataset Builder

This folder contains scripts used to build the TESS-Gaia lightcurve dataset developed by *Han & Brandt 2023* and is currently available for TESS sectors (1-39) ([TGLC website](https://archive.stsci.edu/hlsp/tglc)).

## Data preparation 
To download a full sector and store it in the standardised format, simply call:

```
python build_parent_sample.py -s 23 --tglc_data_path './tglc_data' 
--hdf5_output_path './tglc_data/s0023/MultimodalUniverse' 
--fits_output_path './tglc_data/s0023/fits_lcs' --n_processes 4 --tiny
```

Or, it is also possible edit the shell script with your desired parameters using ```build_dataset.sh```.

The ```-s``` flag determines which sector to download (**i.e. sector 23** ). The following flags: ```--tglc_data_path```, ``` --hdf5_output_path``` and ```--fits_output_path ``` specify respectively where the tglc data should be stored, where the standardised hdf5 files are saved and where the fits files from MAST are kept. ```--n_processes``` allows the downloads and processing of the data to be distributed across cores, if that is available. ```--tiny``` is a boolean flag which can be used for testing, which currently uses 100 samples. This can be changed by modifying ```_TINY_SIZE``` in ```build_parent_sample.py```.

Under the hood, this calls the following code in python, which is as simple and can be used if preferred:

```
from build_parent_sample import TGLC_Downloader

tglc_downloader = TGLC_Downloader(
        sector = sector_num, 
        tglc_data_path = tglc_data_path, 
        hdf5_output_dir = hdf5_output_path,
        fits_dir = fits_output_path,
        n_processes = n_processes
    )
    tglc_downloader.download_sector(tiny = tiny)
```

### Finer control over the data processing
For more control over the data processing, it is possible to make use of methods in the ```TGLC_Downloader``` class. 
The following sections will demonstrate this functionality.

### Downloading the data 
The first step is to download the TGLC data from [TGLC website](https://archive.stsci.edu/hlsp/tglc). This can be done for an entire sector by first building a catalog of the available : 

```
from build_parent_sample import TGLC_Downloader

tglc_downloader = TGLC_Downloader(
        sector = sector_num, 
        tglc_data_path = tglc_data_path, 
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

Instantiate the ```TGLC_Download``` class and then create a catalog of all desired objects. The tiny flag can be set for testing by specifying save_catalog, the catalog can be kept locally for use. The  ```batched_download``` method can then be called for downloading the fits files from MAST. The queries are split into batches of 5000 and for the objects in each batch are processed in parallel. 

### Standardizing the data format 
Secondly, the data is formatted into a similar style to the TESS-SPOC lightcurves. The raw fits files are converted into hdf5 with the following structure (although this can be easily modified to include more or less features from TGLC):

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

### Using the newly created dataset

Using the ```datasets``` library, the dataset can be loaded easily. For example:

```
from datasets import load_dataset
from datasets.data_files import DataFilesPatternsDict

fp = "./tglc_data/s0023/MultimodalUniverse/tglc.py"
data_files = DataFilesPatternsDict.from_patterns({"train": ["TGLC/healpix=*/*.hdf5"]})
tglc = load_dataset(fp, data_files = data_files, trust_remote_code=True)
dset = tglc.with_format('numpy')['train']
```

For a slightly more comprehensive demonstration including plotting a light curve, see the example notebook ```tglc_example.ipynb```.