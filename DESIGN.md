# Design Document for the AstroPile


## Goals and Scope

The AstroPile aims to build a large dataset of diverse astronomical data that may be used to build
astronomical foundation models.

The current types of data that are being collected are:
  - Multiband images from different surveys and instruments
  - Spectra from different instruments

Contrary to previous datasets collected for machine learning applications, the AstroPile does not 
format all data modalities into a uniform way, but rather collects sufficient and necessary metadata
to allow machine learning models to understand the context of each observation. 

The AstroPile also includes pairing between different data modalities for the same astronomical objects. 


## Dataset Format and Architecture

The organization of the data is based on the following strategy:

  - Each source survey is packaged as a separare parent dataset.
  - An `Example` in these parent datasets consists of all observations of an astronomical object from that survey. An example for the format of an `Example` in a survey like HSC would be:  
  ```
    {  
     'object_id': 111111,
     'ra': 124.,
     'dec': 0.,
     'healpix': 1234,
     'image': [{'array': array(224, 224), 'psf_fwhm': 0.7, 'pixel_size': 0.168, 'noise_std':  0.1, 'filter': 'r', 'units': 'nanomaggies', extinction: 0.1},
               {'array': array(224, 224), 'psf_fwhm': 0.7, 'pixel_size': 0.168, 'noise_std':  0.1, 'filter': 'i', 'units': 'nanomaggies', extinction: 0.1}],
      ...  
      'forced.g_cmodel_mag': 22.5,  
      'forced.g_cmodel_mag_err': 0.1,
      'forced.i_cmodel_mag': 21.5,
      'forced.i_cmodel_mag_err': 0.1,
      ...
    }
  ```
  - AstroPile will provide utilities to easily generate cross-matched or concatenated versions of these datasets.


Below is an example of how the user may generate a cross-matched dataset from the HSC and DECaLS surveys:
```python
import datasets
from astropile import cross_match_datasets

# Load the parent datasets
hsc = datasets.load_building(data_dir='~/data/hsc', config_name='pdr3_dud_22.5')
decals = datasets.load_building(data_dir='~/data/decals', config_name='stein_et_al')

# Build a cross-matched dataset by matching the objects in the two surveys within 1 arcsecond
matched_catalog, matched_dset = cross_match_datasets(hsc, decals, match_radius=1.0)

# The cross-matched dataset can now be saved to disk
matched_dataset.save_to_disk("hsc_meets_decals")

# Push to the HuggingFace Datasets Hub, but be careful not to push large datasets as special 
# considerations apply (see https://huggingface.co/docs/hub/repositories-recommendations)
matched_dataset.push_to_hub("my_name/hsc_meets_decals") 
```

## Lowel level dataset format

### Limitations of the HuggingFace Paradigm
The native HuggingFace Datasets paradigm has a few limitations for our uses cases where we want to have the flexibility to cross-match and combine datasets easily,
as well as being able to handle datasets that can be in the hundreds of TB in size.
pip install duckdb
  - **Cold storage of the data in Parquet files** Parquet files have nice properties: the are columnars, so that only a subset of columns can be loaded if desired, they are compressed which can reduce the size of data files in some applications, and they can be streamed from online storage which allows fast direct access to some examples of the data. However, the price to pay for compression is that they do not allow for random access, i.e. if I want to retrieve the 1000th image of my file, I actually need to read the 999th first images before I get to the 1000th. This means that if we want to cross-match say 10 spectra with a datasets of postage stamps of size 20 TB, it's going to take many hours because we need to read the entire dataset to find the stamps we are interested in.


  - **Static Parquet then Live Arrrow model** The idea of HF Datasets is that they would be stored online as static Parquet files, but when "prepared" during the `load_dataset` process, they are turned into memory-mapped Arrow tables. At that point it becomes very efficient to search, filter, or in general do random access things in O(1) on the dataset. Arrow is great, it would be very easy to generate sub-datasets and cross-match from the Arrow tables. However, for large-scale datasets of 100s of TB, this means that the entire dataset needs to be copied locally again (essentially, twice the storage space is needed) before I can start subselecting a portion of it. 


So, the core issue is that Parquet file do not allow us to do efficient search and filtering, and that we don't necessarily have the time or disk space to turn very large datasets into Arrow format.

### Proposal for AstroPile formatting and storage

The idea is to still use the HuggingFace API as much as possible, but with an alternative to the Parquet storage that retains the ability to process files with random access.

  - **File Format** Instead of Parquet, we propose to rely on traditional HDF5 as the standard for storing large data tables. HDF5 has the nice property of being a stable format, and allow for both column-based access and random access along a column. It makes it very efficient to execute a query on some columns, and then retrieve particular rows of some other columns. We can define an HDF5 DatasetBuilder so that HF Datasets can load HDF5 files just in the same way it would load from Parquet files. 


  - **Storage Layout for Efficient Matching and Robust Downloading** Thinking ahead to the case where we have datasets of Billions of entries, and in the hundreds of TB, downloading these datasets will require them to be split in smaller chunks of manageable sizes. If we also want to be able to perform fast searches and cross-matches, it would make sense to have these chunks based on the spatial position of objects on the sky. So, we propose to adopt a simple HealPIX-based splitting of the data using the Hive decomposition strategy. i.e.:
  ```
  root_dataset_folder
    |
    - subset
        |
        - healpix=9999
            |
            - 001-of-002.hdf
            - 002-of-002.hdf
        - healpix=10000
            |
            - 001-of-001.hdf
            ...
  ```

  - **Data Schema for Stored Tables** There is a wide diversity of quantities/features, establishing a common nomenclature for all types of data is not possible. However, we can define a few common columns that are present in all tables and that can be used to perform cross-matches. For instance, we can define a `ra` and `dec` column, and a `healpix` column that can be used to perform spatial searches. For particular types of data, for instance images, spectra, timeseries, we can define a minimal set of columns that are present in all tables of that type. For instance, for images, we can define an `image` column, a `seeing` column, a `pixel_size` column, a `filter` column, etc... These would also have standardized names, to make it easy to access across datasets.
  The tables would be exported in the native official schema for that survey, and the DatasetBuilder would be responsible for applying a set of aliases at reading time to standardize some of the names. The benefit of this approach is that even if we lose the code that exported the dataset, the HDF5 should remain parsable using the official schema.
  Concretely the schema for a dataset exported from HSC would look like:
  ```
  object_id, ra, dec, healpix, forced.g_cmodel_mag, meas.g_cmodel_mag, ..., image, ...
  ```
  i.e. a few names have been standardized, and the rest of the columns are kept as they are in the original survey's official schema.
  The dataset readme file would contain a description of the schema which would be reviewed as part of the dataset review process.
  *Note*: It may also be desirable to define what each survey would consider to be the most general and "go to" say magnitude measurement (for instance forced.g_cmodel_mag - a_g).
  
