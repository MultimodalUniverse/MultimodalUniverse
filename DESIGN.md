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
     'hsc_id': 111111,  
     'hsc_image_r': {'image': array(224, 224), 'seeing': 0.7, 'pixel_size': 0.168, 'noise_std':  0.1, 'filter': 'r', 'units': 'nanomaggies', extinction: 0.1}},  
     'hsc_image_i': {'image': array(224, 224), 'seeing': 0.7, 'pixel_size': 0.168, 'noise_std':  0.1, 'filter': 'i', 'units': 'nanomaggies', extinction: 0.1}},  
      ...  
      'hsc_mag_r': 22.5,  
      'hsc_mag_i': 21.5,
      'hsc_size_r': 0.5,
      'extinction': 0.1,  
      'z_spec': 0.5,  
    }
```
  - AstroPile will provide utilities to easily generate cross-matched or concatenated versions of these datasets.


Below is an example of how the user may generate a cross-matched dataset from the HSC and DECaLS surveys:
```python
from astropile.datasets import HSC, DECaLS
from astropile import cross_match_datasets

# Load the parent datasets
hsc = HSC(data_dir='~/data/hsc', config_name='pdr3_dud_22.5')
decals = DECaLS(data_dir='~/data/decals', config_name='stein_et_al')

# Build a cross-matched dataset by matching the objects in the two surveys within 1 arcsecond
matched_catalog, matched_dset = cross_match_datasets(hsc, decals, match_radius=1.0)

# The cross-matched dataset can now be saved to disk
matched_dataset.save_to_disk("hsc_meets_decals")

# Push to the HuggingFace Datasets Hub, but be careful not to push large datasets as special 
# considerations apply (see https://huggingface.co/docs/hub/repositories-recommendations)
matched_dataset.push_to_hub("my_name/hsc_meets_decals") 
```

### Open design questions


- [ ] Should we enforce the catalogs to adopt some naming conventions for the columns? or do we leave it to the downstream users to find and use the correct column names?
    - The benefit of keeping all original column names is that there is no ambiguity about
    the definition of each column as the data schema of each original survey is preserved. The drawback is that user need to do a little bit of investigation to figure out which column to use for each dataset.

    - The benefit of enforcing a naming convention is that the data is more easily accesible and uniform, but the drawback is if we have for instance a 'mag_i' column, it is not clear what magnitude we are talking about, cmodel, psf, forced, etc... 