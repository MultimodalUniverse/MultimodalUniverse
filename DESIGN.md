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
  - A cross-matching table allows for the stacking of the examples from multiple surveys/modalities.
  - AstroPile will provide utilities to easily generate cross-matched  or concatenated versions of these datasets.

