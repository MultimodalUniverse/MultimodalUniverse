# JWST Dataset

The data is donwloaded from the [DAWN JWST archive](https://dawn-cph.github.io/dja/index.html). The big field images are donwloaded and then cutouts are performed locally. Everyhing is done in the custom script [build_parent_sample.py](build_parent_sample.py).

Each JWST deep survey is treated as a different configuration. Each survey has a different wavelength coverage. We currently support the following surveys: 
- primer-uds
- primer-cosmos
- gds
- gdn
- ngdeep
- ceers-full

For these surveys, we provide the following NIRCam filters:
- f090w (not included for ngdeep)
- f115w
- f150w
- f200w
- f277w
- f356w
- f444w

The current default image size is 96 pixels. For most surveys the pixel scale is 0.02 arcsec/pixel for the short wavelength bands of NIRCam, and 0.04 arcsec/pixel for the long wavelength bands. 

More information about the source imaging data can be found here: https://dawn-cph.github.io/dja/blog/2023/07/18/image-data-products/. 


## Sample selection

Based on the source mosaics from the DAWS JWST archive, we can select a subsample of the data. The subsample can be selected based on the following criteria:

  - Magnitude cut:
    - mag_auto < 27, for primer and ceers fields
    - mag_auto < 27.5, for ngdeep and GOODS fields

  - Color coverage cut:
    - At least 4 bands available for the source.



To build the sample, run the following:
```bash
# To build the deep sample
python build_parent_sample.py survey-name --subsample tiny/all --image_dir path/to/big/fields/are/stored default is current folder --output_dir path/to/dataset 
```

We note that oversubtraction may lead to real signal being recorded in the source data, which we flag (with the entire cutout being below 0). This is rare but we leave it to the user to decide if they want to reconsider this for their science case.
