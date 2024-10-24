# JWST Dataset

The data is donwloaded from the [DAWN JWST archive](https://dawn-cph.github.io/dja/index.html). The big field images are donwloaded and then cutouts are performed locally. Everyhing is done in the custom script [build_parent_sample.py](build_parent_sample.py).

Each JWST deep survey is treated as a different configuration. Each survey has a different wavelength coverage. We currently support the following surveys: 
- primer-uds
- gds
- gdn
- ngdeep
- ceers-full

The current default image size is 96 pixels. The tiny dataset donwloads only one band. 

To build the sample, run the following:
```bash
# To build the deep sample
python build_parent_sample.py survey-name --subsample tiny/all --image_dir path/to/big/fields/are/stored default is current folder --output_dir path/to/dataset 
```

We note that oversubtraction may lead to real signal being recorded in the source data, which we flag (with the entire cutout being below 0). This is rare but we leave it to the user to decide if they want to reconsider this for their science case.
