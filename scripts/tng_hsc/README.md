# TNG HSC-SSP Mock Dataset

This dataset is based on the TNG50-1 and TNG100-1 simulations and includes mock images made using SKIRT and injected into real HSC backgrounds.

For each galaxy, we provide synthetic HSC images in the HSC-SSP grizy-bands.

The default image size is 160 pixels, consistent with the format used in `MultimodalUniverse/scripts/hsc`.

> **Note:**
> - Due to resizing to 160 pixels, the pixel scale differs from that specified on the official TNG project website.
> - Unlike the actual HSC observations, this mock dataset **does not include** the features:  
>   `'a_{g,r,i,z,y}', '{g,r,i,z,y}_extendedness_value', '{g,r,i,z,y}_cmodel_mag', '{g,r,i,z,y}_sdssshape_psf_shape{1,2}', '{g,r,i,z,y}_sdssshape_shape{1,2}'`.

For more details about how the mock images were generated, refer to [Synthetic Stellar Light Images with HSC-SSP Realism](https://www.tng-project.org/data/docs/specifications/#sec5_5).

## Building the Dataset

To build the sample, run:
```bash
python build_parent_sample.py

