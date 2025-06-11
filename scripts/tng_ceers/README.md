# JWST-CEERS TNG Mock Dataset

This dataset is based on the TNG50-1 simulations and includes mock observations at redshifts `z = 3, 4, 5, 6`.

For each galaxy, we provide synthetic JWST images in the following NIRCam filters:
- `F200W`
- `F356W`

The default image size is 96 pixels, consistent with the format used in `MultimodalUniverse/scripts/jwst`.

> **Note:**
> - Due to resizing to 96 pixels, the pixel scale differs from that specified on the official TNG project website.
> - Unlike the actual JWST observations, this mock dataset **does not include** the following features:  
>   `'mag_auto', 'flux_radius', 'flux_auto', 'fluxerr_auto', 'cxx_image', 'cyy_image', 'cxy_image'`.

For more details about how the mock images were generated, refer to [JWST-CEERS Mock Galaxy Imaging](https://www.tng-project.org/data/docs/specifications/#sec5v).

## Building the Dataset

To build the sample, run:
```bash
python build_parent_sample.py
