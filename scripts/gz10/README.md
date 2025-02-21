# GZ10 Galaxy Catalog Dataset

The Galaxy10 DECaLS dataset. This dataset combines images from the DESI Legacy Imaging Surveys (Dey A. et al., 2019) assembled by Walmsley et al., 2021 in the Galaxy Zoo DECaLS Campaign along with combined dataset classification labels from the Galaxy Zoo DECaLS campaign, the original Galaxy Zoo Campaign (Lintott et al. 2008), and the Galaxy Zoo DR2 release (Dey A. et al., 2019). Of these images and labels, 18k were selected in 10 broad classes. 

## Getting Started

### Downloading the Dataset

To download the Galaxy10 DECaLS catalog, use the following `wget` command:

```bash
wget https://www.astro.utoronto.ca/~hleung/shared/Galaxy10/Galaxy10_DECals.h5
```

### Processing the Data

The dataset is provided in an HDF5 file, which needs to be transformed into a format suitable for the MultimodalUniverse. We provide a Python script, `build_parent_sample.py`, to reformat the dataset into healpix directories.

#### Using the build_parent_sample.py Script

Run the `build_parent_sample.py` script as follows, specifying the input path and the desired output path:

```bash
python build_parent_sample.py (input file, probably Galaxy10_DECals.h5) (output_directory)
```

This script will create a directory structure with the dataset split into multiple sub-directories based on healpix indices (NSIDE=16).

## Loading the Dataset with Hugging Face `datasets`

To use the dataset in your projects with different configurations, specify one of the following appropriate builder configuration when loading the dataset. The dataset script `gz10.py` as the files processed with the `build_paranet_sample.py` script should all lie in the same current folder.

```python
from datasets import load_dataset

# For galaxy zoo 10 labels with no uint8 (rgb) images.
dataset = load_dataset('gz10.py', name="gz10", trust_remote_code=True)

# For galaxy zoo 10 labels with uint8 (rgb) images.
dataset = load_dataset('gz10.py', name="gz10_rgb_images", trust_remote_code=True)
```

### Configurations

- `gz10_with_healpix`: Loads the catalog with healpix indices. Useful for positional analysis.
- `gz10_images`: Loads the entire catalog from the HDF5 file without additional formatting.
- `gz10_with_healpix_with_images`: Loads the catalog with healpix indices and includes image data.

### Nota Bene
The GalaxyZoo10 DECaLS "object_id" is specific to the GalaxyZoo10 DECaLS catalog. It is not the same as object_ids from DESI (or other surveys), and is instead included to help cross-match across surveys in the MultimodalUniverse.  

## Documentation
- GZ10 Homepage: https://astronn.readthedocs.io/en/latest/galaxy10.html
