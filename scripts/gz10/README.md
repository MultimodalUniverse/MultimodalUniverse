# GZ10 Galaxy Catalog Dataset

The GZ10 catalog, provided by Leung et al. (2018), features 17,736 galaxies with labels from the Galaxy Zoo 2 project. 

## Getting Started

### Downloading the Dataset

To download the GZ10 catalog, use the following `wget` command:

```bash
wget https://www.astro.utoronto.ca/~hleung/shared/Galaxy10/Galaxy10_DECals.h5
```

### Processing the Data

The dataset is provided in an HDF5 file, which needs to be transformed into a format suitable for the AstroPile. We provide a Python script, `build_parent_sample.py`, to reformat the dataset into healpix directories.

#### Using the build_parent_sample.py Script

Run the `build_parent_sample.py` script as follows, specifying the input path and the desired output path:

```bash
python build_parent_sample.py (input file, probably Galaxy10_DECals.h5) (output_directory)
```

This script will create a directory structure with the dataset split into multiple sub-directories based on healpix indices (NSIDE=16).

## Loading the Dataset with Hugging Face `datasets`

To use the dataset in your projects with different configurations, specify the appropriate builder configuration when loading the dataset:

```python
from datasets import load_dataset

# For dataset with healpix indices (no images)
dataset = load_dataset('path_to_dataset_script', name="gz10_with_healpix")

# For the entire dataset from a single HDF5 file
dataset = load_dataset('path_to_dataset_script', name="gz10_images")

# For dataset with healpix indices and images
dataset = load_dataset('path_to_dataset_script', name="gz10_with_healpix_with_images")
```

### Configurations

- `gz10_with_healpix`: Loads the catalog with healpix indices. Useful for positional analysis.
- `gz10_images`: Loads the entire catalog from the HDF5 file without additional formatting.
- `gz10_with_healpix_with_images`: Loads the catalog with healpix indices and includes image data.