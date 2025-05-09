# Galaxy Zoo Clump Scout Dataset

The Galaxy Zoo Clump Scout dataset contains labels of clumps in SDSS galaxies collected as part of the Galaxy Zoo project. The catalog includes 14,596 galaxies spanning redshift 0.02 < z < 0.15, with classifications from citizen scientists identifying star-forming clumps in galaxies.

## Getting Started

### Downloading the Dataset

The dataset is automatically downloaded from the IOP Science journal website when running the processing script. The source data comes from Adams et al. 2022 (DOI: 10.3847/1538-4357/ac6512).

### Processing the Data

The dataset needs to be transformed into a format suitable for the MultimodalUniverse. We provide a Python script, `build_parent_sample.py`, to reformat the dataset into healpix directories.

#### Using the build_parent_sample.py Script

Run the `build_parent_sample.py` script as follows:

```bash
python build_parent_sample.py ./gzclumps ./gzclumps
```

This script will:
1. Download the source catalog from IOP Science
2. Create a directory structure with the dataset split into multiple sub-directories based on healpix indices (NSIDE=16)
3. Process and save the data in HDF5 format

## Loading the Dataset with Hugging Face `datasets`

To use the dataset in your projects, you can load it using the Hugging Face datasets library:

```python
from datasets import load_dataset

# Load the GZ Clumps dataset
dataset = load_dataset('gzclumps.py', name="gzclumps", trust_remote_code=True)
```

### Features

The dataset includes the following features for each galaxy:
- Host galaxy right ascension and declination
- Clump right ascension and declination
- Pixel position of clump in host galaxy (X and Y)
- Shape parameters (r, e1, e2)
- Redshift
- Completeness score
- Unusual flag
- Object ID
- Pixel scale

### Nota Bene
The object_ids in this catalog are specific to the Galaxy Zoo Clump Scout catalog and are designed to help cross-match across surveys in the MultimodalUniverse.

## Documentation
- Original Paper: Adams et al. 2022, "Galaxy Zoo: Clump Scout: Surveying the Local Universe for Giant Star-forming Clumps" (DOI: 10.3847/1538-4357/ac6512)
