# HST COSMOS Dataset

Data is downloaded from the MAST archive using astroquery.
Koekemoer et al. '07: [COSMOS ACS Survey Overview](https://arxiv.org/pdf/astro-ph/0703095)

## Sample selection

Source selection comes from Willett et al. '17: [Galaxy-Zoo Hubble](https://ui.adsabs.harvard.edu/abs/2017MNRAS.464.4176W/abstract)

## Step 1: Download required galaxy zoo Hubble data products

For the cutout generation to work, the user must download these two files
from the [galaxy zoo hubble public dataset](https://data.galaxyzoo.org/#section-11)

gz_hubble_main.csv:  Table 4: Morphological classifications for galaxies in main HST sample -> CSV -> gz_hubble_main.csv.gz
metadata_main.csv:  Metadata -> GZH main sample -> CSV

## Step 2: Building the dataset

To build the sample, run the following:
```bash
python3 build_parent_sample.py --metadata_path=metadata_main.csv --morphology_path=gz_hubble_main.csv --downloads_folder=COSMOS/ --output_dir=COSMOS/ --debug=True --nan_tolerance=0.2 --zero_tolerance=0.2
```
WARNING: if debug=False, this call will download a large number of files.

## Step 3: Loading the dataset with HuggingFace

Run through test.ipynb
