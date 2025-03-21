# HST COSMOS Dataset

Data is donwloaded from the MAST archive using astroquery.
Koekemoer et al. '07: [COSMOS ACS Survey Overview](https://arxiv.org/pdf/astro-ph/0703095)

Source selection comes from Willett et al. '17: [Galaxy-Zoo Hubble](https://ui.adsabs.harvard.edu/abs/2017MNRAS.464.4176W/abstract)

## Sample selection

The sample has two parts: 

- Galaxy sources are selected from the galaxy zoo catalog. 
- Random patches are also selected, ensuring no overlap with the galaxy sources.

## Building the dataset

To build the sample, run the following:
```bash
python build_parent_sample.py 
```
