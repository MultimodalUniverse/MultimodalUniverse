# HST COSMOS Dataset

Data is donwloaded from the MAST archive using astroquery.

## Sample selection

The sample has two parts: 

- Galaxy sources are selected from the galaxy zoo catalog. 
- Random patches are also selected, ensuring no overlap with the galaxy sources.

## Building the dataset

To build the sample, run the following:
```bash
python build_parent_sample.py 
```
