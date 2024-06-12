# Scripts for building a Legacy Survey sample based on Stein et al. (2019)


This assumes the data has already been downloaded from GLOBUS using the original instructions.

## Generating the catalog 

```bash
python build_parent_sample.py [download directory] [output directory]
```
This will generate a fits catalog of the parent sample, with the necessary information to cross-match against DESI spectra.

Benefit of using this sample is that no significant local processing is required to generate the parent sample.