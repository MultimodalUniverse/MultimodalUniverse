# SAGES

This is an implementation of an MMU compatible version of the [Stellar Abundances and Galactic Evolution Survey (SAGES) DR1 dataset](https://arxiv.org/abs/2306.15611).

## Data preparation

Run the `download.py` script. This will download the catalog.

Run the `process.py` script. This will process the catalog and create the necessary files in the MMU format.

Both scripts take `--help` as an argument to show the available options.

For convenience, a single script `download_and_process.py` is provided that runs both scripts in sequence.
