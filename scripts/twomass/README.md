# 2MASS

This folder contains a dataset for the Two Micron All Sky Survey (2MASS), which provides all-sky near-IR photometry in the J, H, and Ks bands at 1.235, 1.662, and 2.159 μm. Current there is support for the Point Source Catalog, which consists of over 500 million stars and galaxies.

For more information on 2MASS, see https://www.ipac.caltech.edu/2mass/overview/about2mass.html and https://www.ipac.caltech.edu/2mass/releases/allsky/doc/explsup.html.

For more information on the Point Source Catalog, see https://www.ipac.caltech.edu/2mass/releases/allsky/doc/sec2_2.html, and particular https://www.ipac.caltech.edu/2mass/releases/allsky/doc/sec2_2a.html for details on the format and columns.

## Data Preparation

Run the `prepare.py` script to download and prepare the dataset in MMU format. This runs all necessary steps.

To run the individual steps manually, here are more details:

The data can be downloaded using the `download_parts.py` script, which will grab the 2MASS PSC data in gzipped csv format. You can then run the `to_parquet.py` script, which will read in all the gzipped csv files, turn them into PyArrow tables, add a healpix column for partitioning, and then merge everything into a partitioned Parquet dataset. This can be used with HF dataset directly. Running `to_hdf5.py`, will convert the Parquet files into HDF5 files, after which you can use the `twomass.py` descriptor to load the files into HF datasets.

The file list was obtained from [Bulk Catalog Download](https://irsa.ipac.caltech.edu/data/2MASS/docs/releases/allsky/doc/sec1_4.html#ftpdes) section of the [2MASS page](https://irsa.ipac.caltech.edu/Missions/2mass.html) on the NASA/IPAC Infrared Science Archive.

## Dataset

See https://www.ipac.caltech.edu/2mass/releases/allsky/doc/sec2_2a.html for details on the format and columns. All columns are exposed.
