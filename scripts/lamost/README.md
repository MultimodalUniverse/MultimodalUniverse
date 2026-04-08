# LAMOST Dataset Collection

This folder contains the scripts used to build the LAMOST spectroscopic parent sample, based on optical spectra from the Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST).

## Sample selection

In the current version of the dataset, we do not apply any quality cuts. This ensures maximum flexibility. Quality cuts can be applied on the local catalog after download.

## Data preparation

The pipeline has three steps:

### Step 1: Download tar.gz archives

`download_data.py` downloads the LAMOST catalog and the raw spectra as date-level tar.gz archives (one per observation night). The tar.gz files are **not extracted** — they are kept as-is to minimize the number of files on disk.

```bash
python3 download_data.py --catalog_name LRS_stellar --release dr11_v2.0 -o ./lamost_data
```

This will:
1. Download and extract the catalog FITS file (e.g. `dr11_v2.0_LRS_stellar.fits`).
2. Determine unique observation dates from the catalog.
3. Download one `<date>.tar.gz` per night into the output directory.

Already-downloaded tar.gz files are skipped, so the download is resume-safe. Multi-node parallel downloading is supported via `--rank` and `--world_size` (see `download_data.sbatch` for a SLURM example).

### Step 2: Process tar.gz archives into intermediate HDF5s

`process_tars.py` streams through each tar.gz, extracts spectra matching the catalog, and writes one intermediate HDF5 per tar into `<output_dir>/_intermediate/`. Already-processed tars are skipped, making this step resume-safe.

```bash
python3 process_tars.py dr11_v2.0_LRS_stellar.fits ./lamost_data ./output
```

This step supports multi-node parallelism via `--rank` and `--world_size`, which shards the tar files across workers (see `process_tars.sbatch` for a SLURM example):

```bash
python3 process_tars.py dr11_v2.0_LRS_stellar.fits ./lamost_data ./output --rank 0 --world_size 4
python3 process_tars.py dr11_v2.0_LRS_stellar.fits ./lamost_data ./output --rank 1 --world_size 4
```

### Step 3: Build the parent sample

`build_parent_sample.py` reads all intermediate HDF5 files, computes healpix indices, and reshuffles spectra into the final healpix-grouped directory structure. Run this once after all `process_tars.py` workers have finished.

```bash
python3 build_parent_sample.py dr11_v2.0_LRS_stellar.fits ./output
```

The final output has the structure:
```
output/<catalog_name>/healpix=<N>/001-of-001.hdf5
```

Use `--tiny` on both `process_tars.py` and `build_parent_sample.py` to process only the first 50 catalog rows for quick testing.

Now, this dataset can be loaded with `datasets.load_dataset`!

### Documentation

- LAMOST official website: http://www.lamost.org/
- LAMOST survey overview: https://ui.adsabs.harvard.edu/abs/2012RAA....12.1197C/abstract
