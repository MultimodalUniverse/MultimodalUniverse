#!/bin/bash

echo =============
echo GAIA DR3 TEST
echo =============

echo ========================
echo DOWNLOADING TINY DATASET
echo ========================

# download files
python3 download_parts.py --tiny --output_dir . --aria2

echo =============================
echo ADDING EXTRA INFO TO XP FILES
echo =============================

python3 add_extras.py --input_dir .

echo ==================
echo PARTITIONING FILES
echo ==================

mkdir -p gaia
python3 to_parquet.py --input_dir . --output_dir gaia --nside 16

echo ==================
echo CONVERTING TO HDF5
echo ==================

python3 to_hdf5.py --input_dir gaia --cleanup

echo ==================
echo TESTING HF LOADING
echo ==================

# test loading
python3 test_load.py

## cleanup
rm *.hdf5
rm -r gaia
