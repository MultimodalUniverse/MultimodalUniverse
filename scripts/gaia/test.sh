#!/bin/bash

echo =============
echo GAIA DR3 TEST
echo =============

echo ========================
echo DOWNLOADING TINY DATASET
echo ========================

# download files
python3 download_parts.py --tiny --output_dir .

echo ==================
echo MERGING HDF5 FILES
echo ==================

# merge files
python3 merge_parts.py --input_dir . --output_file merged.hdf5

echo ===================
echo HEALPIXIFYING FILES
echo ===================

# healpixify 
mkdir gaia
python3 healpixify.py --input_file merged.hdf5 --output_dir . --nside 8

echo ==================
echo TESTING HF LOADING
echo ==================

# test loading
python3 test_load.py
