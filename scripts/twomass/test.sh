#!/bin/bash

echo ==========
echo 2MASS TEST
echo ==========

echo ========================
echo DOWNLOADING TINY DATASET
echo ========================

# download files
python3 download_parts.py --tiny --output_dir _2mass/psc

echo =====================================
echo PREPARING PARTITIONED PARQUET DATASET
echo =====================================

# healpixify 
python3 to_parquet.py --nside 16 --input_dir _2mass/psc --output_dir _2mass_pq/psc --file_prefix psc

echo ==================
echo CONVERTING TO HDF5 
echo ==================

# convert
python3 to_hdf5.py --data_dir _2mass_pq/psc --output_dir 2mass/psc

echo ==================
echo TESTING HF LOADING
echo ==================

# test loading
python3 test_load.py

echo ============
echo TEST PASSED! 
echo ============
