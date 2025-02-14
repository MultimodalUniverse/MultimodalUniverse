#!/bin/bash

echo =============
echo GALEX TEST
echo =============

echo ========================
echo DOWNLOADING TINY DATASET
echo ========================

# download files
python3 download.py --output_dir . --tiny

echo ==================
echo BUILDING HDF5 FILES
echo ==================

# process files
python3 process.py --input_dir . --output_dir ais --tiny

python3 merge.py --input_dir ais
python3 cleanup.py --input_dir ais --remove

echo ==================
echo TESTING HF LOADING
echo ==================

# test loading
python3 test_load.py
