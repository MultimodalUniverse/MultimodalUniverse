#!/bin/bash

echo =============
echo SAGES DR1 TEST
echo =============

echo ========================
echo DOWNLOADING TINY DATASET
echo ========================

# download files
python3 download.py --output_dir .

echo ==================
echo BUILDING HDF5 FILES
echo ==================

# merge files
python3 process.py --input_dir . --output_dir dr1 --tiny

echo ==================
echo TESTING HF LOADING
echo ==================

# test loading
python3 test_load.py
