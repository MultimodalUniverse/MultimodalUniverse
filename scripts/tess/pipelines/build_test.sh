#!/bin/bash
if python build_parent_sample.py --pipeline spoc -s 23 --data_path './tiny_spoc' --hdf5_output_path './tiny_spoc' --fits_output_path './tiny_spoc/fits' --n_processes 6 --tiny; then
    echo "Build for the SPOC parent sample was successful"
else
    echo "Build parent sample for SPOC failed"
    exit 1
fi

if python -c "from datasets import load_dataset; from datasets.data_files import DataFilesPatternsDict; data_files = DataFilesPatternsDict.from_patterns({'train': ['./tiny_spoc/SPOC/healpix=*/*.hdf5']}); dset = load_dataset('./spoc.py', trust_remote_code=True, split='train', streaming='true', data_files=data_files).with_format('numpy'); print('loaded dataset'); next(iter(dset));"; then
    echo "Load dataset for SPOC successful"
else
    echo "Load dataset for SPOC failed"
    exit 1
fi