#!/bin/bash
if python build_parent_sample.py --pipeline qlp -s 23 --data_path './tiny_qlp' --hdf5_output_path './tiny_qlp' --fits_output_path './tiny_qlp/fits' --n_processes 4 --tiny; then
    echo "Build for the QLP parent sample was successful"
else
    echo "Build parent sample for QLP failed"
    exit 1
fi

if python -c "from datasets import load_dataset; from datasets.data_files import DataFilesPatternsDict; data_files = DataFilesPatternsDict.from_patterns({'train': ['./tiny_qlp/QLP/healpix=*/*.hdf5']}); dset = load_dataset('./qlp.py', trust_remote_code=True, split='train', streaming='true', data_files=data_files).with_format('numpy'); print('loaded dataset'); next(iter(dset));"; then
    echo "Load dataset for QLP successful"
else
    echo "Load dataset for QLP failed"
    exit 1
fi