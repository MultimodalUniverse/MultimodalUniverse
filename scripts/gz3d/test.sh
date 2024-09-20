#!/bin/bash

# install the sdss-access tool
pip install sdss_access

# download a small sample
echo "Downloading and building hdf5 sample"
if python build_parent_sample.py --tiny; then
    echo "Building parent sample for gz3d succeeded"
else
    echo "Building parent sample for gz3d failed"
    exit 1
fi

# try to load the dataset
echo "Loading dataset"
if python -c "from datasets import load_dataset; dset = load_dataset('./gz3d.py', trust_remote_code=True, split='train', streaming=True).with_format('numpy'); next(iter(dset));"; then
    echo "Loaded gz3d dataset succeeded"
else
    echo "Loading gz3d dataset failed"
    exit 1
fi
