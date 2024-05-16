#!/bin/bash

# Download the dataset
if python download_data.py ./; then
    echo "Download DES Y3 SNe Ia dataset successful"
else
    echo "Download DES Y3 SNe Ia dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./des_y3_sne_ia ./ --tiny; then
    echo "Build parent sample for DES Y3 SNe Ia successful"
else
    echo "Build parent sample for DES Y3 SNe Ia failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./des_y3_sne_ia.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for DES Y3 SNe Ia successful"
else
    echo "Load dataset for DES Y3 SNe Ia failed"
    exit 1
fi