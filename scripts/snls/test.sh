#!/bin/bash

# Download the dataset
if python download_data.py --destination_path ./snls; then
    echo "Download SNLS dataset successful"
else
    echo "Download SNLS dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./snls ./ --tiny; then
    echo "Build parent sample for SNLS successful"
else
    echo "Build parent sample for SNLS failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./snls.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for SNLS successful"
else
    echo "Load dataset for SNLS failed"
    exit 1
fi