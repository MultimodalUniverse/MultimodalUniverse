#!/bin/bash

# Download the dataset
if python download_data.py ./; then
    echo "Download CFA SNII dataset successful"
else
    echo "Download CFA SNII dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./CFA_SNII ./ --tiny; then
    echo "Build parent sample for CFA SNII successful"
else
    echo "Build parent sample for CFA SNII failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA SNII successful"
else
    echo "Load dataset for CFA SNII failed"
    exit 1
fi
