#!/bin/bash

# Download the dataset
if python download_data.py --destination_path ./data; then
    echo "Download Foundation dataset successful"
else
    echo "Download Foundation dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./data . --tiny --dirty; then
    echo "Build parent sample for Foundation successful"
else
    echo "Build parent sample for Foundation failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./foundation.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for Foundation successful"
else
    echo "Load dataset for Foundation failed"
    exit 1
fi