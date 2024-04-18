#!/bin/bash

# Download the dataset
if python download_data.py ./ cfa4; then
    echo "Download CFA4 dataset successful"
else
    echo "Download CFA4 dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./CFA4 ./ cfa4 --tiny; then
    echo "Build parent sample for CFA4 successful"
else
    echo "Build parent sample for CFA4 failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa4.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA4 successful"
else
    echo "Load dataset for CFA4 failed"
    exit 1
fi
