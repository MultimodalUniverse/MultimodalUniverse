#!/bin/bash

# Download the dataset
if python download_data.py --tiny; then
    echo "Download YSE dataset successful"
else
    echo "Download YSE dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py --tiny; then
    echo "Build parent sample for YSE successful"
else
    echo "Build parent sample for YSE failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./yse.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for YSE successful"
else
    echo "Load dataset for YSE failed"
    exit 1
fi
