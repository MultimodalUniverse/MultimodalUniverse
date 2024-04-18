#!/bin/bash

# Download the dataset
if python download_data.py ./data_orig --tiny; then
    echo "Download BTSbot dataset successful"
else
    echo "Download BTSbot dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./data_orig ./data; then
    echo "Build parent sample for BTSbot successful"
else
    echo "Build parent sample for BTSbot failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./btsbot.py', 'BTSbot_training_set', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for BTSbot successful"
else
    echo "Load dataset for BTSbot failed"
    exit 1
fi