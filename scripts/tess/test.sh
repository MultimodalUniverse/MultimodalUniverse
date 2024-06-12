#!/bin/bash

# Download the dataset
if python mast_s3_transfer.py --tiny ./tess_data/; then
    echo "Download TESS dataset successful"
else
    echo "Download TESS dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./tess_data/ . --num_processes 1 --tiny; then
    echo "Build parent sample for TESS successful"
else
    echo "Build parent sample  for TESS failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./tess.py', trust_remote_code=True, split='train', streaming='true').with_format('numpy'); print('loaded dataset'); next(iter(dset));"; then
    echo "Load dataset for TESS successful"
else
    echo "Load dataset for TESS failed"
    exit 1
fi