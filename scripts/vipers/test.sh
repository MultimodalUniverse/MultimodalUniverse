#!/bin/bash

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py "./" --num_processes 1 --tiny; then
    echo "Build parent sample for VIPERS successful"
else
    echo "Build parent sample for VIPERS failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./vipers.py', trust_remote_code=True, split='train', name='vipers_w4').with_format('numpy'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for VIPERS successful"
else
    echo "Load dataset for VIPERS failed"
    exit 1
fi

