#!/bin/bash

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./gzclumps ./gzclumps; then
    echo "Build parent sample for gzclumps successful"
else
    echo "Build parent sample for gzclumps failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./gzclumps.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for GZClumps successful"
else
    echo "Load dataset for GZClumps failed"
    exit 1
fi
