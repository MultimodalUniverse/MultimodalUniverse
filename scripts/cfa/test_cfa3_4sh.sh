#!/bin/bash

# Download the dataset
if python download_data.py ./ cfa3_4sh; then
    echo "Download CFA3 Stripped Core dataset successful"
else
    echo "Download CFA3 Stripped Core dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./CFA3_4SH ./ cfa3_4sh --tiny; then
    echo "Build parent sample for CFA3 Stripped Core successful"
else
    echo "Build parent sample for CFA3 Stripped Core failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa3_4sh.py', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA3 Stripped Core successful"
else
    echo "Load dataset for CFA3 Stripped Core failed"
    exit 1
fi
