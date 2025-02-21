#!/bin/bash

# Download the dataset
if python download_data.py ./data_orig --tiny; then
    echo "Download BTSbot dataset successful"
    echo ""
else
    echo "Download BTSbot dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py ./data_orig .; then
    echo "Build parent sample for BTSbot successful"
    echo ""
else
    echo "Build parent sample for BTSbot failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c \
"
from datasets import load_dataset
for split in ('train', 'val', 'test'):
    dset_split = load_dataset('./btsbot.py', 'BTSbot', trust_remote_code=True, split=split);
    print(f'loaded {split} split from dataset with {len(dset_split)} examples')
    example = next(iter(dset_split))
    object_id = example['object_id']
    print('Some example data from the first example:')
    print(f'object_id={object_id}')
    print()
"; then
    echo "Load dataset for BTSbot successful"
else
    echo "Load dataset for BTSbot failed"
    exit 1
fi

# Clean up
rm -r ./data
