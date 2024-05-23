#!/bin/bash

# First build the parent sample for the deep field
if python build_parent_sample.py --apogee_data_path=apogee --output_dir=. --num_processes 1 --tiny; then
    echo "Build parent sample for apogee successful"
else
    echo "Build parent sample for apogee failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./apogee.py', trust_remote_code=True, split='train').with_format('numpy'); next(iter(dset));"; then
    echo "Load dataset for apogee successful"
else
    echo "Load dataset for apogee failed"
    exit 1
fi

