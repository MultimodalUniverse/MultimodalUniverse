#!/bin/bash

# First build the parent sample for the deep field
if python build_parent_sample.py pdr3_dud_22.5.sql . --rerun pdr3_dud_rev --num_processes 1 --tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./hsc.py', 'pdr3_dud_22.5', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi

# Then build this parent sample for the wide field 
if python build_parent_sample.py pdr3_wide_22.5.sql . --rerun pdr3_wide --num_processes 1 --tiny; then
    echo "Build parent sample for wide field successful"
else
    echo "Build parent sample for wide field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./hsc.py', 'pdr3_wide_22.5', trust_remote_code=True, split='train'); print(next(iter(dset)))"; then
    echo "Load dataset for wide field successful"
else
    echo "Load dataset for wide field failed"
    exit 1
fi
