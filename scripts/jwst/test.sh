#!/bin/bash

if python build_parent_sample.py --tiny; then
    echo "Build tiny parent sample successful"
else
    echo "Build tiny parent sample failed"
    exit 1
fi

if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'all', trust_remote_code=True, split='train', streaming=True).with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load tiny dataset successful"
else
    echo "Load tiny dataset failed"
    exit 1
fi

