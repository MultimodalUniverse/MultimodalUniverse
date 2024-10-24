#!/bin/bash

# Iterate over all the surveys and download the data
for survey in ngdeep ceers-full primer-uds gdn gds; do
    echo "Testing $survey"
    if python build_parent_sample.py $survey --subsample tiny; then
        echo "Build tiny parent sample for $survey successful"
    else
        echo "Build tiny parent sample for $survey failed"
        exit 1
    fi

    if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', '$survey-tiny', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
        echo "Load tiny dataset for $survey-tiny successful"
    else
        echo "Load tiny dataset for $survey-tiny failed"
        exit 1
    fi
done
