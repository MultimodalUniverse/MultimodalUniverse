#!/bin/bash

# Create output dir
mkdir ./output_files

# First download the spectral data
if python download_script.py 9000 80 1 catalog ./output_files/ ; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Untar files
for file in ./output_files/*.tar; do
    tar -xvf "$file" -C ./output_files/
done


# Now build the parent sample for the deep field
if python build_parent_sample.py catalog_hdf5.hdf5 parent_sample_xray.hdf5 ./output_files/ ; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./chandra.py', 'csc21_spectra', trust_remote_code=True, split='train').with_format('numpy'); next(iter(dset));"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi
