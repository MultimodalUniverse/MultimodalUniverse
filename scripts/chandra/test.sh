#!/bin/bash
DATA_PATH="./output_data"
# Create output dir
mkdir -p $DATA_PATH

# First download the Chandra spectral data
if python download_script.py --min_cnts 9000 --min_sig 80 --max_theta 1 --output_file catalog --file_path $DATA_PATH/ ; then
    echo "Download parent sample for Chandra spectra successful"
else
    echo "Download parent sample for Chandra spectra"
    exit 1
fi

# Untar files
for file in $DATA_PATH/*.tar; do
    tar -xvf "$file" -C $DATA_PATH/
done


# Now build the parent sample
if python build_parent_sample.py --cat_file catalog.hdf5 --output_file parent_sample_xray.hdf5 --file_path $DATA_PATH/  ; then
    echo "Build parent sample for Chandra spectra successful"
else
    echo "Build parent sample for Chandra spectra"
    exit 1
fi

# Try to load the dataset with huggingface dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./chandra.py', 'chandra_spectra', trust_remote_code=True, split='train').with_format('numpy'); next(iter(dset));"; then
    echo "Load dataset for Chandra spectra successful"
else
    echo "Load dataset for Chandra spectra failed"
    exit 1
fi

