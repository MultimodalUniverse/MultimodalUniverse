#!/bin/bash

for dataset in spoc qlp tglc; do
    echo "Testing $dataset"
    if python build_parent_sample.py --pipeline $dataset -s 23 --data_path "./${dataset}-tiny" --hdf5_output_path "./${dataset}-tiny" --fits_output_path "./${dataset}-tiny/fits" --n_processes 6 --tiny; then
        echo ">>> Build for the ${dataset}-tiny parent sample was successful"
    else
        echo ">>> Build parent sample for ${dataset}-tiny failed"
        continue
    fi

    if python -c "from datasets import load_dataset; dset = load_dataset('./tess.py', name='${dataset}-tiny', trust_remote_code=True, split='train', streaming='true').with_format('numpy'); print('loaded dataset'); next(iter(dset));"; then
        echo ">>> Load dataset for ${dataset}-tiny successful"
    else
        echo ">>> Load dataset for ${dataset}-tiny failed"
    fi
done
