#!/bin/bash

for dataset in spoc qlp tglc; do
    echo "Testing $dataset"
    if python build_parent_sample.py --pipeline $dataset -s 62,63 --data_path "./${dataset}-tiny" --hdf5_output_path "./${dataset}-tiny" --fits_output_path "./${dataset}-tiny/fits" --n_processes 6 --tiny; then
        echo ">>> Build for the ${dataset}-tiny parent sample was successful"
        
        if python -c "from datasets import load_dataset; dset = load_dataset('./tess.py', name='${dataset}-tiny', trust_remote_code=True, split='train', streaming='true').with_format('numpy'); print('loaded dataset'); next(iter(dset));"; then
            echo ">>> Load dataset for ${dataset}-tiny successful"
        else
            echo ">>> Load dataset for ${dataset}-tiny failed"
        fi
    else
        echo ">>> Build parent sample for ${dataset}-tiny failed"
    fi
    break
done


### BASIC USAGE
# # Download with database tracking
# python build_parent_sample.py --pipeline tglc -s 23 --data_path "./data" --db_path "./data/downloads.db"

# # Resume failed downloads
# python build_parent_sample.py --pipeline tglc -s 23 --data_path "./data" --resume

# # Check download status
# python check_failures.py ./data/downloads.db

### SECTORS
# # Download all TGLC sectors
# python build_parent_sample.py --pipeline tglc -s all --data_path "./data" --hdf5_output_path "./data"

# # Download a range of sectors
# python build_parent_sample.py --pipeline spoc -s "10-15" --data_path "./data" --hdf5_output_path "./data"

# # Download specific sectors
# python build_parent_sample.py --pipeline qlp -s "1,5,10,15" --data_path "./data" --hdf5_output_path "./data"

# # Resume a multi-sector download, skipping already completed sectors
# python build_parent_sample.py --pipeline spoc -s all --data_path "./data" --hdf5_output_path "./data" --resume --skip_existing
