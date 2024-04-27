#!/bin/bash

# install the sdss-access tool
pip install sdss_access

# download a small sample
python access_transfer.py --limit --destination_path .

# build the parent sample
python build_parent_sample.py --manga_data_path . --output_dir out --num_processes 1

# try to load the dataset
python -c "from datasets import load_dataset; dset = load_dataset('./manga.py', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))";
