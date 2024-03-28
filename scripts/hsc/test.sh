#!/bin/bash

# First build the parent sample for the deep field
python build_parent_sample.py pdr3_dud_22.5.sql . --rerun pdr3_dud_rev --tiny

# Try to load the dataset with hugging face dataset
python -c "from datasets import load_dataset; dset = load_dataset('./hsc.py', 'pdr3_dud_22.5', trust_remote_code=True, split='train'); print(next(iter(dset)))"

# Then build this parent sample for the wide field 
python build_parent_sample.py pdr3_wide_22.5.sql . --rerun pdr3_wide --tiny

# Try to load the dataset with hugging face dataset
python -c "from datasets import load_dataset; dset = load_dataset('./hsc.py', 'pdr3_wide_22.5', trust_remote_code=True, split='train'); print(next(iter(dset)))"


