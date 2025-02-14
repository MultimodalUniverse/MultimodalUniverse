#!/bin/bash

# Download the dataset
if python download_data.py ./downloaded cfa3; then
    echo "Download CFA3 dataset successful"
else
    echo "Download CFA3 dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py --data_path ./downloaded/CFA3 --output_dir ./cfa3 --dataset cfa3 --tiny; then
    echo "Build parent sample for CFA3 successful"
else
    echo "Build parent sample for CFA3 failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa.py', 'cfa3', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA3 successful"
else
    echo "Load dataset for CFA3 failed"
    exit 1
fi

# Download the dataset
if python download_data.py ./downloaded cfa_SECCSN; then
    echo "Download CFA3 Stripped Core dataset successful"
else
    echo "Download CFA3 Stripped Core dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py --data_path ./downloaded/CFA_SECCSN --output_dir ./cfa_seccsn --dataset cfa_SECCSN --tiny; then
    echo "Build parent sample for CFA Stripped Core successful"
else
    echo "Build parent sample for CFA Stripped Core failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa.py', 'cfa_SECCSN', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA3 Stripped Core successful"
else
    echo "Load dataset for CFA3 Stripped Core failed"
    exit 1
fi

if python download_data.py ./downloaded cfa4; then
    echo "Download CFA4 dataset successful"
else
    echo "Download CFA4 dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py --data_path ./downloaded/CFA4 --output_dir ./cfa4 --dataset cfa4 --tiny; then
    echo "Build parent sample for CFA4 successful"
else
    echo "Build parent sample for CFA4 failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa.py', 'cfa4', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA4 successful"
else
    echo "Load dataset for CFA4 failed"
    exit 1
fi

if python download_data.py ./downloaded cfa_snII; then
    echo "Download CFA SNII dataset successful"
else
    echo "Download CFA SNII dataset failed"
    exit 1
fi

# First build the parent sample and save both raw and H5 to current directory
if python build_parent_sample.py --data_path ./downloaded/CFA_SNII --output_dir ./cfa_snii --dataset cfa_snII --tiny; then
    echo "Build parent sample for CFA SNII successful"
else
    echo "Build parent sample for CFA SNII failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./cfa.py', 'cfa_snII', trust_remote_code=True, split='train'); print(f'loaded dataset with {len(dset)} examples'); next(iter(dset));"; then
    echo "Load dataset for CFA SNII successful"
else
    echo "Load dataset for CFA SNII failed"
    exit 1
fi

rmdir ./downloaded
