#!/bin/bash

'primer-cosmos': {
        'filters': ['f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
        'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v6/index.html',
        'version' : 'v6.0',
    },
    'ceers-full': {
    'filters': ['f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.0',
    },
     'ngdeep': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.2',
    },
    'primer-uds': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v6/index.html',
     'version' : 'v6.0',
    },
     'gds': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.0',
    },
    'gdn': {
    'filters': ['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
    'base_url': 'https://s3.amazonaws.com/grizli-v2/JwstMosaics/v7/index.html',
     'version' : 'v7.3',

# First build the parent sample for the gdn field
if python build_parent_sample.py gdn --subsample tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'gdn', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi

# Then build this parent sample for the gds field 
if python build_parent_sample.py gds --subsample tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'gds', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi

# Then build this parent sample for the primer field 
if python build_parent_sample.py primer-uds --subsample tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'primer-uds', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi

# Then build this parent sample for the ngdeep field 
if python build_parent_sample.py ngdeep --subsample tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'ngdeep', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi

# Then build this parent sample for the ceers field 
if python build_parent_sample.py ceers-full --subsample tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'ceers-full', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi

# Then build this parent sample for the cosmos field 
if python build_parent_sample.py primer-cosmos --subsample tiny; then
    echo "Build parent sample for deep field successful"
else
    echo "Build parent sample for deep field failed"
    exit 1
fi

# Try to load the dataset with hugging face dataset
if python -c "from datasets import load_dataset; dset = load_dataset('./jwst.py', 'primer-cosmos', trust_remote_code=True, split='train').with_format('numpy'); print(next(iter(dset)))"; then
    echo "Load dataset for deep field successful"
else
    echo "Load dataset for deep field failed"
    exit 1
fi