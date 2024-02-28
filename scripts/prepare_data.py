# Small script to run the dataset preparation for all astropile datasets
from datasets import load_dataset

print('Preparing DESI dataset') 
dset_desi = load_dataset('/home/flanusse/AstroPile/desi', 
                         trust_remote_code=True, num_proc=32,
                         cache_dir='/home/flanusse/hf_cache')

print('Preparing SDSS dataset') 
dset_sdss = load_dataset('/home/flanusse/AstroPile/sdss', 
                         trust_remote_code=True, num_proc=32,
                         cache_dir='/home/flanusse/hf_cache')

print('Preparing HSC dataset') 
dset_hsc = load_dataset('/home/flanusse/AstroPile/hsc', 
                         trust_remote_code=True, num_proc=32,
                         cache_dir='/home/flanusse/hf_cache')
