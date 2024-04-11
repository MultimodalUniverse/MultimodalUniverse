from datasets import load_dataset

decals16 = load_dataset(f'/mnt/ceph/users/polymathic/AstroPile_tiny/decals/decals.py', trust_remote_code=True, cache_dir='/mnt/home/lparker/ceph/AstroPileDatasets/decals16_tiny')
print('DECaLS16 Loaded')

sdss = load_dataset(f'/mnt/ceph/users/polymathic/AstroPile_tiny/sdss/sdss.py', trust_remote_code=True, cache_dir='/mnt/home/lparker/ceph/AstroPileDatasets/sdss_tiny')
print('SDSS loaded')
