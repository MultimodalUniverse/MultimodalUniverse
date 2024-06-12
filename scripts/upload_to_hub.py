# Small script to run the dataset preparation for all astropile datasets
from datasets import load_dataset, Dataset
from glob import glob
from functools import partial
import pickle
import sys

# Max size of the dataset to upload
max_size = 10 # GB

# Listing all the datasets in the target directorty
datasets = glob('/mnt/ceph/users/polymathic/MultimodalUniverse/*/*.py')
datasets = [d.split('/')[-1].split('.py')[0] for d in datasets]

print("Found the following datasets:")
print(datasets)

def gen_from_iterable_dataset(iterable_ds):
    yield from iterable_ds

for dataset in datasets:
    print(f'Preparing {dataset} dataset') 
    dset = load_dataset(f'/mnt/ceph/users/polymathic/MultimodalUniverse/{dataset}', 
                        trust_remote_code=True, streaming=True, split='train')

    # Extract the first element of the dataset to evaluate the size
    top = next(iter(dset))
    element_size = sys.getsizeof(pickle.dumps(top))
    max_elements = int(max_size * 1e9 / element_size)
    print(f"Element size: {element_size} bytes, so we can upload {max_elements} elements to the hub.")

    # Restrict the dataset to the maximum number of elements as to remain within budget
    dset = dset.take(max_elements)

    ds = Dataset.from_generator(partial(gen_from_iterable_dataset, dset), 
                                features=dset.features,
                                cache_dir=f'/tmp/hf_cache/{dataset}')

    # Save the dataset
    ds.push_to_hub('AstroPile/{dataset}', config_name=dset.config_name)
    print(f"Dataset {dataset} uploaded to the hub")
