# Small script to run the dataset preparation for all astropile datasets
from datasets import load_dataset, Dataset
from huggingface_hub import DatasetCardData, DatasetCard
from glob import glob
from functools import partial
import pickle
import sys
import shutil
import argparse
import os

# Define the destination folder where the scripts will be copied
DESTINATION_FOLDER = "/mnt/ceph/users/polymathic/public_data/MultimodalUniverse/v1"

# Folder where the generated data lives (can be different from the destination folder when testing)
DATA_FOLDER = "/mnt/ceph/users/polymathic/public_data/MultimodalUniverse/v1"

# Max size of the dataset to upload
MAX_SIZE = 100 # GB

repo = 'MultimodalUniverse'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads a dataset to the hub")
    parser.add_argument("--only_cards", action="store_true", help="Only create the dataset cards")
    parser.add_argument("--datasets", type=str, help="Comma separated list of datasets to upload")
    parser.add_argument("--copy", action="store_true", help="Copy the dataset python file to the destination folder")
    parser.add_argument("--destination", type=str, default=DESTINATION_FOLDER, help="Destination folder")
    parser.add_argument("--max_size", type=int, default=MAX_SIZE, help="Maximum size of the dataset to upload (in GB)")
    args = parser.parse_args()
    only_cards = args.only_cards
    copy = args.copy
    destination = args.destination
    max_size = args.max_size

    if args.datasets:
        datasets = args.datasets.split(',')
    else:
        datasets = glob(f'{DATA_FOLDER}/*/*.py')
        datasets = [d.split('/')[-1].split('.py')[0] for d in datasets]
    print(f"Found {len(datasets)} datasets to upload: {datasets}")

    def gen_from_iterable_dataset(iterable_ds):
        yield from iterable_ds

    for dataset in datasets:
        print(f'Preparing {dataset} dataset')

        if copy:
            # create the destination folder if it does not exist
            shutil.os.makedirs(f"{destination}/{dataset}", exist_ok=True)

            # Copy the file to the destination folder
            shutil.copyfile(f"./{dataset}/{dataset}.py",
                            f"{destination}/{dataset}/{dataset}.py")
            print("Copied file:", f"{dataset}/{dataset}.py")

        dset = load_dataset(f'{destination}/{dataset}', 
                            trust_remote_code=True, streaming=True, split='train')

        # Extract the first element of the dataset to evaluate the size
        top = next(iter(dset))
        element_size = sys.getsizeof(pickle.dumps(top))
        max_elements = int(max_size * 1e9 / element_size)
        # And remain within the budget 100_000 elements
        max_elements = min(max_elements, 100_000)
        print(f"Element size: {element_size} bytes, so we can upload {max_elements} elements to the hub.")

        if not only_cards:
            # Restrict the dataset to the maximum number of elements as to remain within budget
            dset = dset.take(max_elements)

            ds = Dataset.from_generator(partial(gen_from_iterable_dataset, dset), 
                                        features=dset.features,
                                        cache_dir=f'/tmp/hf_cache_tmp/{dataset}',
                                        num_proc=16)

            # Save the dataset
            ds.push_to_hub(f'{repo}/{dataset}', config_name=dset.config_name, 
                            max_shard_size="2GB")
            print(f"Dataset {dataset} uploaded to the hub")

        # Create the dataset card
        card = DatasetCardData(
            description=dset.description,
            homepage=dset.homepage,
            version=dset.version.__str__(),
            citation=dset.citation,
        )
        content=f"""
    ---
    {card.to_yaml()}
    ---

    # {dataset.capitalize()} Dataset

    {dset.license}

    {dset.description}

    {dset.citation}
    """
        c = DatasetCard(content)
        c.push_to_hub(f'{repo}/{dataset}')