import torch
import tqdm
from torch.utils.data import DataLoader
from datasets.arrow_dataset import Dataset as HF_Dataset
from typing import Tuple, Any

def split_dataset(
        dataset: HF_Dataset, 
        split: str = 'naive'
        ) -> Tuple[Any, Any]:
    """
    Splits a dataset into training and testing sets.

    Parameters:
    - dataset: The dataset to be split.
    - split: The splitting strategy. Currently, only 'naive' is implemented.

    Returns:
    - A tuple of (train_dataset, test_dataset).
    """
    if split == 'naive':
        train_test_split = dataset.train_test_split(test_size=0.2)
    else:
        raise ValueError('Split method not implemented yet.')
    return train_test_split['train'], train_test_split['test']

def compute_dataset_statistics(
        dataset: HF_Dataset, 
        flag: str, 
        loading: str = 'full', 
        batch_size: int = 128, 
        num_workers: int = 8
        ) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    Computes mean and standard deviation of a dataset for a specific feature.

    Parameters:
    - dataset: The dataset to compute statistics for. Assumes images are B x C x H x W.
    - flag: The key in the dataset corresponding to the feature of interest.
    - loading: Specifies whether to load the dataset 'full' at once or 'iterated' through a DataLoader.
    - batch_size: The batch size to use when 'loading' is set to 'iterated'.
    - num_workers: The number of worker processes to use when 'loading' is 'iterated'.

    Returns:
    - A tuple of (mean, std) tensors for the specified feature.
    """
    dummy = get_nested(dataset[0], flag)

    if len(dummy.shape) == 3:
        axis = (0, 2, 3)
        input_channels = dummy.shape[0]

    elif dummy.shape == ():
        axis = 0
        input_channels = 1

    else: 
        raise ValueError('Invalid shape of the feature tensor, only supports images or scalars.')

    # Compute statistics either for the entire dataset loaded in memory or iteratively.
    if loading == 'full':
        # print(get_nested(dataset, flag))
        # TODO @Liam this breaks for me because I can't do get_nested(dataset, flag) because dataset is an indexable list (HF Dataset), not a dict.
        # can only key into an element or batch, not the whole dataset
        # iterated works fine as it uses batches
        mean, std = torch.mean(get_nested(dataset, flag), dim=axis), torch.std(get_nested(dataset, flag), dim=axis)
    elif loading == 'iterated':
        mean, mean_sq = torch.zeros(input_channels), torch.zeros(input_channels)
        dummy_loader = DataLoader(dataset, batch_size=batch_size, num_workers=num_workers)
        n_batches = len(dummy_loader)

        for batch in dummy_loader:
            # added .float() as pytorch can't take a mean of Long (i.e. uint8)
            mean += torch.mean(get_nested(batch, flag).float(), dim=axis)
            mean_sq += torch.mean(get_nested(batch, flag).float()**2, dim=axis)

        mean /= n_batches
        std = (mean_sq / n_batches - mean**2).sqrt()
    else:
        raise ValueError('Invalid loading method specified.')
    
    if len(dummy.shape) == 3:
        mean = mean[:,None,None]
        std = std[:,None,None]

    return mean, std

def normalize_sample(sample, mean, std, dynamic_range, z_score=True):
    """
    Normalizes a sample, with optional dynamic range compression.
    """
    if z_score:
        sample = (sample - mean) / std
    if dynamic_range:
        sample = torch.arcsinh(sample/3)
    return sample

def denormalize_sample(sample, mean, std, dynamic_range, z_score=True):
    """
    Reverses normalization (and optional dynamic range compression) on a sample.
    """
    if dynamic_range:
        sample = torch.sinh(sample*3)
    if z_score:
        sample = sample * std + mean
    return sample

def get_nested(dic, compound_key: str, default=None, raise_on_missing=True):
    """
    Get a nested value from a dictionary using a compound key.
    """
    # if . in compout_key, split it and get the value
    if '.' in compound_key:
        keys = compound_key.split('.')
        current_value = dic
        try:
            for key in keys:
                current_value = current_value[key]
            return current_value
        except (KeyError, TypeError):
            if raise_on_missing:
                raise KeyError(f'Key {compound_key} not found in dictionary {dic}.')
            return default
    else:
        return dic[compound_key]