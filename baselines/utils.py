import torch
import tqdm
from torch.utils.data import DataLoader, Dataset
from typing import Tuple, Any

def split_dataset(
        dataset: Dataset, 
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
        dataset: Dataset, 
        flag: str, 
        axis_of_interest: int, 
        loading: str = 'full', 
        batch_size: int = 128, 
        num_workers: int = 8
        ) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    Computes mean and standard deviation of a dataset for a specific feature.

    Parameters:
    - dataset: The dataset to compute statistics for.
    - flag: The key in the dataset corresponding to the feature of interest.
    - axis_of_interest: The axis along which to compute the statistics.
    - loading: Specifies whether to load the dataset 'full' at once or 'iterated' through a DataLoader.
    - batch_size: The batch size to use when 'loading' is set to 'iterated'.
    - num_workers: The number of worker processes to use when 'loading' is 'iterated'.

    Returns:
    - A tuple of (mean, std) tensors for the specified feature.
    """
    dummy = dataset[flag][0]

    # Determine whether the dataset contains scalars or higher-dimensional data.
    if dummy.dim() == 0:  # Scalar data
        input_channels = 1
        axis = (0,)
    else:  # Higher-dimensional data
        input_channels = dummy[None, ...].shape[axis_of_interest]
        axis = tuple(i for i in range(dummy.dim()) if i != axis_of_interest)

    # Compute statistics either for the entire dataset loaded in memory or iteratively.
    if loading == 'full':
        mean, std = torch.mean(dataset[flag], dim=axis), torch.std(dataset[flag], dim=axis)
    elif loading == 'iterated':
        mean, mean_sq = torch.zeros(input_channels), torch.zeros(input_channels)
        dummy_loader = DataLoader(dataset, batch_size=batch_size, num_workers=num_workers)
        n_batches = len(dummy_loader)

        for batch in tqdm.tqdm(dummy_loader, total=n_batches, desc=f'Computing statistics for {flag}'):
            mean += torch.mean(batch[flag], dim=axis)
            mean_sq += torch.mean(batch[flag]**2, dim=axis)

        mean /= n_batches
        std = (mean_sq / n_batches - mean**2).sqrt()
    else:
        raise ValueError('Invalid loading method specified.')

    return mean, std

def dynamic_range_compression(norm_image):
    """
    Applies dynamic range compression to normalized images.
    """
    return torch.arcsinh(norm_image)

def dynamic_range_decompression(compressed_image):
    """
    Reverses dynamic range compression on images.
    """
    return torch.sinh(compressed_image)

def normalize_sample(sample, mean, std, dynamic_range):
    """
    Normalizes a sample, with optional dynamic range compression.
    """
    sample = (sample - mean) / std
    if dynamic_range:
        sample = dynamic_range_compression(sample / 3)
    return sample

def denormalize_sample(sample, mean, std, dynamic_range):
    """
    Reverses normalization (and optional dynamic range compression) on a sample.
    """
    if dynamic_range:
        sample = dynamic_range_decompression(sample*3)
    return sample * std + mean
