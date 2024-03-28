import sys

sys.path.append("../")
import torch
from pytorch_lightning import LightningDataModule
from torch.utils.data import Dataset, DataLoader
from utils import split_dataset, compute_dataset_statistics, normalize_sample, get_nested
from typing import Any


class GenerativeWrapper(LightningDataModule):
    def __init__(
        self,
        train_dataset: Dataset,
        test_dataset: Dataset,
        dimensions: int,
        batch_size: int = 128,
        num_workers: int = 8,
        test_size: float = 0.2,
        split_method: str = "naive",
        loading: str = "iterated",
        feature_flag: str = "image",
    ):
        """
        Initializes the data module with a dataset that is already loaded, setting up parameters
        for data processing and batch loading.

        Parameters:
        - train_dataset (Dataset): The pre-loaded dataset, expected to be a torch.utils.data.Dataset with images in size B x C x H x W.
        - test_dataset (Dataset): The pre-loaded dataset, expected to be a torch.utils.data.Dataset with images in size B x C x H x W.
        - dimensions (int): The number of dimensions of the data (int 1, 2, or 3). e.g. for spectra (1D), images (2D), or volumes (3D).
        - batch_size (int): The size of each data batch for loading.
        - num_workers (int): Number of subprocesses to use for data loading.
        - test_size (float): The proportion of the dataset to reserve for testing.
        - split_method (str): Strategy for splitting the dataset ('naive' implemented).
        - loading (str): Approach for loading the dataset ('full' or 'iterated').
        - feature_flag (str): The key in the dataset corresponding to the image data.
        """

        self.train_dataset = train_dataset
        self.test_dataset = test_dataset
        self.dimensions = dimensions
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.test_size = test_size
        self.loading = loading
        self.feature_flag = feature_flag

    def setup(self, stage=None):
        pass

    def collate_fn(self, batch):
        B = torch.stack(
            [get_nested(batch_item, self.feature_flag) for batch_item in batch]
        ).unsqueeze(1)[..., :1024]
        print(B.shape)
        return B

    def train_dataloader(self):
        return DataLoader(
            self.train_dataset,
            batch_size=self.batch_size,
            num_workers=self.num_workers,
            collate_fn=self.collate_fn,
        )

    def test_dataloader(self):
        return DataLoader(
            self.test_dataset,
            batch_size=self.batch_size,
            num_workers=self.num_workers,
            collate_fn=self.collate_fn,
        )
