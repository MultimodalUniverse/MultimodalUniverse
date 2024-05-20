import sys
sys.path.append('../')

import torch
import datasets
from typing import Any, Tuple
from lightning import LightningDataModule
from torch.utils.data import DataLoader


class PhotoZDataset(LightningDataModule):
    def __init__(
            self, 
            dataset_path: str,
            batch_size: int = 128, 
            num_workers: int = 8, 
            split: str = 'random',
            val_size: float = 0.2,
            redshift_train_range: Tuple[float, float] = (0.0, 0.8),
            redshift_test_range: Tuple[float, float] = (0.8, 1.2),
            range_compression_factor: float = 0.01,
        ):
        super().__init__()
        self.save_hyperparameters()

    def _split_dataset(self, dset):
        # Split the dataset using a random split
        if self.hparams.split == 'random':
            train_test_split = dset.train_test_split(test_size=self.hparams.val_size)
            return train_test_split['train'], train_test_split['test']
        
        # Split the dataset using a high-z split
        elif self.hparams.split == 'high-z':
            low_z_dataset = dset.filter(lambda example: example['Z'] >= self.hparams.redshift_train_range[0] and example['Z'] < self.hparams.redshift_train_range[1])
            high_z_dataset = dset.filter(lambda example: example['Z'] >= self.hparams.redshift_train_range[0] and example['Z'] < self.hparams.redshift_test_range[1])
            return low_z_dataset, high_z_dataset

    def setup(self, stage=None):
        # Get the dataset
        dset = datasets.load_from_disk(self.hparams.dataset_path)

        # Get only image and redshift
        dset = dset.remove_columns([column for column in dset.column_names if column not in ['image', 'Z']])
        self.dset = dset.with_format("torch")

        # Shuffle the dataset
        self.dset = self.dset.shuffle(
            seed=42
        )

        # Split the dataset
        self.train_dataset, self.val_dataset = self._split_dataset(self.dset)

    def collate_fn(self, batch):
        batch = torch.utils.data.default_collate(batch)
        x, y = batch['image']['array'], batch['Z']

        # Range compress x
        x = torch.arcsinh(x/self.hparams.range_compression_factor)*self.hparams.range_compression_factor
        x = x * 10.0
        x = torch.clamp(x, -1.0, 1.0)

        return x, y

    def train_dataloader(self):
        return DataLoader(
            self.train_dataset, 
            batch_size=self.hparams.batch_size, 
            num_workers=self.hparams.num_workers, 
            drop_last=True,
            pin_memory=True,
            collate_fn=self.collate_fn,
            persistent_workers=self.hparams.num_workers > 0
        )

    def val_dataloader(self):
        return DataLoader(
            self.val_dataset, 
            batch_size=self.hparams.batch_size, 
            num_workers=self.hparams.num_workers, 
            pin_memory=True,
            collate_fn=self.collate_fn,
            persistent_workers=self.hparams.num_workers > 0
        )