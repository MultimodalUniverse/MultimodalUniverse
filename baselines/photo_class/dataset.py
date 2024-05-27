import torch
import datasets
from typing import Any, Tuple
from lightning import LightningDataModule
from torch.utils.data import DataLoader


class PhotoClassDataset(LightningDataModule):
    def __init__(
            self,
            dataset_path: str,
            batch_size: int = 128,
            num_workers: int = 8,
            split: str = 'random',
            val_size: float = 0.2,
            redshift_train_range: Tuple[float, float] = (0.0, 0.1),  # change?
            redshift_test_range: Tuple[float, float] = (0.1, 0.6),  # change?
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
            low_z_dataset = dset.filter(lambda example: example['hostgal_specz'] >= self.hparams.redshift_train_range[0] and example['hostgal_specz'] < self.hparams.redshift_train_range[1])
            high_z_dataset = dset.filter(lambda example: example['hostgal_specz'] >= self.hparams.redshift_train_range[0] and example['hostgal_specz'] < self.hparams.redshift_test_range[1])
            return low_z_dataset, high_z_dataset

        # Any split worth looking at for classification?

    def setup(self, stage=None):
        # Get the dataset
        dset = datasets.load_from_disk(self.hparams.dataset_path)

        # Get relevant parameters
        # dset = dset.remove_columns([column for column in dset.column_names if column not in ['obj_type', 'lightcurve']])
        dset = dset.remove_columns([column for column in dset.column_names if column not in ['obj_type', 'lightcurve', 'hostgal_specz']])
        dset = dset.with_format("torch")

        # Shuffle the dataset
        dset = dset.shuffle(
            seed=42
        )

        # Split the dataset
        self.train_dataset, self.val_dataset = self._split_dataset(dset)
        self.num_classes = len(set(self.train_dataset['obj_type']))

        # # Get mean and std of the training set
        # self.z_mean = self.train_dataset['Z'].mean()
        # self.z_std = self.train_dataset['Z'].std()

    def collate_fn(self, batch):
        batch = torch.utils.data.default_collate(batch)
        x, y = batch['lc']['array'], batch['obj_type']

        # Not sure what this does
        # # Range compress x
        # x = torch.arcsinh(x/self.hparams.range_compression_factor)*self.hparams.range_compression_factor
        # x = x * 10.0
        # x = torch.clamp(x, -1.0, 1.0)

        # Z-score y
        # y = (y - self.z_mean) / self.z_std

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
