import sys
sys.path.append('../')

import torch
import datasets
from typing import Any, Tuple
from lightning import LightningDataModule
from torch.utils.data import DataLoader


class PROVABGSDataset(LightningDataModule):
    def __init__(
            self, 
            dataset_path: str,
            modality: str = 'image',
            batch_size: int = 128, 
            num_workers: int = 8, 
            val_size: float = 0.2,
            range_compression_factor: float = 0.01,
            keep_in_memory: bool = True,
            properties: list = ['Z_HP', 'Z_MW', 'TAGE_MW', 'AVG_SFR', 'LOG_MSTAR']
        ):
        super().__init__()
        self.save_hyperparameters()

        if modality not in ["image", "spectrum", "photometry"]:
            raise ValueError("Invalid modality")

    def setup(self, stage=None):
        # Get the dataset
        dset = datasets.load_from_disk(self.hparams.dataset_path, keep_in_memory=self.hparams.keep_in_memory)
        dset.set_format('torch')
        dset = dset.shuffle(seed=42)

        # Remove irrelevant columns
        dset.remove_columns([column for column in dset.column_names if column not in [self.hparams.modality] + self.hparams.properties])

        # Map the dataset
        dset = dset.map(
            lambda x: {
                'sSFR': torch.log(x['AVG_SFR']) - torch.log(x['Z_MW']),
                'LOG_ZMW': torch.log(x['Z_MW']),
            },
            n_proc=self.hparams.num_workers
        )

        # Split to train and test
        train_test_split = dset.train_test_split(test_size=self.hparams.val_size)
        self.train_dataset = train_test_split['train']
        self.test_dataset = train_test_split['test']

    def collate_fn(self, batch):
        batch = torch.utils.data.default_collate(batch)

        # Get image and range compress
        if self.hparams.modality == 'image':
            x = batch['image']['array']
            x = torch.arcsinh(x/self.hparams.range_compression_factor)*self.hparams.range_compression_factor
            x = x * 10.0
            x = torch.clamp(x, -1.0, 1.0)

        # Or get photometry
        elif self.hparams.modality == 'photometry':
            x = torch.stack([batch['MAG_G'], batch['MAG_R'], batch['MAG_Z']]).permute(1, 0)

        # Get properties
        y = torch.stack([batch[prop] for prop in self.hparams.properties], dim=1)

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
            self.test_dataset, 
            batch_size=self.hparams.batch_size, 
            num_workers=self.hparams.num_workers, 
            pin_memory=True,
            collate_fn=self.collate_fn,
            persistent_workers=self.hparams.num_workers > 0
        )