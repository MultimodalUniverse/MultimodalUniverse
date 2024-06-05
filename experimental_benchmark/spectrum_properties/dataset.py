import torch
import datasets
from typing import Any, Tuple
from lightning import LightningDataModule
from torch.utils.data import DataLoader
from torch.nn.utils.rnn import pad_sequence


class SpectrumDataset(LightningDataModule):
    def __init__(
            self, 
            dataset_path: str,
            cache_path: str = None,
            modality: str = 'provabgs',
            batch_size: int = 128, 
            num_workers: int = 0, 
            val_size: float = 0.2,
            range_compression_factor: float = 0.01,
            keep_in_memory: bool = True,
            properties: list = ['Z_HP', 'Z_MW', 'TAGE_MW', 'AVG_SFR', 'LOG_MSTAR'],
        ):
        super().__init__()
        self.save_hyperparameters()

        if modality not in ['desi', 'sdss', 'provabgs']:
            raise ValueError("Invalid modality, must be one of ['desi', 'sdss', 'provabgs']")

    def setup(self, stage=None):
        # Get the dataset
        try: 
            dset = datasets.load_from_disk(self.hparams.dataset_path, keep_in_memory=self.hparams.keep_in_memory)
        except:
            dset = datasets.load_dataset(
                self.hparams.dataset_path, 
                cache_dir=self.hparams.cache_path,
            )
        dset.set_format('torch')
        dset = dset.shuffle(seed=42)

        if self.hparams.modality == 'provabgs':
            dset = dset.map(
                lambda x: {'AVG_SFR': torch.log(x['AVG_SFR']) - torch.log(x['Z_MW']), 'Z_MW': torch.log(x['Z_MW'])},
            )

        # Remove irrelevant columns
        modality_columns = ['spectrum'] + self.hparams.properties
        dset = dset.select_columns(modality_columns)

        # Split to train and test
        train_test_split = dset["train"].train_test_split(test_size=self.hparams.val_size)
        self.train_dataset = train_test_split['train']
        self.test_dataset = train_test_split['test']

    def collate_fn(self, batch):
        def _z_norm(x):
            """ make spectrum have std==1, mean==0 """
            sigma, mu = torch.std_mean(x, dim=1, keepdim=True)
            x = (x - mu)/(sigma + 1e-8)
            return x
        def _rms_norm(x):
            """ following Zhong+2023 https://arxiv.org/abs/2311.04146 """
            summands = (x**2).sum(dim=1, keepdim=True)
            x = x/(summands**0.5 + 1e-8)
            return x

        if self.hparams.modality == 'desi':
            batch = torch.utils.data.default_collate(batch)
            x = batch['spectrum']['flux'].squeeze()
            x = _rms_norm(x)

            # Get properties
            y = torch.stack([batch[p] for p in self.hparams.properties])
            return x.unsqueeze(1), y.unsqueeze(-1)

        if self.hparams.modality == 'provabgs':
            batch = torch.utils.data.default_collate(batch)
            x = batch['spectrum']['flux'].squeeze()
            x = _rms_norm(x)

            # Get properties
            y = torch.stack([batch[p] for p in self.hparams.properties])
            return x.unsqueeze(1), y.unsqueeze(-1)

        elif self.hparams.modality == 'sdss':
            # As SDSS spectra have variable lengths we pad with zeros to
            # longest item in the batch
            x = pad_sequence(
                [b['spectrum']['flux'] for b in batch], 
                batch_first=True,
            ).squeeze()
            # sdss dataset is riddled with nans so we need the below:
            x = torch.nan_to_num(x)
            x = _rms_norm(x)

            # Get properties
            y = torch.tensor([b['Z'] for b in batch])
            return x.unsqueeze(1), y.unsqueeze(-1)
        else:
            raise NotImplementedError


    def train_dataloader(self):
        print(self.train_dataset)
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
        print(self.test_dataset)
        return DataLoader(
            self.test_dataset, 
            batch_size=self.hparams.batch_size, 
            num_workers=self.hparams.num_workers, 
            pin_memory=True,
            collate_fn=self.collate_fn,
            persistent_workers=self.hparams.num_workers > 0
        )


