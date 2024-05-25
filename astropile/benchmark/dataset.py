# This module contains the Lightning dataset wrapper to easily access AstroPile datasets.
import torch
import lightning as L
import datasets 
from torch.utils.data import DataLoader
import typing as T

from astropile.utils import cross_match_datasets

class AstroPile(L.LightningDataModule):
    def __init__(
            self, 
            name: str,
            batch_size: int = 128, 
            num_workers: int = 8, 
            test_size: float = 0.1,
            local_astropile_root: str = None,
            config_name: T.Optional[str]=None):
        """ Lightning DataModule for AstroPile datasets.
        """
        super().__init__()
        self.save_hyperparameters()

    def setup(self, stage=None):
        """ Setup the dataset.
        """
        if self.hparams.local_astropile_root is not None:
            dset = datasets.load_dataset(self.hparams.name, data_dir=self.hparams.local_astropile_root, trust_remote_code=True)
        else:
            dset = datasets.load_dataset(self.hparams.name)
        
        dset = dset.with_format("torch")

        # Apply shuffling at the top level
        dset = dset.shuffle(seed=42)

        # Spliting dataset into train, val, and test sets
        dset, self.test_dataset = dset.train_test_split(test_size=self.hparams.test_size)
        self.train_dataset, self.val_dataset = dset.train_test_split(test_size=self.hparams.test_size)
        
    def train_dataloader(self):
        return DataLoader(self.train_dataset, batch_size=self.hparams.batch_size, num_workers=self.hparams.num_workers, drop_last=True)

    def val_dataloader(self):
        return DataLoader(self.val_dataset, batch_size=self.hparams.batch_size, num_workers=self.hparams.num_workers, drop_last=True)

    def test_dataloader(self):
        return DataLoader(self.test_dataset, batch_size=self.hparams.batch_size, num_workers=self.hparams.num_workers, drop_last=True)
    

class CrossMatchedAstroPile(L.LightningDataModule):
    def __init__(
            self, 
            left: str,
            right: str,
            local_astropile_root: str,
            batch_size: int = 128, 
            num_workers: int = 8, 
            test_size: float = 0.1,
            matching_radius: float = 1.0,
            cache_dir: str = None,
            save_to_disk: bool = False,
            left_config_name: T.Optional[str]=None,
            right_conig_name: T.Optional[str]=None):
        """ Lightning DataModule for datasets resulting from cross-matching of parent 
        samples.
        """
        super().__init__()
        self.save_hyperparameters()

    
    def setup(self, stage=None):
        """ Setup the dataset.
        """

        # Build the cross-matched dataset
        left = datasets.load_dataset_builder(self.hparams.left, trust_remote_code=True,
                                              data_dir=self.hparams.local_astropile_root)
        right = datasets.load_dataset_builder(self.hparams.right, self.hparams.right_config_name,
                                             trust_remote_code=True,
                                             data_dir=self.hparams.local_astropile_root)
        if self.hparams.left_config_name is not None:
            configs = [self.hparams.left_config_name]
        else:
            configs = left.builder_configs
            configs = [config for config in configs if config != "all"]

        # Let's process each config separately
        # This is because the cross-matching currently only works if a singe file
        # exists for a given healpix cell. With the way we store SDSS for instance,
        # several surveys can overlap on the sky, and that causes problems. So here
        # as a temporary fix, we process sub-configs one by one.
        dsets = []
        for i, config in enumerate(configs):
            print("Processing config from left dataset: ", config)
            left = datasets.load_dataset_builder(self.hparams.left, config, trust_remote_code=True,
                                                  data_dir=self.hparams.local_astropile_root)
            dset = cross_match_datasets(
                left,
                right,
                matching_radius=self.hparams.radius,  # In arcsecs
                cache_dir=self.hparams.cache_dir,
                num_proc=self.hparams.num_workers,
            )
            dsets.append(dset)

        # Concatenate all the datasets
        dset = datasets.concatenate_datasets(dsets)
        
        dset = dset.with_format("torch")

        # Apply shuffling at the top level
        dset = dset.shuffle(seed=42)

        # Spliting dataset into train, val, and test sets
        dset, self.test_dataset = dset.train_test_split(test_size=self.hparams.test_size)
        self.train_dataset, self.val_dataset = dset.train_test_split(test_size=self.hparams.test_size)
        
    def train_dataloader(self):
        return DataLoader(self.train_dataset, batch_size=self.hparams.batch_size, num_workers=self.hparams.num_workers, drop_last=True)

    def val_dataloader(self):
        return DataLoader(self.val_dataset, batch_size=self.hparams.batch_size, num_workers=self.hparams.num_workers, drop_last=True)

    def test_dataloader(self):
        return DataLoader(self.test_dataset, batch_size=self.hparams.batch_size, num_workers=self.hparams.num_workers, drop_last=True)