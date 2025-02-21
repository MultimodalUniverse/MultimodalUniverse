import torch
import datasets
from datasets.arrow_dataset import Dataset as HF_Dataset  # for typing etc
from lightning import LightningDataModule
from torch.utils.data import DataLoader


class GZ10Dataset(LightningDataModule):
    def __init__(self, 
                 dataset_path: str, # path to script
                 batch_size: int = 128, 
                 num_workers: int = 0,
                 val_size = 0.2,
                 streaming: bool = False,
                 keep_in_memory: bool = True,
        ):
        super().__init__()
        self.save_hyperparameters()

    def prepare_data(self):
        # Load the dataset
        dset = datasets.load_dataset(
            self.hparams.dataset_path, 
            name='gz10_rgb_images', 
            streaming=self.hparams.streaming, 
        )
        dset.set_format('torch')

        # Split the dataset
        train_test_split = dset['train'].train_test_split(
            test_size=self.hparams.val_size, seed=42
        )  
        self.train_dataset = train_test_split['train']
        self.val_dataset = train_test_split['test']

    def setup(self, stage=None):
        pass

    def collate_fn(self, batch):
        batch = torch.utils.data.default_collate(batch)
        x = batch['rgb_image'] / 255.0
        y = batch['gz10_label']
        return x, y

    def train_dataloader(self):
        return DataLoader(
            self.train_dataset, 
            batch_size=self.hparams.batch_size, 
            num_workers=self.hparams.num_workers, 
            collate_fn=self.collate_fn
        )
    
    def val_dataloader(self):
        return DataLoader(
            self.val_dataset, 
            batch_size=self.hparams.batch_size, 
            num_workers=self.hparams.num_workers, 
            collate_fn=self.collate_fn
        )    
    
