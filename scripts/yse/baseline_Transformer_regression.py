import h5py
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

import torch
from torch import nn

import pytorch_lightning as pl
from pytorch_lightning import Trainer
from torch.utils.data import DataLoader, Dataset

DTYPE = torch.float32

### Dataset Class ###
class SimpleDataset(Dataset):
    def __init__(self, inputs, labels, transform=None, preprocess=None):
        # If there's a preprocessing function, apply it
        if preprocess is not None:
            inputs = preprocess(inputs)

        self.inputs = inputs
        self.labels = labels
        self.transform = transform

    def __len__(self):
        return len(self.inputs)

    def __getitem__(self, idx):
        data, labels = torch.tensor(self.inputs[idx]), torch.tensor(self.labels[idx])

        if self.transform is not None:
            data, labels = self.transform(data, labels)
        
        return data, labels

class PredictionDataset(Dataset):
    """Dataset for prediction."""
    def __init__(self, data):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.data[idx]

class TransformerModel(pl.LightningModule):
    def __init__(self, lr):
        super().__init__()
        self.lr          = lr
        self.processor   = nn.Linear(18, 64)
        self.transformer = nn.TransformerEncoder(nn.TransformerEncoderLayer(d_model=64, nhead=8), num_layers=2)
        self.regressor   = nn.Linear(64, 1)
        self.loss_fn     = nn.MSELoss()

    def forward(self, x):
        x = x.permute(1, 0, 2)  # Transformer expects [seq_len, batch, features]
        x = self.processor(x)
        x = self.transformer(x)
        x = x.mean(dim=0)  # Combine sequence elements
        return self.regressor(x)

    def training_step(self, batch, batch_idx):
        x, y  = batch
        y_hat = self(x)
        loss  = self.loss_fn(y_hat.squeeze(), y)
        self.log('train_loss', loss)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = self.loss_fn(y_hat.squeeze(), y)
        self.log('val_loss', loss)

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.lr)
    
if __name__ == '__main__':

    # Hyperparameters
    SEED = 42
    EPOCHS = 50
    BATCH_SIZE = 32
    LEARNING_RATE = 1e-4
    NUM_WORKERS = 7
    DATA_PATH = "../../data/yse.h5"

    with h5py.File(DATA_PATH) as f:
        photo_z  = f['photo_z'][:]
    arg_keep = np.where(photo_z >= 0)

    with h5py.File(DATA_PATH) as f:
        data     = f['banded_data'][arg_keep]
        photo_z  = f['photo_z'][arg_keep]

    n_objects = data.shape[0]
    n_size    = data.shape[-1]
    data      = data.reshape(n_objects, -1, n_size).transpose(0, 2, 1).astype(np.float32)
    labels    = photo_z.astype(np.float32)

    data_train, data_valid, label_train, label_valid = train_test_split(data, labels, test_size=0.2, random_state=SEED)

    train_dataset = SimpleDataset(data_train, label_train, transform=None, preprocess=None)
    train_loader  = DataLoader(dataset=train_dataset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, shuffle=True, persistent_workers=True)

    valid_dataset = SimpleDataset(data_valid, label_valid, transform=None, preprocess=None)
    valid_loader  = DataLoader(dataset=valid_dataset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, shuffle=False, persistent_workers=True)

    model   = TransformerModel(LEARNING_RATE)
    trainer = Trainer(max_epochs=EPOCHS)
    trainer.fit(model, train_loader, valid_loader)

    # Generate predictions
    prediction_dataset    = PredictionDataset(data_valid)
    prediction_dataloader = DataLoader(prediction_dataset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, shuffle=False, persistent_workers=True)
    predictions           = trainer.predict(model, dataloaders=prediction_dataloader)
    predictions_valid     = torch.cat(predictions, dim=0).squeeze().cpu().numpy()

    prediction_dataset    = PredictionDataset(data_train)
    prediction_dataloader = DataLoader(prediction_dataset, batch_size=BATCH_SIZE, num_workers=NUM_WORKERS, shuffle=False, persistent_workers=True)
    predictions           = trainer.predict(model, dataloaders=prediction_dataloader)
    predictions_train     = torch.cat(predictions, dim=0).squeeze().cpu().numpy()

    # Plot results
    sns.jointplot(x=label_train, y=predictions_train, kind="kde")
    plt.plot([label_train.min(), label_train.max()], [label_train.min(), label_train.max()], 'k--')
    plt.xlabel('True')
    plt.ylabel('Predicted')
    plt.show()

    sns.jointplot(x=label_valid, y=predictions_valid, kind="kde")
    plt.plot([label_valid.min(), label_valid.max()], [label_valid.min(), label_valid.max()], 'k--')
    plt.xlabel('True')
    plt.ylabel('Predicted')
    plt.show()

