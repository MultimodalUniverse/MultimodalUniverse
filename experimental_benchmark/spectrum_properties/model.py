import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
import resnet1d
from torchvision import transforms
import torch
from tqdm import tqdm

__all__ = ["ResNet18_spectrum"]


class ParentModel(L.LightningModule):
    """This is the base model class. Note that it does not contain the model architecture itself"""
    def __init__(self, lr: float = 5e-3):
        super().__init__()
        self.save_hyperparameters()
                
    def forward(self, x):
        return self.model(x)
        
    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.mse_loss(y_hat, y)
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.mse_loss(y_hat, y)
        self.log('val_loss', loss, on_epoch=True, prog_bar=True)
        return loss
    
    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.hparams.lr)
        return optimizer


class ResNet18_spectrum(ParentModel):
    """ResNet18 model for spectra property estimation"""
    def __init__(
        self, 
        input_channels: int = 1, 
        n_out: int = 1, 
        lr: float = 2e-6
    ):
        super().__init__(lr=lr)
        self.save_hyperparameters()

        # Set up modified ResNet18
        self.model = resnet1d.resnet18()
        self.model.conv1 = nn.Conv1d(
                1, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.fc = nn.Linear(512, n_out)

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.huber_loss(y_hat, y)
        self.log('train_loss', loss, on_step=True, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.huber_loss(y_hat, y)
        self.log('val_loss', loss, on_epoch=True, prog_bar=True)
        return loss
