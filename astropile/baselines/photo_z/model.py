import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
from torchvision import transforms
import torch
from tqdm import tqdm

__all__ = ["PhotoZCNN", "ResNet18"]


class PhotoZModel(L.LightningModule):
    """This is the base model class for photo-z estimation. Note that it does not contain the model architecture itself"""
    def __init__(self, lr: float = 5e-3):
        super().__init__()
        self.save_hyperparameters()
                
    def forward(self, x):
        return self.model(x).squeeze()
        
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


class ResNet18(PhotoZModel):
    """ResNet18 model for galaxy property estimation"""
    def __init__(
        self, 
        input_channels: int = 5, 
        n_out: int = 5, 
        lr: float = 5e-4
    ):
        super().__init__(lr=lr)
        self.save_hyperparameters()

        # Set up modified ResNet18
        self.model = models.resnet18(weights=None)
        self.model.conv1 = nn.Conv2d(
                3, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.fc = nn.Linear(512, n_out)

        # Set up transforms
        self.transform = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomVerticalFlip(),
            transforms.GaussianBlur(3),
        ])

    def training_step(self, batch, batch_idx):
        # Custom training step to apply transforms
        x, y = batch
        x = self.transform(x)
        y_hat = self(x)
        loss = F.mse_loss(y_hat, y)
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        return loss


