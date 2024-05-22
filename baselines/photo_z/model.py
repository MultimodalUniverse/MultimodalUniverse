import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
import torch
from tqdm import tqdm

__all__ = ["PhotoZCNN", "PhotoZResNet18"]


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


class PhotoZCNN(PhotoZModel):
    """Simple CNN model for photo-z estimation"""
    def __init__(
            self, 
            input_channels: int = 5, 
            hidden_channels: int = 64, 
            num_layers: int = 5, 
            n_out: int = 1, 
            lr: float = 5e-3):
        super().__init__(lr=lr)
        self.save_hyperparameters()
        
        # Dynamically create layers based on input parameters
        layers = []
        out_channels = hidden_channels
        for i in range(num_layers):
            layers.append(nn.Conv2d(input_channels if i == 0 else out_channels, out_channels, kernel_size=3, padding=1))
            layers.append(nn.ReLU())
            layers.append(nn.MaxPool2d(kernel_size=2, stride=2))
        layers.append(nn.AdaptiveAvgPool2d(1))
        layers.append(nn.Flatten())
        layers.append(nn.Linear(out_channels, n_out))
        self.model = nn.Sequential(*layers)


class PhotoZResNet18(PhotoZModel):
    """ResNet18 model for photo-z estimation"""
    def __init__(self, n_out: int = 1, lr: float = 5e-3):
        super().__init__(lr=lr)
        self.save_hyperparameters()

        # Set up modified ResNet18
        self.model = models.resnet18(weights=None)
        self.model.conv1 = nn.Conv2d(
                3, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.fc = nn.Linear(512, n_out)



