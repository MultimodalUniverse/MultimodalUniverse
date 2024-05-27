import lightning as L
import torch
import torch.nn as nn
import torchvision.models as models
from typing import Union, List

__all__ = ['ConvolutionalModel']

class _ImageModel(L.LightningModule):
    """This is the base model class for image classification. Note that it does not contain the model architecture itself"""
    def __init__(self, 
                 input_channels: int = 3,
                 output_size: int = 1,
                 loss: str = 'mse',
                 target: Union[str, List[str]] = 'Z',
                 lr: float = 1e-3):
        super().__init__()

        if loss == 'mse':
            self.loss = nn.MSELoss()
        else:
            raise ValueError(f"Loss {loss} not supported.")

    def forward(self, x):
        return self.model(x)
        
    def training_step(self, batch, batch_idx):
        x = batch['image']['array']
        y = batch[self.hparams.target]
        y_hat = self(x)
        loss = self.loss(y_hat.squeeze(), y.squeeze())
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x = batch['image']['array']
        y = batch[self.hparams.target]
        y_hat = self(x)
        loss = self.loss(y_hat.squeeze(), y.squeeze())
        self.log('val_loss', loss, on_epoch=True, prog_bar=True)
        return loss
    
    def configure_optimizers(self):
        optimizer = torch.optim.AdamW(self.parameters(), lr=self.hparams.lr)
        return optimizer
    

class ConvolutionalModel(_ImageModel):
    def __init__(self, 
                 input_channels: int = 3, 
                 output_size: int = 1,
                 name: str = "resnet18",
                 loss: str = "mse",
                 target: Union[str, List[str]] = 'Z',
                 lr: float = 5e-4):
        super().__init__(input_channels=input_channels, 
                         output_size=output_size, 
                         loss=loss, 
                         target=target,
                         lr=lr)
        self.save_hyperparameters()

        if self.hparams.name == "resnet18":
            self.model = models.resnet18(weights=None)
            self.model.conv1 = nn.Conv2d(
                    self.hparams.input_channels, 
                    64, 
                    kernel_size=7, stride=2, padding=3, bias=False
            )
            self.model.fc = nn.Linear(512, output_size)
        else:
            raise ValueError(f"Model {self.hparams.name} not supported.")
        