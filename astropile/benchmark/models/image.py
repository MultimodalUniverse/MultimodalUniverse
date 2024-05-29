import lightning as L
import torch
import torch.nn as nn
import torchvision.models as models
from typing import Union, List, Optional
import torchvision.transforms as transforms

__all__ = ['ConvolutionalModel']

class _ImageModel(L.LightningModule):
    """This is the base model class for image classification. Note that it does not contain the model architecture itself"""
    def __init__(self, 
                 input_channels: int = 3,
                 output_size: int = 1,
                 loss: str = 'mse',
                 target: Union[str, List[str]] = 'Z',
                 range_compression_factor: float = 0.01,
                 lr: float = 1e-3):
        super().__init__()

        if loss == 'mse':
            self.loss = nn.MSELoss()
        else:
            raise ValueError(f"Loss {loss} not supported.")

        # Standard image augmentation
        self.transform = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomVerticalFlip(),
            transforms.RandomRotation(90)
        ])

    def forward(self, batch):
        x = batch['image']['array']
        # Apply range compression to inputs
        x = torch.arcsinh(x / self.hparams.range_compression_factor)*self.hparams.range_compression_factor
        x = x * 10.0
        x = torch.clamp(x, -1.0, 1.0)
        return self.model(x)
        
    def training_step(self, batch, batch_idx):
        y = batch[self.hparams.target]
        # Apply standard image augmentation
        batch['image']['array'] = self.transform(batch['image']['array'])
        y_hat = self(batch)
        loss = self.loss(y_hat.squeeze(), y.squeeze())
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(self, batch, batch_idx):
        y = batch[self.hparams.target]
        y_hat = self(batch)
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
                 model_name: str = "resnet18",
                 loss: str = "mse",
                 target: Union[str, List[str]] = 'Z',
                 range_compression_factor: Optional[float] = None,
                 lr: float = 5e-4):
        super().__init__(input_channels=input_channels, 
                         output_size=output_size, 
                         loss=loss, 
                         target=target,
                         lr=lr)
        self.save_hyperparameters()

        if self.hparams.model_name == "resnet18":
            self.model = models.resnet18(weights=None)
            self.model.conv1 = nn.Conv2d(
                    self.hparams.input_channels, 
                    64, 
                    kernel_size=7, stride=2, padding=3, bias=False
            )
            self.model.fc = nn.Linear(512, output_size)
        else:
            raise ValueError(f"Model {self.hparams.name} not supported.")
        