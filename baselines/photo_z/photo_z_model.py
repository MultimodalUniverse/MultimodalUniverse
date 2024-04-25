import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
from tqdm import tqdm

class SimpleCNN(L.LightningModule):
    def __init__(self, input_channels, layer_width, num_layers, num_classes, learning_rate=1e-3):
        super(SimpleCNN, self).__init__()
        self.save_hyperparameters()
        self.learning_rate = learning_rate
        
        # Dynamically create layers based on input parameters
        layers = []
        out_channels = layer_width
        for i in range(num_layers):
            layers.append(nn.Conv2d(input_channels if i == 0 else out_channels, out_channels, kernel_size=3, padding=1))
            layers.append(nn.ReLU())
            layers.append(nn.MaxPool2d(kernel_size=2, stride=2))
        
        self.conv_layers = nn.Sequential(*layers)
        self.global_avg_pool = nn.AdaptiveAvgPool2d(1)
        self.fc = nn.Linear(out_channels, num_classes)
        
    def forward(self, x):
        x = self.conv_layers(x)
        x = self.global_avg_pool(x)
        x = torch.flatten(x, 1)
        x = self.fc(x)
        return x.squeeze()
    
    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.learning_rate)
        return optimizer
    
    def _r2(self, y_hat, y):
        ss_res = torch.sum((y - y_hat) ** 2, dim=0)
        ss_tot = torch.sum((y - torch.mean(y, dim=0)) ** 2, dim=0)
        return 1 - ss_res / ss_tot
    
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
    
    def test_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.mse_loss(y_hat, y)
        self.log('test_loss', loss, on_epoch=True, prog_bar=True)
        return loss
    
class TrainingOnlyProgressBar(L.pytorch.callbacks.TQDMProgressBar):
    def init_validation_tqdm(self):
        bar = tqdm(
            disable=True
        )
        return bar
