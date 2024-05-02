import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
import pyro
import pyro.distributions as dist
import pyro.distributions.transforms as T
import torch
from tqdm import tqdm

__all__ = ["PhotoZCNN", "PhotoZNF"]


class PhotoZCNN(L.LightningModule):
    def __init__(
            self, 
            input_channels: int = 5, 
            hidden_channels: int = 64, 
            num_layers: int = 5, 
            num_classes: int = 1, 
            lr: float = 5e-3):
        super().__init__()
        self.save_hyperparameters()
        
        # Dynamically create layers based on input parameters
        layers = []
        out_channels = hidden_channels
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
    
    def forward_features(self, x):
        x = self.conv_layers(x)
        x = self.global_avg_pool(x)
        x = torch.flatten(x, 1)
        return x.squeeze()
    
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


class ConditionalFlowStack(dist.conditional.ConditionalComposeTransformModule):
    def __init__(self, input_dim, context_dim, hidden_dims, num_flows, device):

        coupling_transforms = [
            T.conditional_spline(input_dim, context_dim, hidden_dims=hidden_dims, order='quadratic').to(device)
            for _ in range(num_flows)
        ]

        super().__init__(coupling_transforms, cache_size=1)
        

class PhotoZNF(L.LightningModule):
    def __init__(self, FeatureExtractor, feature_size, num_classes, num_flows=1, hidden_dims=[32], lr=5e-4, device='cuda'):
        super().__init__()
        self.save_hyperparameters()
        pyro.clear_param_store()

        self.feature_extractor = FeatureExtractor

        # freeze the feature extractor
        for param in self.feature_extractor.parameters():
            param.requires_grad = False

        self.flow = ConditionalFlowStack(num_classes, feature_size, hidden_dims, num_flows, device)
        self.prior = dist.Normal(torch.zeros(num_classes).to(device), torch.ones(num_classes).to(device))
        self.lr = lr
    
    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.flow.parameters(), lr=self.lr)
        return optimizer

    def single_sample(self, x, n_samples=100):
        z = self.feature_extractor.forward_features(x)
        flow_dist = dist.conditional.ConditionalTransformedDistribution(self.prior, [self.flow]).condition(z)
        return flow_dist.sample(torch.Size([n_samples,]))
        
    def training_step(self, batch, batch_idx):
        x, y = batch
        z = self.feature_extractor.forward_features(x)
        flow_dist = dist.conditional.ConditionalTransformedDistribution(self.prior, [self.flow]).condition(z)
        loss = -flow_dist.log_prob(y.reshape(-1,1)).mean()
        self.log('train_nll', loss, on_epoch=True, prog_bar=True)
        return loss
    
    def validation_step(self, batch, batch_idx):
        x, y = batch
        z = self.feature_extractor.forward_features(x)
        flow_dist = dist.conditional.ConditionalTransformedDistribution(self.prior, [self.flow]).condition(z)
        loss = -flow_dist.log_prob(y.reshape(-1,1)).mean()
        self.log('val_nll', loss, on_epoch=True, prog_bar=True)
        return loss
    
    def test_step(self, x, batch, batch_idx):
        x, y = batch
        z = self.feature_extractor.forward_features(x)
        flow_dist = dist.conditional.ConditionalTransformedDistribution(self.prior, [self.flow]).condition(z)
        loss = -flow_dist.log_prob(y.reshape(-1,1)).mean()
        self.log('test_nll', loss, on_epoch=True, prog_bar=True)
        return loss

class TrainingOnlyProgressBar(L.pytorch.callbacks.TQDMProgressBar):
    def init_validation_tqdm(self):
        bar = tqdm(
            disable=True
        )
        return bar


