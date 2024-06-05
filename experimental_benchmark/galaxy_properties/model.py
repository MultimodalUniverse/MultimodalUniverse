import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
import torchvision.models as models
from torchvision import transforms
import torch
from tqdm import tqdm

from modules import spectrum_mlp, resnet1d

__all__ = ["ImageResNet18", "ImageDenseNet121", "ImageEfficientNet", "PhotometryMLP", "SpectrumConvAtt", "SpectrumResNet18"]


class PROVABGSModel(L.LightningModule):
    """This is the base model class for photo-z estimation. Note that it does not contain the model architecture itself"""
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


class ImageModel(PROVABGSModel):
    """This is the base model class for image-based photo-z estimation. Note that it does not contain the model architecture itself"""
    def __init__(
        self,
        input_channels: int = 5,
        n_out: int = 5,
        lr: float = 5e-3
    ):
        super().__init__(lr=lr)

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
        print(y_hat.shape, y.shape)
        loss = F.mse_loss(y_hat, y)
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        return loss


class ImageResNet18(ImageModel):
    """ResNet18 model for galaxy property estimation"""
    def __init__(
        self, 
        input_channels: int = 5, 
        n_out: int = 5, 
        lr: float = 5e-4
    ):
        super().__init__(
            input_channels=input_channels,
            n_out=n_out,
            lr=lr
        )
        self.save_hyperparameters()

        # Set up modified ResNet18
        self.model = models.resnet18(weights=None)
        self.model.conv1 = nn.Conv2d(
                3, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.fc = nn.Linear(512, n_out)


class ImageDenseNet121(ImageModel):
    """DenseNet121 model for galaxy property estimation"""
    def __init__(
        self, 
        input_channels: int = 5, 
        n_out: int = 5, 
        lr: float = 5e-4
    ):
        super().__init__(
            input_channels=input_channels,
            n_out=n_out,
            lr=lr
        )
        self.save_hyperparameters()

        # Set up modified DenseNet121
        self.model = models.densenet121(weights=None)
        self.model.features.conv0 = nn.Conv2d(
            3, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.classifier = nn.Linear(1024, n_out)


class ImageEfficientNetB0(ImageModel):
    """EfficientNet model for galaxy property estimation"""
    def __init__(
        self, 
        input_channels: int = 5, 
        n_out: int = 5, 
        lr: float = 5e-4
    ):
        super().__init__(
            input_channels=input_channels,
            n_out=n_out,
            lr=lr
        )
        self.save_hyperparameters()

        # Set up modified EfficientNet
        self.model = models.efficientnet_b0(weights=None)
        self.model.features[0][0] = nn.Conv2d(
            input_channels, 32, kernel_size=3, stride=2, padding=1, bias=False
        )
        self.model.classifier[1] = nn.Linear(self.model.classifier[1].in_features, n_out)


class PhotometryMLP(PROVABGSModel):
    """Simple MLP model for galaxy property estimation"""
    def __init__(
        self, 
        input_channels: int = 3,
        hidden_channels: int = 64, 
        num_layers: int = 5, 
        n_out: int = 5, 
        lr: float = 5e-3
    ):
        super().__init__(lr=lr)
        self.save_hyperparameters()
        
        # Dynamically create layers based on input parameters
        layers = []
        out_channels = hidden_channels
        for i in range(num_layers):
            layers.append(nn.Linear(input_channels if i == 0 else out_channels, out_channels))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(out_channels, n_out))
        self.model = nn.Sequential(*layers)


class SpectrumConvAtt(PROVABGSModel):
    """Spectrum encoder

    Modified version of the encoder by Serrà et al. (2018), which combines a 3 layer CNN
    with a dot-product attention module. This encoder adds a MLP to further compress the
    attended values into a low-dimensional latent space.

    Paper: Serrà et al., https://arxiv.org/abs/1805.03908
    """

    def __init__(
        self, 
        n_out: int = 5, 
        n_hidden: list = (32, 32), 
        filters = [8, 16, 16, 32],
        sizes = [5, 10, 20, 40],
        act: list = None, 
        dropout: float = 0,
        lr: float = 5e-4,
    ):
        super().__init__(lr=lr)
        self.n_latent = n_out

        self.conv1, self.conv2, self.conv3, self.conv4 = self._conv_blocks(
            filters, sizes, dropout=dropout
        )
        self.n_feature = filters[-1] // 2

        # pools and softmax work for spectra and weights
        self.pool1, self.pool2, self.pool3 = tuple(
            nn.MaxPool1d(s, padding=s // 2) for s in sizes[:3]
        )
        self.softmax = nn.Softmax(dim=-1)

        # small MLP to go from CNN features to latents
        if act is None:
            act = [nn.PReLU(n) for n in n_hidden]
            # last activation identity to have latents centered around 0
            act.append(nn.Identity())
        self.mlp = spectrum_mlp(
            self.n_feature, self.n_latent, n_hidden=n_hidden, act=act, dropout=dropout
        )

    def _conv_blocks(self, filters, sizes, dropout=0):
        convs = []
        for i in range(len(filters)):
            f_in = 1 if i == 0 else filters[i - 1]
            f = filters[i]
            s = sizes[i]
            p = s // 2
            conv = nn.Conv1d(
                in_channels=f_in,
                out_channels=f,
                kernel_size=s,
                padding=p,
            )
            norm = nn.InstanceNorm1d(f)
            act = nn.PReLU(f)
            drop = nn.Dropout(p=dropout)
            convs.append(nn.Sequential(conv, norm, act, drop))
        return tuple(convs)

    def _downsample(self, x):
        # compression
        x = x.unsqueeze(1)
        x = self.pool1(self.conv1(x))
        x = self.pool2(self.conv2(x))
        x = self.pool3(self.conv3(x))
        x = self.conv4(x)
        C = x.shape[1] // 2
        # split half channels into attention value and key
        h, a = torch.split(x, [C, C], dim=1)

        return h, a

    def forward(self, y):
        # run through CNNs
        h, a = self._downsample(y)
        # softmax attention
        a = self.softmax(a)

        # attach hook to extract backward gradient of a scalar prediction
        # for Grad-FAM (Feature Activation Map)
        if ~self.training and a.requires_grad == True:
            a.register_hook(self._attention_hook)

        # apply attention
        x = torch.sum(h * a, dim=2)

        # run attended features into MLP for final latents
        x = self.mlp(x)
        return x

    @property
    def n_parameters(self):
        return sum(p.numel() for p in self.parameters() if p.requires_grad)

    def _attention_hook(self, grad):
        self._attention_grad = grad

    @property
    def attention_grad(self):
        if hasattr(self, "_attention_grad"):
            return self._attention_grad
        else:
            return None


class SpectrumResNet18(PROVABGSModel):
    """ResNet18 model for spectra property estimation"""
    def __init__(
        self, 
        input_channels: int = 1, 
        n_out: int = 5, 
        lr: float = 2e-6
    ):
        super().__init__(lr=lr)
        self.save_hyperparameters()

        # Set up modified ResNet18
        self.model = resnet1d()
        self.model.conv1 = nn.Conv1d(
                1, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.fc = nn.Linear(512, n_out)