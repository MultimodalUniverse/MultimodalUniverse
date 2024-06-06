import lightning as L
import torchmetrics
import torch
import torch.nn as nn
import torch.nn.functional as F
from torchvision import models
from torchvision import transforms

__all__ = ["ResNet18"]


class GZ10Model(L.LightningModule):
    """This is the base model class for galaxy property estimation. Note that it does not contain the model architecture itself"""
    def __init__(
        self,
        input_channels: int = 3,
        num_classes: int = 10,
        lr: float = 5e-5,
        label_smoothing: float = 0.,
        weight: torch.Tensor = None,
        reduction: str = 'mean',
        top_k: int = 1,
    ):
        super().__init__()
        
        # Set up metrics
        self.accuracy_top1 = torchmetrics.Accuracy(task='multiclass', num_classes=num_classes, top_k=1)
        self.accuracy_top5 = torchmetrics.Accuracy(task='multiclass', num_classes=num_classes, top_k=5)

    def forward(self, x):
        return self.model(x)

    def get_loss(self, y_pred, y):
        return F.cross_entropy(
            y_pred, 
            y.long(), 
            label_smoothing=self.hparams.label_smoothing, 
            weight=self.hparams.weight, 
            reduction=self.hparams.reduction
        )

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(self.transforms(x))

        # Calculate loss
        loss = self.get_loss(y_hat, y)

        # Log metrics
        self.log("train_loss", loss, on_epoch=True, prog_bar=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)

        # Calculate loss and accuracy
        self.accuracy_top1(y_hat, y)
        self.accuracy_top5(y_hat, y)
        loss = self.get_loss(y_hat, y)

        # Log metrics
        self.log("val_acc_top1", self.accuracy_top1, on_epoch=True, prog_bar=True)
        self.log("val_acc_top5", self.accuracy_top5, on_epoch=True, prog_bar=True)
        self.log("val_loss", loss, on_epoch=True, prog_bar=True)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.hparams.lr)


class ResNet18(GZ10Model):
    """ResNet18 model for galaxy property estimation"""
    def __init__(
        self,
        input_channels: int = 3,
        num_classes: int = 10,
        lr: float = 1e-4,
        label_smoothing: float = 0.,
        weight: torch.Tensor = None,
        reduction: str = 'mean',
        top_k: int = 1,
    ):
        super().__init__(
            input_channels=input_channels,
            num_classes=num_classes,
            lr=lr,
            label_smoothing=label_smoothing,
            weight=weight,
            reduction=reduction,
            top_k=top_k
        )
        self.save_hyperparameters()

        # Set up the model
        self.model = models.resnet18(weights=None)
        self.model.conv1 = nn.Conv2d(
                input_channels, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.fc = nn.Linear(512, num_classes)  

        # Set up transforms
        self.transforms = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomVerticalFlip(),
            transforms.RandomAffine(0, translate=(0.2, 0.2)),
            transforms.RandomRotation(180),
            transforms.ColorJitter(brightness=0.1, contrast=0.1, saturation=0.1, hue=0.1),
        ])


class EfficientNetB0(GZ10Model):
    """EfficientNetB0 model for galaxy property estimation"""
    def __init__(
        self,
        input_channels: int = 3,
        num_classes: int = 10,
        lr: float = 1e-4,
        label_smoothing: float = 0.,
        weight: torch.Tensor = None,
        reduction: str = 'mean',
        top_k: int = 1,
    ):
        super().__init__(
            input_channels=input_channels,
            num_classes=num_classes,
            lr=lr,
            label_smoothing=label_smoothing,
            weight=weight,
            reduction=reduction,
            top_k=top_k
        )
        self.save_hyperparameters()

        # Set up the model
        self.model = models.efficientnet_b0(weights=None)
        self.model.features[0][0] = nn.Conv2d(
            input_channels, 32, kernel_size=3, stride=2, padding=1, bias=False
        )
        self.model.classifier[1] = nn.Linear(self.model.classifier[1].in_features, num_classes)

        # Set up transforms
        self.transforms = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomVerticalFlip(),
            transforms.RandomAffine(0, translate=(0.2, 0.2)),
            transforms.RandomRotation(180),
            transforms.ColorJitter(brightness=0.1, contrast=0.1, saturation=0.1, hue=0.1),
        ])


class DenseNet121(GZ10Model):
    """DenseNet121 model for galaxy property estimation"""
    def __init__(
        self,
        input_channels: int = 3,
        num_classes: int = 10,
        lr: float = 1e-4,
        label_smoothing: float = 0.,
        weight: torch.Tensor = None,
        reduction: str = 'mean',
        top_k: int = 1,
    ):
        super().__init__(
            input_channels=input_channels,
            num_classes=num_classes,
            lr=lr,
            label_smoothing=label_smoothing,
            weight=weight,
            reduction=reduction,
            top_k=top_k
        )
        self.save_hyperparameters()

        # Set up the model
        self.model = models.densenet121(weights=None)
        self.model.features.conv0 = nn.Conv2d(
            input_channels, 64, kernel_size=7, stride=2, padding=3, bias=False
        )
        self.model.classifier = nn.Linear(1024, num_classes)

        # Set up transforms
        self.transforms = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomVerticalFlip(),
            transforms.RandomAffine(0, translate=(0.2, 0.2)),
            transforms.RandomRotation(180),
            transforms.ColorJitter(brightness=0.1, contrast=0.1, saturation=0.1, hue=0.1),
        ])


class SmallConvModel(GZ10Model):
    """Small convolutional model for galaxy property estimation"""
    def __init__(
        self,
        input_channels: int = 3,
        num_classes: int = 10,
        lr: float = 1e-4,
        label_smoothing: float = 0.,
        weight: torch.Tensor = None,
        reduction: str = 'mean',
        top_k: int = 1,
    ):
        super().__init__(
            input_channels=input_channels,
            num_classes=num_classes,
            lr=lr,
            label_smoothing=label_smoothing,
            weight=weight,
            reduction=reduction,
            top_k=top_k
        )
        self.save_hyperparameters()

        # Set up the model, input image size is 256x256
        self.model = nn.Sequential(
            nn.Conv2d(input_channels, 32, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Conv2d(32, 64, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Conv2d(64, 128, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Conv2d(128, 256, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Conv2d(256, 512, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2, stride=2),
            nn.Flatten(),
            nn.Linear(512 * 8 * 8, num_classes),
        )

        # Set up transforms
        self.transforms = transforms.Compose([
            transforms.RandomHorizontalFlip(),
            transforms.RandomVerticalFlip(),
            transforms.ColorJitter(brightness=0.1, contrast=0.1, saturation=0.1, hue=0.1),
        ])
