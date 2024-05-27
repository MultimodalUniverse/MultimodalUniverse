import lightning as L
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.metrics import Accuracy
import torchvision.models as models
from torchvision import transforms
from tqdm import tqdm

__all__ = ["PhotoClassCNN", "ResNet18"]


class PhotoClassModel(L.LightningModule):  # currently regression, need to change to classification
    """This is the base model class for photometric classification. Note that it does not contain the model architecture itself"""
    def __init__(self, lr: float = 5e-3):
        super().__init__()
        self.save_hyperparameters()
        self.learning_rate = learning_rate

        # model architecture
        self.conv1 = nn.Conv2d(3, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 32, 3, 1)
        self.conv3 = nn.Conv2d(32, 64, 3, 1)
        self.conv4 = nn.Conv2d(64, 64, 3, 1)

        self.pool1 = torch.nn.MaxPool2d(2)
        self.pool2 = torch.nn.MaxPool2d(2)

        n_sizes = self._get_output_shape(input_shape)

        # linear layers for classifier head
        self.fc1 = nn.Linear(n_sizes, 512)
        self.fc2 = nn.Linear(512, 128)
        self.fc3 = nn.Linear(128, num_classes)

        self.accuracy = Accuracy(task="multiclass", num_classes=10)

    def _get_output_shape(self, shape):
        '''returns the size of the output tensor from the conv layers'''

        batch_size = 1
        inp = torch.autograd.Variable(torch.rand(batch_size, *shape))

        output_feat = self._feature_extractor(inp)
        n_size = output_feat.data.view(batch_size, -1).size(1)
        return n_size

    # computations
    def _feature_extractor(self, x):
        '''extract features from the conv blocks'''
        x = F.relu(self.conv1(x))
        x = self.pool1(F.relu(self.conv2(x)))
        x = F.relu(self.conv3(x))
        x = self.pool2(F.relu(self.conv4(x)))
        return x


    def forward(self, x):
       '''produce final model output'''
       x = self._feature_extractor(x)
       x = x.view(x.size(0), -1)
       x = F.relu(self.fc1(x))
       x = F.relu(self.fc2(x))
       x = F.log_softmax(self.fc3(x), dim=1)
       return x

    # def forward(self, x):
    #     return self.model(x).squeeze()

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.nll_loss(y_hat, y)
        preds = torch.argmax(y_hat, dim=1)
        acc = self.accuracy(preds, y)
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        self.log('train_acc', acc, on_step=True, on_epoch=True, logger=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.nll_loss(y_hat, y)
        preds = torch.argmax(y_hat, dim=1)
        acc = self.accuracy(preds, y)
        self.log('val_loss', loss, on_epoch=True, prog_bar=True)
        self.log('val_acc', acc, prog_bar=True)
        return loss

    def test_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.nll_loss(y_hat, y)

        preds = torch.argmax(y_hat, dim=1)
        acc = self.accuracy(preds, y)
        self.log('test_loss', loss, prog_bar=True)
        self.log('test_acc', acc, prog_bar=True)
        return loss

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.hparams.lr)
        return optimizer


class ResNet18(PhotoClassModel):
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
        loss = F.nll_loss(y_hat, y)
        self.log('train_loss', loss, on_epoch=True, prog_bar=True)
        return loss
