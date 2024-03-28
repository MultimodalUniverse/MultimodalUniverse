import pytorch_lightning as pl
import torchmetrics
import torch
import torch.functional as F


def make_conv_block(in_channels, out_channels, kernel_size, stride, padding):
    return torch.nn.Sequential(
        torch.nn.Conv2d(in_channels=in_channels, out_channels=out_channels, kernel_size=kernel_size, stride=stride, padding=padding),
        torch.nn.ReLU(),
        torch.nn.MaxPool2d((2, 2))
    )

class SmallConvModel(pl.LightningModule):

    def __init__(self, config):

        super().__init__()

        self.config = config

        self.encoder = torch.nn.Sequential(*[
            make_conv_block(
                in_channels=layer_config['in_channels'],
                out_channels=layer_config['out_channels'],
                kernel_size=(layer_config['kernel_size'], layer_config['kernel_size']),
                stride=layer_config['stride'],
                padding=layer_config['padding']
            )
            for layer_config in config['layers']
        ])

        # channels * img_size**2 / 2**len(layers)
        # self.representation_dim = int(config['layers'][-1]['out_channels']*(config['img_size']**2)/(2**len(config['layers'])))
        self.representation_dim = config['representation_dim']
        head = torch.nn.Sequential(
            torch.nn.Flatten(),
            # would be 16 (128 / 2**3) but due to stride, is 14
            torch.nn.Linear(in_features=self.representation_dim, out_features=config['head_size']),
            torch.nn.Dropout(0.5),
            torch.nn.ReLU(),
            torch.nn.Linear(in_features=config['head_size'], out_features=config['num_classes']),
            torch.nn.Sigmoid()
        )

        self.head = head

        self.model = torch.nn.Sequential(self.encoder, self.head)

        self.get_loss = cross_entropy_loss

        self.train_accuracy = torchmetrics.Accuracy(task='multiclass', num_classes=config['num_classes'])
        self.val_accuracy = torchmetrics.Accuracy(task='multiclass', num_classes=config['num_classes'])

    def forward(self, x):
        return self.model(x)

    def predict_step(self, batch, batch_idx):
        if isinstance(batch, list):  # (x, y) list including labels from dataloader that should be ignored
            return self.forward(batch[0])
        else:
            return self.forward(batch)

    def configure_optimizer(self):
        return torch.optim.Adam()

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)

        self.train_accuracy(y_hat, y)
        self.log('train_acc', self.train_accuracy, on_step=False, on_epoch=True, prog_bar=True, logger=True)

        loss = self.get_loss(y_hat, y.to(torch.float32))
        self.log("train_loss", loss, on_step=False, on_epoch=True, prog_bar=True, logger=True)

        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)

        self.val_accuracy(y_hat, y)
        self.log('val_acc', self.val_accuracy, on_step=False, on_epoch=True, prog_bar=True, logger=True)

        loss = self.get_loss(y_hat, y.to(torch.float32))
        self.log("val_loss", loss) #, on_step=False, on_epoch=True, prog_bar=True, logger=True)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters())


def cross_entropy_loss(y_pred, y, label_smoothing=0., weight=None):
    # y should be shape (batch) and ints
    # y_pred should be shape (batch, classes)
    # returns loss of shape (batch)
    # will reduce myself
    return F.cross_entropy(y_pred, y.long(), label_smoothing=label_smoothing, weight=weight, reduction='none')



def main():

    config = {
        'channels': 3,
        'img_size': 224,
        'layers':
            [
                {'in_channels': 3, 'out_channels': 32, 'kernel_size': 3, 'stride': 1, 'padding': 0},
                {'in_channels': 32, 'out_channels': 32, 'kernel_size': 3, 'stride': 1, 'padding': 0},
                {'in_channels': 32, 'out_channels': 32, 'kernel_size': 3, 'stride': 1, 'padding': 0},
            ],
        'representation_dim': 32*26*26,
        'head_size': 64,
        'num_classes': 10
    }

    model = SmallConvModel(config)
    print(model)

    x = torch.randn(1, 3, 224, 224)
    y = model(x)
    print(y)


if __name__ == '__main__':
    main()
