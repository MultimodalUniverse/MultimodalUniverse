import pytorch_lightning as pl
import torchmetrics
import torch
import torch.nn.functional as F


def make_conv_block(in_channels, out_channels, kernel_size, stride, padding):
    return torch.nn.Sequential(
        torch.nn.Conv2d(in_channels=in_channels, out_channels=out_channels, kernel_size=kernel_size, stride=stride, padding=padding),
        torch.nn.ReLU(),
        torch.nn.MaxPool2d((2, 2))
    )

class SmallConvModel(pl.LightningModule):

    def __init__(self, config):

        super().__init__()
        self.save_hyperparameters()

        self.config = config

        encoder_layers = []
        in_channels = config['channels']
        for layer_config in config['layers']:
            encoder_layers.append(
                make_conv_block(
                    in_channels=in_channels,
                    out_channels=layer_config['out_channels'],
                    kernel_size=(layer_config['kernel_size'], layer_config['kernel_size']),
                    stride=layer_config['stride'],
                    padding=layer_config['padding']
                )
            )
            # update for next layer
            in_channels = layer_config['out_channels']

        if config['global_average_pooling']:
            encoder_layers += torch.nn.AdaptiveAvgPool2d(1)
            self.representation_dim = config['layers'][-1]['out_channels']
        else:
            self.representation_dim = config['representation_dim']

        self.encoder = torch.nn.Sequential(*encoder_layers)

        head = torch.nn.Sequential(
            torch.nn.Flatten(),
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

        loss = self.get_loss(y_hat, y)
        self.log("train_loss", loss, on_step=False, on_epoch=True, prog_bar=True, logger=True)

        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)

        self.val_accuracy(y_hat, y)
        self.log('val_acc', self.val_accuracy, on_step=False, on_epoch=True, prog_bar=True, logger=True)

        loss = self.get_loss(y_hat, y)
        self.log("val_loss", loss) #, on_step=False, on_epoch=True, prog_bar=True, logger=True)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.config['learning_rate'])


def cross_entropy_loss(y_pred, y, label_smoothing=0., weight=None, reduction='mean'):
    # y should be shape (batch) and ints
    # y_pred should be shape (batch, classes)
    # returns loss of shape (batch)
    # will reduce myself
    return F.cross_entropy(y_pred, y.long(), label_smoothing=label_smoothing, weight=weight, reduction=reduction)


def default_config():
    return {
        'channels': 3,
        'img_size': 224,
        'learning_rate': 1e-3,
        'layers':
            [
                {'out_channels': 32, 'kernel_size': 3, 'stride': 1, 'padding': 0},
                {'out_channels': 32, 'kernel_size': 3, 'stride': 1, 'padding': 0},
                {'out_channels': 32, 'kernel_size': 3, 'stride': 1, 'padding': 0},
            ],
        'representation_dim': 32*26*26,
        'global_average_pooling': False,
        'head_size': 64,
        'num_classes': 10
    }


def main():

    config = default_config()

    model = SmallConvModel(config)
    print(model)

    y_true = torch.randint(0, 10, (1,))

    x = torch.randn(1, 3, 224, 224)
    y = model(x)
    print(y)
    print(y_true)
    
    print(cross_entropy_loss(y, y_true))


if __name__ == '__main__':
    main()
