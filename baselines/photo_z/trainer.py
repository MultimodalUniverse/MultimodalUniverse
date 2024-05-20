from lightning.pytorch.cli import ArgsType, LightningCLI

from dataset import PhotoZDataset
from model import PhotoZCNN, PhotoZResNet18

def photo_z_cli(args: ArgsType = None, run: bool = True):
    cli = LightningCLI(
        PhotoZResNet18,
        PhotoZDataset,
    )
    return cli

if __name__ == "__main__":
    photo_z_cli(run=True)
