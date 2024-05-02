from lightning.pytorch.cli import ArgsType, LightningCLI

from dataset import PhotoZDataset
from model import PhotoZCNN

def photo_z_cli(args: ArgsType = None, run: bool = True):
    cli = LightningCLI(
        PhotoZCNN,
        PhotoZDataset,
    )
    return cli

if __name__ == "__main__":
    photo_z_cli(run=True)
