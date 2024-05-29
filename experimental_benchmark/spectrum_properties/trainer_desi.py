import lightning as L
from dataset import SpectrumDataset
from model import ResNet18_spectrum
from lightning.pytorch.loggers import CSVLogger

if __name__ == "__main__":
    logger = CSVLogger("logs", name="desi")

    trainer = L.Trainer(logger=logger)
    datamodule = SpectrumDataset(
        modality='desi',
        num_workers=8,
        dataset_path='/scratch/msmith/projects/astropile_project/astropile/desi/desi2.py'
        cache_path='/scratch/msmith/projects/astropile_project/astropile_compiled/desi',
    )
    trainer.fit(ResNet18_spectrum(), datamodule)
