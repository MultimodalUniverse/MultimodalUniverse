import lightning as L
from dataset import SpectrumDataset
from model import ResNet18_spectrum
from lightning.pytorch.loggers import CSVLogger

if __name__ == "__main__":
    logger = CSVLogger("logs", name="sdss")

    trainer = L.Trainer(logger=logger)
    datamodule = SpectrumDataset(
        modality='sdss',
        num_workers=8,
        dataset_path='/scratch/msmith/projects/astropile_project/astropile/sdss/sdss.py',
        cache_path='/scratch/msmith/projects/astropile_project/astropile_compiled/sdss',
    )
    trainer.fit(ResNet18_spectrum(), datamodule)
