import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import r2_score
import lightning as L
import numpy as np


class R2ScoreCallback(L.Callback):
    """Callback to calculate the R^2 score on the validation set."""
    def __init__(
        self,
        properties: list = ['Z_HP', 'Z_MW', 'TAGE_MW', 'AVG_SFR', 'LOG_MSTAR'] # should be same as in dataset
    ):
        super().__init__()
        self.properties = properties
        self.predictions = []
        self.targets = []

    def on_validation_batch_end(self, trainer, pl_module, outputs, batch, batch_idx):
        img, z = batch
        targets = z
        preds = pl_module(img)
        self.predictions.extend(preds.cpu().numpy())
        self.targets.extend(targets.cpu().numpy())

    def on_validation_epoch_end(self, trainer, pl_module):
        predictions = np.array(self.predictions)
        targets = np.array(self.targets)

        # Log the R^2 score for each property
        for i in range(predictions.shape[1]):
            r2 = r2_score(targets[:, i], predictions[:, i])
            pl_module.log(f'{self.properties[i]} R^2', r2, on_epoch=True, prog_bar=True, logger=True)

        # Clear the lists for the next epoch
        self.predictions = []
        self.targets = []
