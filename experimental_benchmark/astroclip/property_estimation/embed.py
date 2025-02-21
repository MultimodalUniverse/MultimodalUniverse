import os
from argparse import ArgumentParser

import torch
import numpy as np
from astropy.table import Table, join
from datasets import load_from_disk, load_dataset
from provabgs import models as Models
from torchvision.transforms import CenterCrop, Compose
from tqdm import tqdm

from datamodule import AstroClipCollator, AstroClipDataloader
from astroclip.models import AstroClipModel

def embed_provabgs(
    astroclip_model_path: str,
    desi_decals_path: str,
    provabgs_path: str,
    save_path: str = None,
    batch_size: int = 128,
    num_workers: int = 20,
):
    """Cross-match the AstroCLIP and PROVABGS datasets."""

    # Set up the model
    astroclip = AstroClipModel.load_from_checkpoint(
        checkpoint_path=astroclip_model_path,
    )
    astroclip.eval()

    # Load the PROVABGS dataset
    cols = ['LOG_MSTAR', 'MAG_G', 'MAG_R', 'MAG_Z', 'AVG_SFR', 'Z_MW', 'Z_HP', 'TAGE_MW', 'object_id']
    provabgs = load_dataset(provabgs_path)
    provabgs.set_format('numpy', columns=cols)
    provabgs = provabgs['train'] # it's all in train

        # Convert provabgs to table
    provabgs = Table({col: np.array(provabgs[col]) for col in cols})

    # Scale the properties
    provabgs["LOG_MSTAR"] = provabgs["LOG_MSTAR"]
    provabgs["sSFR"] = np.log(provabgs["AVG_SFR"]) - np.log(provabgs["Z_MW"])
    provabgs["Z_MW"] = np.log(provabgs["Z_MW"])

    # Filter out galaxies with no best fit model
    provabgs = provabgs[
        (provabgs["LOG_MSTAR"] > 0)
        * (provabgs["MAG_G"] > 0)
        * (provabgs["MAG_R"] > 0)
        * (provabgs["MAG_Z"] > 0)
    ]

    # Load the AstroCLIP dataset
    dataloader = AstroClipDataloader(
        desi_decals_path,
        batch_size=batch_size,
        num_workers=num_workers,
        collate_fn=AstroClipCollator(),
        columns=["image", "targetid", "spectrum"],
    )
    dataloader.setup("fit")

    # Process the images
    with torch.no_grad():
        train_image_embeddings, train_spectrum_embeddings, train_targetids = [], [], []
        for batch in tqdm(dataloader.train_dataloader(), desc="Processing train images"):
            train_image_embeddings.append(astroclip(batch["image"].cuda(), input_type="image").detach().cpu().numpy())
            train_spectrum_embeddings.append(astroclip(batch["spectrum"].cuda(), input_type="spectrum").detach().cpu().numpy())
            train_targetids.append(batch["object_id"])

        test_image_embeddings, test_spectrum_embeddings, test_targetids = [], [], []
        for batch in tqdm(dataloader.val_dataloader(), desc="Processing test images"):
            test_image_embeddings.append(astroclip(batch["image"].cuda(), input_type="image").detach().cpu().numpy())
            test_spectrum_embeddings.append(astroclip(batch["spectrum"].cuda(), input_type="spectrum").detach().cpu().numpy())
            test_targetids.append(batch["object_id"])

    # Create tables for the train and test datasets
    train_table = Table(
        {
            "object_id": np.concatenate(train_targetids),
            "image_embedding": np.concatenate(train_image_embeddings),
            "spectrum_embedding": np.concatenate(train_spectrum_embeddings),
        }
    )

    test_table = Table(
        {
            "object_id": np.concatenate(test_targetids),
            "image_embedding": np.concatenate(test_image_embeddings),
            "spectrum_embedding": np.concatenate(test_spectrum_embeddings),
        }
    )

    # Join the PROVABGS and AstroCLIP datasets
    train_provabgs = join(
        provabgs, train_table, keys_left="object_id", keys_right="object_id"
    )
    test_provabgs = join(
        provabgs, test_table, keys_left="object_id", keys_right="object_id"
    )
    print("Number of galaxies in train:", len(train_provabgs))
    print("Number of galaxies in test:", len(test_provabgs))
    
    # Save the datasets
    train_provabgs.write(os.path.join(save_path, "train_provabgs.hdf5"))
    test_provabgs.write(os.path.join(save_path, "test_provabgs.hdf5"))

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "astroclip_model_path",
        type=str,
        help="Path to the AstroCLIP dataset.",
    )
    parser.add_argument(
        "desi_decals_path",
        type=str,
        help="Path to the DESI-DECaLS dataset.",
    )
    parser.add_argument(
        "provabgs_path",
        type=str,
        help="Path to the PROVABGS dataset.",
    )
    parser.add_argument(
        "save_path",
        type=str,
        help="Path to save the paired datasets.",
    )

    args = parser.parse_args()
    embed_provabgs(
        astroclip_model_path=args.astroclip_model_path,
        desi_decals_path=args.desi_decals_path,
        provabgs_path=args.provabgs_path,
        save_path=args.save_path,
    )
