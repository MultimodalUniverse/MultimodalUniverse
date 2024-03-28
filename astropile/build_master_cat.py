from astropy.coordinates import SkyCoord
from datasets import DatasetBuilder, Dataset
from .utils import get_catalog
import numpy as np
from astropy import units
import pandas as pd


def extract_cat_params(cat: DatasetBuilder):
    """This just grabs the ra, dec, and healpix columns from a catalogue."""
    cat = get_catalog(cat)
    subcat = pd.DataFrame(data=dict((col, cat[col].data) for col in ["ra", "dec", "healpix"]))
    return subcat


def build_catalogue(cats: list[DatasetBuilder], names: list[str], matching_radius: float = 1.0):
    """
    Build a master catalogue from a list of AstroPile catalogues. This extracts
    minimal information from each catalogue and collates it into a single table.

    The table is formatted as: ra, dec, healpix, name1, name2, ..., nameN,
    name1_idx, name2_idx, ..., nameN_idx. where ra and dec are in arcsec,
    healpix is a healpix index, name1, name2, ..., nameN are boolean flags
    indicating whether a source is present in the corresponding catalogue, and
    name1_idx, name2_idx, ..., nameN_idx are the indices of the sources in the
    corresponding catalogue.

    Parameters
    ----------
    cats : list[DatasetBuilder]
        List of AstroPile catalogues to be combined.
    names : list[str]
        List of names for the catalogues. This will appear as the column header
        in the master catalogue for that dataset.
    matching_radius : float, optional
        The maximum separation between two sources in the catalogues to be
        considered a match, by default 1.0 [arcsec].

    Returns
    -------
    master_cat : pd.DataFrame
        The master catalogue containing the combined information from all the
        input catalogues.
    """

    # Set the columns for the master catalogue
    master_cat = pd.DataFrame(
        columns=["ra", "dec", "healpix"] + names + [f"{name}_idx" for name in names]
    )

    for cat, name in zip(cats, names):
        # Extract the relevant columns
        cat = extract_cat_params(cat)

        # Match the catalogues
        master_coords = SkyCoord(master_cat.loc[:, "ra"], master_cat.loc[:, "dec"], unit="deg")
        cat_coords = SkyCoord(cat.loc[:, "ra"], cat.loc[:, "dec"], unit="deg")
        idx, sep2d, _ = master_coords.match_to_catalog_sky(cat_coords)
        mask = sep2d < matching_radius * units.arcsec

        # Update the matching columns
        master_cat.loc[mask, name] = True
        master_cat.loc[mask, name + "_idx"] = idx[mask]

        # Add new rows to the master catalogue
        if len(master_cat) == 0:
            # keep everything for first catalogue
            mask = np.zeros(len(cat), dtype=bool)
        else:
            # match to master catalogue so far
            idx, sep2d, _ = cat_coords.match_to_catalog_sky(master_coords)
            mask = sep2d < matching_radius * units.arcsec
        idx = np.arange(len(cat), dtype=int)
        name_data = []
        name_idx_data = []
        for subname in names:
            if subname != name:
                # Add rows for each catalogue. These are False becaue they didn't match
                name_data.append(np.zeros(np.sum(~mask), dtype=bool))
                name_idx_data.append(-np.ones(np.sum(~mask), dtype=int))
            else:
                # Add rows for the current catalogue. These are True because they are the current catalogue
                name_data.append(np.ones(np.sum(~mask), dtype=bool))
                name_idx_data.append(idx[~mask])
        # Collect the new rows into a DataFrame
        append_cat = pd.DataFrame(
            columns=["ra", "dec", "healpix"] + names + [f"{name}_idx" for name in names],
            data=np.stack(
                [cat.loc[~mask, col] for col in ["ra", "dec", "healpix"]]
                + name_data
                + name_idx_data
            ).T,
        )

        # Append the new rows to the master catalogue
        master_cat = pd.concat([master_cat, append_cat], ignore_index=True)

    # Convert the columns to the correct data types
    master_cat["ra"] = master_cat["ra"].astype(float)
    master_cat["dec"] = master_cat["dec"].astype(float)
    master_cat["healpix"] = master_cat["healpix"].astype(int)
    for name in names:
        master_cat[name] = master_cat[name].astype(bool)
        master_cat[f"{name}_idx"] = master_cat[f"{name}_idx"].astype(int)

    return master_cat
