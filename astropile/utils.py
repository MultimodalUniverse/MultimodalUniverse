import os
from datasets import DatasetBuilder, Dataset
from astropy.table import Table, hstack, vstack
from astropy.coordinates import SkyCoord
from astropy import units as u
from typing import List
from functools import partial
from multiprocessing import Pool
import numpy as np
import h5py
import pandas as pd
from astropy import units

def _file_to_catalog(filename: str, keys: List[str],  keys_mapper: dict = {}):
    with h5py.File(filename, 'r') as data:
        return Table({k: data[keys_mapper[k]] if k in keys_mapper.keys() else  data[k] for k in keys})

def get_catalog(dset: DatasetBuilder,
                keys: List[str] = ['object_id', 'ra', 'dec', 'healpix'],
                keys_mapper: dict = None,
                healpix_indices: List[int] = None,
                split: str = 'train',
                num_proc: int = 1):
    """Return the catalog of a given astropile parent sample.
    
    Args:
        dset (GeneratorBasedBuilder): An AstroPile dataset builder.
        keys (List[str], optional): List of column names to include in the catalog. Defaults to ['object_id', 'ra', 'dec', 'healpix'].
        keys_mapper (dict, optional): Dictionary to map the column names in the catalog to the keys in the data files. Defaults to None.
        healpix_indices (List[int], optional): List of healpix indices to include in the catalog. Defaults to None.
        split (str, optional): The split of the dataset to retrieve the catalog from. Defaults to 'train'.
        num_proc (int, optional): Number of processes to use for parallel processing. Defaults to 1.

    Returns:
        astropy.table.Table: The catalog of the parent sample.
        
    Raises:
        ValueError: If no data files are specified in the dataset builder.
    """
    if not dset.config.data_files:
        raise ValueError(f"At least one data file must be specified, but got data_files={dset.config.data_files}")
    catalogs = []

    if healpix_indices is not None:
        data_files = [f for f in dset.config.data_files[split] if int(f.split('healpix=')[1].split('/')[0]) in healpix_indices]
    else:
        data_files = dset.config.data_files[split]

    if num_proc > 1:
        with Pool(num_proc) as pool:
            catalogs = pool.map(partial(_file_to_catalog, keys=keys, keys_mapper=keys_mapper), data_files)
    else:
        for filename in data_files:
            catalogs.append(_file_to_catalog(filename, keys=keys, keys_mapper=keys_mapper))
    return vstack(catalogs)

def cross_match_datasets(left : DatasetBuilder, 
                         right : DatasetBuilder,
                         left_keys_mapper : dict = {},
                         right_keys_mapper : dict = {},
                         cache_dir : str = None,
                         keep_in_memory : bool = False,
                         matching_radius : float = 1., 
                         return_catalog_only : bool = False,
                         num_proc : int = None):
    """ Utility function to generate a new cross-matched dataset from two AstroPile 
    datasets.

    Args:
        left (GeneratorBasedBuilder): The left dataset to be cross-matched.
        right (GeneratorBasedBuilder): The right dataset to be cross-matched.
        left_keys_mapper (dict, optional): Dictionary to map the column names in the left catalog to the keys in the data files. Defaults to None.
        right_keys_mapper (dict, optional): Dictionary to map the column names in the right catalog to the keys in the data files. Defaults to None.
        cache_dir (str, optional): The directory to cache the cross-matched dataset. Defaults to None.
        keep_in_memory (bool, optional): If True, the cross-matched dataset will be kept in memory. Defaults to False.
        matching_radius (float, optional): The maximum separation in arcseconds for a match to be considered. Defaults to 1.
        return_catalog_only (bool, optional): If True, only the cross-matched catalog will be returned. Defaults to False.

    Returns:
        tuple: A tuple containing the cross-matched catalog and the new dataset.

    Raises:
        AssertionError: If the number of matches in the cross-matched catalog is not equal for both datasets.

    Example:
        left_dataset = ...
        right_dataset = ...
        matched_catalog, new_dataset = cross_match_datasets(left_dataset, right_dataset)
    """
    # Identify the intersection of healpix regions between the two datasets
    left_healpix = [int(f.split('healpix=')[1].split('/')[0]) for f in left.config.data_files['train']]
    right_healpix = [int(f.split('healpix=')[1].split('/')[0]) for f in right.config.data_files['train']]
    common_healpix = list(set(left_healpix).intersection(right_healpix))
    print("Number of common healpix regions: ", len(common_healpix))

    # Access the catalogs for both datasets
    cat_left = get_catalog(left, keys_mapper=left_keys_mapper, healpix_indices=common_healpix, num_proc=num_proc)
    cat_left['sc'] = SkyCoord(cat_left['ra'], 
                              cat_left['dec'], unit='deg')
    
    cat_right = get_catalog(right, keys_mapper=right_keys_mapper, healpix_indices=common_healpix, num_proc=num_proc)
    cat_right['sc'] = SkyCoord(cat_right['ra'],
                               cat_right['dec'], unit='deg')

    # Cross match the catalogs and restricting them to matches
    idx, sep2d, _ = cat_left['sc'].match_to_catalog_sky(cat_right['sc'])
    mask = sep2d < matching_radius*u.arcsec
    cat_left = cat_left[mask]
    cat_right = cat_right[idx[mask]]
    assert len(cat_left) == len(cat_right), "There was an error in the cross-matching."
    print("Initial number of matches: ", len(cat_left))
    matched_catalog = hstack([cat_left, cat_right], 
                             table_names=[left.config.name, right.config.name],
                             uniq_col_name='{table_name}_{col_name}')
    # Remove objects that were matched between the two catalogs but fall under different healpix indices
    mask = matched_catalog[f'{left.config.name}_healpix'] == matched_catalog[f'{right.config.name}_healpix']
    matched_catalog = matched_catalog[mask]
    print("Number of matches lost at healpix region borders: ", len(cat_left) - len(matched_catalog))
    print("Final size of cross-matched catalog: ", len(matched_catalog))

    # Adding default columns to respect format
    matched_catalog['object_id'] = matched_catalog[left.config.name+'_object_id']
    matched_catalog['ra'] = 0.5*(matched_catalog[left.config.name+'_ra'] +
                                 matched_catalog[right.config.name+'_ra'])
    matched_catalog['dec'] = 0.5*(matched_catalog[left.config.name+'_dec'] +
                                 matched_catalog[right.config.name+'_dec'])
    
    # Check that all matches have the same healpix index
    assert np.all(matched_catalog[left.config.name+'_healpix'] == matched_catalog[right.config.name+'_healpix']), "There was an error in the cross-matching."
    matched_catalog['healpix'] = matched_catalog[left.config.name+'_healpix']
    matched_catalog = matched_catalog.group_by(['healpix'])

    if return_catalog_only:
        return matched_catalog

    # Retrieve the list of files of both datasets
    files_left = left.config.data_files['train']
    files_right = right.config.data_files['train']
    catalog_groups = [group for group in matched_catalog.groups]
    # Create a generator function that merges the two generators
    def _generate_examples(groups):
        for group in groups:
            healpix = group['healpix'][0]
            generators = [
                        # Build generators that only reads the files corresponding to the current healpix index
                        left._generate_examples(
                                        files=[files_left[[i for i in range(len(files_left)) if f'healpix={healpix}'in files_left[i]][0]]],
                                        object_ids=[group[left.config.name+'_object_id']]),
                        right._generate_examples(
                                        files=[files_right[[i for i in range(len(files_right)) if f'healpix={healpix}'in files_right[i]][0]]],
                                        object_ids=[group[right.config.name+'_object_id']])
                    ]
            # Retrieve the generators for both datasets
            for i, examples in enumerate(zip(*generators)):
                left_id, example_left = examples[0]
                right_id, example_right = examples[1]
                assert str(group[i][left.config.name+'_object_id']) in left_id, "There was an error in the cross-matching generation."
                assert str(group[i][right.config.name+'_object_id']) in right_id, "There was an error in the cross-matching generation."
                example_left.update(example_right)
                yield example_left
    
    # Merging the features of both datasets
    features = left.info.features.copy()
    features.update(right.info.features)

    # Generating a description for the new dataset based on the two parent datasets
    description = (f"Cross-matched dataset between {left.info.builder_name}:{left.info.config_name} and {right.info.builder_name}:{left.info.config_name}.\nBelow are the original descriptions\n\n"
                   f"{left.info.description}\n\n{right.info.description}")
    
    # Create the new dataset
    return Dataset.from_generator(_generate_examples,
                                                   features,
                                                   cache_dir=cache_dir,
                                                   gen_kwargs={'groups':catalog_groups},
                                                   num_proc=num_proc,
                                                   keep_in_memory=keep_in_memory,
                                                   description=description)


def extract_cat_params(cat: DatasetBuilder):
    """This just grabs the ra, dec, and healpix columns from a catalogue."""
    cat = get_catalog(cat)
    subcat = pd.DataFrame(data=dict((col, cat[col].data) for col in ["ra", "dec", "healpix"]))
    return subcat


def build_master_catalog(cats: list[DatasetBuilder], names: list[str], matching_radius: float = 1.0):
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

    if len(cats) != len(names):
        raise ValueError("The number of catalogues and names must be the same.")

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
