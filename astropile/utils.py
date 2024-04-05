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


def _file_to_catalog(filename: str, keys: List[str]):
    with h5py.File(filename, 'r') as data:
        return Table({k: data[k] for k in keys})

def get_catalog(dset: DatasetBuilder,
                keys: List[str] = ['object_id', 'ra', 'dec', 'healpix'],
                split: str = 'train',
                num_proc: int = 1):
    """Return the catalog of a given astropile parent sample.
    
    Args:
        dset (GeneratorBasedBuilder): An AstroPile dataset builder.
        keys (List[str], optional): List of column names to include in the catalog. Defaults to ['object_id', 'ra', 'dec', 'healpix'].
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
    if num_proc > 1:
        with Pool(num_proc) as pool:
            catalogs = pool.map(partial(_file_to_catalog, keys=keys), dset.config.data_files[split])
    else:
        for filename in dset.config.data_files[split]:
            catalogs.append(_file_to_catalog(filename, keys=keys))
    return vstack(catalogs)

def cross_match_datasets(left : DatasetBuilder, 
                         right : DatasetBuilder,
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
    # Access the catalogs for both datasets
    cat_left = get_catalog(left)
    cat_left['sc'] = SkyCoord(cat_left['ra'], 
                              cat_left['dec'], unit='deg')
    
    cat_right = get_catalog(right)
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
    matched_catalog['healpix'] = matched_catalog[left.config.name+'_healpix']
    
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


