import os
from datasets import GeneratorBasedBuilder, Dataset
from astropy.table import Table, hstack
from astropy.coordinates import SkyCoord
from astropy import units as u
from typing import List
from functools import partial

def get_catalog(dset: GeneratorBasedBuilder):
    """Return the catalog of a given astropile parent sample.
    
    Args:
        dset (GeneratorBasedBuilder): An AstroPile dataset builder.

    Returns:
        astropy.table.Table: The catalog of the parent sample.
    """
    # Check that a data folder has been specified   
    assert dset.config.data_dir is not None, "The data directory must be specified in the config of the dataset."

    # Retrieving the urls for the catalog
    catalog_filename = dset.URLS['catalog'].split('/')[-1]
    return Table.read(os.path.join(dset.config.data_dir, catalog_filename))

def get_generator(dset: GeneratorBasedBuilder):
    """Return the generator of a given astropile parent sample.
    
    Args:
        dset (GeneratorBasedBuilder): An AstroPile dataset builder.

    Returns:
        generator: The generator of the parent sample.
    """
    # Check that a data folder has been specified   
    assert dset.config.data_dir is not None, "The data directory must be specified in the config of the dataset."

    # Retrieving the path for the catalog
    catalog_filename = dset.URLS['catalog'].split('/')[-1]
    data_filename = dset.URLS['data'].split('/')[-1]

    return partial(dset._generate_examples, 
                   catalog=os.path.join(dset.config.data_dir, catalog_filename),
                   data=os.path.join(dset.config.data_dir, data_filename))


def cross_match_datasets(left : GeneratorBasedBuilder, 
                         right : GeneratorBasedBuilder,
                         cache_dir : str = None,
                         keep_in_memory : bool = False,
                         matching_radius : float = 1., 
                         radec_keys_left : List[str] = ['ra', 'dec'],
                         radec_keys_right : List[str] = ['ra', 'dec'],
                         id_key_left : str = 'object_id',
                         id_key_right : str = 'object_id',
                         return_catalog_only : bool = False,):
    """ Utility function to generate a new cross-matched dataset from two AstroPile 
    datasets.

    Args:
        left (GeneratorBasedBuilder): The left dataset to be cross-matched.
        right (GeneratorBasedBuilder): The right dataset to be cross-matched.
        cache_dir (str, optional): The directory to cache the cross-matched dataset. Defaults to None.
        keep_in_memory (bool, optional): If True, the cross-matched dataset will be kept in memory. Defaults to False.
        matching_radius (float, optional): The maximum separation in arcseconds for a match to be considered. Defaults to 1.
        radec_keys_left (List[str], optional): The keys in the left dataset that correspond to right ascension and declination. Defaults to ['ra', 'dec'].
        radec_keys_right (List[str], optional): The keys in the right dataset that correspond to right ascension and declination. Defaults to ['ra', 'dec'].
        id_key_left (str, optional): The key in the left dataset that corresponds to the object identifier. Defaults to 'object_id'.
        id_key_right (str, optional): The key in the right dataset that corresponds to the object identifier. Defaults to 'object_id'.
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
    cat_left['sc'] = SkyCoord(cat_left[radec_keys_left[0]], 
                              cat_left[radec_keys_left[1]], unit='deg')
    
    cat_right = get_catalog(right)
    cat_right['sc'] = SkyCoord(cat_right[radec_keys_right[0]],
                               cat_right[radec_keys_right[1]], unit='deg')

    # Cross match the catalogs and restricting them to matches
    idx, sep2d, _ = cat_left['sc'].match_to_catalog_sky(cat_right['sc'])
    mask = sep2d < matching_radius*u.arcsec
    cat_left = cat_left[mask]
    cat_right = cat_right[idx[mask]]
    assert len(cat_left) == len(cat_right), "There was an error in the cross-matching."
    print("Number of matches: ", len(cat_left))
    matched_catalog = hstack([cat_left, cat_right])

    if return_catalog_only:
        return matched_catalog

    # Retrieve the generators for both datasets
    generators = [get_generator(left), get_generator(right)]
    keys = [cat_left[id_key_left],
            cat_right[id_key_right]]

    # Create a generator function that merges the two generators
    def _generate_examples():
        for i, examples in enumerate(zip(*[g(keys=k) for (g, k) in zip(generators, keys)])):
            example = {}
            for _, ex in examples:
                example.update(ex)
            yield example
    
    # Merging the features of both datasets
    features = left.info.features.copy()
    features.update(right.info.features)

    # Generating a description for the new dataset based on the two parent datasets
    description = (f"Cross-matched dataset between {left.info.builder_name}:{left.info.config_name} and {right.info.builder_name}:{left.info.config_name}.\nBelow are the original descriptions\n\n"
                   f"{left.info.description}\n\n{right.info.description}")
    
    # Create the new dataset
    return matched_catalog, Dataset.from_generator(_generate_examples,
                                                   features,
                                                   cache_dir=cache_dir,
                                                   keep_in_memory=keep_in_memory,
                                                   description=description)


