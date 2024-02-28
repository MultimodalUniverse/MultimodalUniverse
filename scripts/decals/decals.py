import os
import numpy as np
import datasets
import glob
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
from datasets.utils.logging import get_logger
import itertools
import h5py
from astropy.table import Table, vstack, join

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = """\
    @InProceedings{huggingface:dataset,
    title = {A great new dataset},
    author={huggingface, Inc.
    },
    year={2020}
    }
"""

# TODO: Add description of the dataset here
# You can copy an official description
_DESCRIPTION = ""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""


_VERSION = "0.0.1"

logger = get_logger(__name__)

class DECALS(datasets.GeneratorBasedBuilder):
    """TODO: Short description for my new dataset."""

    VERSION = _VERSION

    # TODO: replace config name and description with the correct ones
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="stein_et_al_north", version=VERSION, 
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['north/images_npix152_*.hdf5']}),
                               description="Description of this configuration."),
        datasets.BuilderConfig(name="stein_et_al_south", version=VERSION, 
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['south/images_npix152_*.hdf5']}),
                               description="Description of this configuration."),
    ]

    # TODO: replace config name with the correct one
    DEFAULT_CONFIG_NAME = "stein_et_al_north"

    _bands = ['g', 'r', 'z']
    _pixel_scale = 0.262
    _image_size = 152

    # TODO: modify/replace the following methods to fit your dataset
    def _generate_examples(self, files, object_ids=None):
        """ Yields examples as (key, example) tuples, and if keys is not None,
        only yields examples whose key is in keys (in the order of keys).
        """
        # If no keys are provided, return all the examples
        if keys is None:
            keys = catalog['inds']

        # Open all the data files
        files = [h5py.File(file, 'r') for file in itertools.chain.from_iterable(files)]

        catalogs = []
        CATALOG_COLUMNS = ['inds']
        for d in files:
            catalogs.append(Table(data=[d[k][:] for k in CATALOG_COLUMNS], 
                                  names=CATALOG_COLUMNS))
            catalog = vstack(catalogs, join_type='exact')

        # Preparing an index for fast searching through the catalog
        sort_index = np.argsort(catalog['inds'])
        sorted_ids = catalog['inds'][sort_index]

        # Open all the data files
        files = [h5py.File(file, 'r') for file in data]

        # Loop over the indices and yield the requested data
        for i, id in enumerate(keys):
            # Extract the indices of requested ids in the catalog 
            idx = sort_index[np.searchsorted(sorted_ids, id)]
            row = catalog[idx]

            # Get the entry from the corresponding file
            file_idx = id // 1_000_000
            file_ind = id % 1_000_000

            example = {
                'image': [{
                        'band': b.lower(),
                        'array': files[file_idx]['images'][file_ind][k],
                        'psfsize': files[file_idx]['psfsize'][file_ind][k],
                        'scale': self._pixel_scale,
                    } for k, b in enumerate(self._bands)],
                'z_spec': row['z_spec'],
                'ebv': files[file_idx]['ebv'][file_ind],
                'flux_g': row['flux'][0],
                'flux_r': row['flux'][1],
                'flux_z': row['flux'][2],
            }

            # Checking that we are retriving the correct data
            assert (row['inds'] == keys[i]) & (files[file_idx]['inds'][file_ind] == keys[i]) , ("There was an indexing error when reading decals images spectra", (files[file_idx]['inds'][file_ind], keys[i]))

            yield str(row['inds']), example

    def _info(self):
        """ Defines the features available in this dataset.
        """
        features = Features({
            'image': Sequence(feature={
                'band': Value('string'),
                'array': Array2D(shape=(self._image_size, self._image_size), dtype='float32'),
                'psf_fwhm': Value('float32'),
                'scale': Value('float32'),
            }),
            'z_spec': Value('float32'),
            'ebv': Value('float32'),
            'flux_g': Value('float32'),
            'flux_r': Value('float32'),
            'flux_z': Value('float32'),
        })
        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=features,
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        """We handle string, list and dicts in datafiles"""
        if not self.config.data_files:
            raise ValueError(f"At least one data file must be specified, but got data_files={self.config.data_files}")
        data_files = dl_manager.download_and_extract(self.config.data_files)
        if isinstance(data_files, (str, list, tuple)):
            files = data_files
            if isinstance(files, str):
                files = [files]
            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            return [datasets.SplitGenerator(name=datasets.Split.TRAIN, gen_kwargs={"files": files})]
        splits = []
        for split_name, files in data_files.items():
            if isinstance(files, str):
                files = [files]
            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            splits.append(datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})) 
        return splits
