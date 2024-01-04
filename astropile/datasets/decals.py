import os
import numpy as np
import datasets
import glob
from datasets import Features, Value, Array2D, Sequence
from datasets.utils.logging import get_logger

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

# TODO: replace config name and urls with the correct ones
# Download URLs for different variants of the dataset
_URLS = {
    "stein_et_al_north": {'catalog': "https://users.flatironinstitute.org/~flanusse/decals_catalog_north.fits",
                          'data': "north/images_npix152*.h5"},
    "stein_et_al_south": {'catalog': "https://users.flatironinstitute.org/~flanusse/decals_catalog_south.fits",
                          'data': "south/images_npix152*.h5"}
}

# TODO: specify the features of the dataset
_FEATURES = {
    "stein_et_al_north": Features({
            'image': Sequence(feature={
                'band': Value('string'),
                'array': Array2D(shape=(152, 152), dtype='float32'),
                'psfsize': Value('float32'),
                'scale': Value('float32'),
            }),
            'z_spec': Value('float32'),
            'ebv': Value('float32'),
            'flux_g': Value('float32'),
            'flux_r': Value('float32'),
            'flux_z': Value('float32'),
        }),
    "stein_et_al_south": Features({
            'image': Sequence(feature={
                'band': Value('string'),
                'array': Array2D(shape=(152, 152), dtype='float32'),
                'psfsize': Value('float32'),
                'scale': Value('float32'),
            }),
            'z_spec': Value('float32'),
            'ebv': Value('float32'),
            'flux_g': Value('float32'),
            'flux_r': Value('float32'),
            'flux_z': Value('float32'),
        }),
}

_VERSION = "0.0.1"

logger = get_logger(__name__)

class DECALS(datasets.GeneratorBasedBuilder):
    """TODO: Short description for my new dataset."""

    VERSION = _VERSION

    # TODO: replace config name and description with the correct ones
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="stein_et_al_north", version=VERSION, 
                               description="Description of this configuration."),
        datasets.BuilderConfig(name="stein_et_al_south", version=VERSION, 
                               description="Description of this configuration."),
    ]

    # TODO: replace config name with the correct one
    DEFAULT_CONFIG_NAME = "stein_et_al_north"

    _bands = ['g', 'r', 'z']
    _pixel_scale = 0.262

    # TODO: modify/replace the following methods to fit your dataset
    def _generate_examples(self, catalog, data, keys = None):
        """ Yields examples as (key, example) tuples, and if keys is not None,
        only yields examples whose key is in keys (in the order of keys).
        """
        import h5py
        from astropy.table import Table

        # Opening the catalog
        catalog = Table.read(catalog)

        # If no keys are provided, return all the examples
        if keys is None:
            keys = catalog['inds']

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
        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=_FEATURES[self.config.name],
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

    @property
    def URLS(self):
        return _URLS[self.config.name]

    def _split_generators(self, dl_manager):
        # First, attempt to access the files locally, if unsuccessful, emit a warning and attempt to download them
        if dl_manager.manual_dir is not None:
            data_dir = {'catalog': os.path.join(dl_manager.manual_dir, self.URLS['catalog'].split('/')[-1])}
            data_dir['data'] = glob.glob(os.path.join(dl_manager.manual_dir, self.URLS['data']))
            print(os.path.join(dl_manager.manual_dir, self.URLS['data']), data_dir['data'])
        else:
            logger.error("This dataset requires downloading manually data through GLOBUS")
            raise ValueError("This dataset requires downloading manually data through GLOBUS")

        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={**data_dir}
            )
        ]

