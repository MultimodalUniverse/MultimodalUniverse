import os
import numpy as np
import datasets
from datasets import Features
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
    "[config_name_to_be_replaced]": {'catalog': "...",
                                    'data':    "..."}
}

# TODO: specify the features of the dataset
_FEATURES = {
    "[config_name_to_be_replaced]": Features({}),
}

_VERSION = "0.0.1"

logger = get_logger(__name__)

class MyDataset(datasets.GeneratorBasedBuilder):
    """TODO: Short description for my new dataset."""

    VERSION = _VERSION

    # TODO: replace config name and description with the correct ones
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="[config_name_to_be_replaced]", version=VERSION, 
                               description="Description of this configuration."),
    ]

    # TODO: replace config name with the correct one
    DEFAULT_CONFIG_NAME = "[config_name_to_be_replaced]"


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
            keys = catalog['object_id']

        # Preparing an index for fast searching through the catalog
        sort_index = np.argsort(catalog['object_id'])
        sorted_ids = catalog['object_id'][sort_index]

        # Opening data file and iterating over the requested keys
        with h5py.File(data, 'r') as data:
            # Loop over the indices and yield the requested data
            for i, id in enumerate(keys):
                # Extract the indices of requested ids in the catalog 
                idx = sort_index[np.searchsorted(sorted_ids, id)]
                row = catalog[idx]
                
                example = {
                    '....': data['....'][idx],
                    '...':  row['...'],
                }

                # Checking that we are retriving the correct data
                assert (row['object_id'] == keys[i]) & (data['target_ids'][idx] == keys[i]) , ("There was an indexing error when reading desi spectra", (key, keys[i]))

                yield str(row['object_id']), example        

    # --- Nothing to change below here ---

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
            data_dir = dl_manager.manual_dir
            data_dir = {k: os.path.join(data_dir, self.URLS[k].split('/')[-1]) 
                        for k in self.URLS}
        else:
            logger.warning("We recommend downloading data manually through GLOBUS" 
                           "and specifying the manual_dir argument to pass to the dataset builder."
                           "Downloading data automatically through the dataset builder will proceed but is not recommended.")
            data_dir = dl_manager.download_and_extract(self.URLS)

        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={**data_dir}
            )
        ]

