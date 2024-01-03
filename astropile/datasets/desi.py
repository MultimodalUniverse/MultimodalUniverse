import os
import numpy as np
import datasets
from datasets import Features, Value, Array2D
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
_DESCRIPTION = """\
Spectra datset from DESI.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

# Download URLs for different variants of the dataset
# TODO: these files should be versionned
_URLS = {
    "edr_sv3": {'catalog': "https://users.flatironinstitute.org/~flanusse/desi_catalog.fits",
                'data':    "https://users.flatironinstitute.org/~flanusse/desi_sv3.hdf"}
}

_VERSION = "0.0.1"

logger = get_logger(__name__)

class DESI(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="edr_sv3", version=VERSION, 
                               description="One percent survey from the DESI Early Data Release."),
    ]

    DEFAULT_CONFIG_NAME = "edr_sv3"

    @property
    def URLS(self):
        return _URLS[self.config.name]

    def _info(self):
        """ Defines the features available in this dataset.
        """
        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=Features({
                'spectrum': Array2D(shape=(None, 2), dtype='float32'), # Stores flux and ivar
                'lambda_min': Value('float32'), # Min and max wavelength
                'lambda_max': Value('float32'),
                'resolution': Value('float32'), # Resolution of the spectrum
                'z': Value('float32'),
                'ebv': Value('float32'),
                # And so on...
            }),
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

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

    def _generate_examples(self, catalog, data, keys = None):
        """ Yields examples as (key, example) tuples.
        """
        import h5py
        from astropy.table import Table

        # Opening the catalog
        catalog = Table.read(catalog)

        # If no keys are provided, return all the examples
        if keys is None:
            keys = catalog['TARGETID']

        # Preparing an index for fast searching through the catalog
        sort_index = np.argsort(catalog['TARGETID'])
        sorted_ids = catalog['TARGETID'][sort_index]

        # Opening data file and iterating over the requested keys
        with h5py.File(data, 'r') as data:
            # Loop over the indices and yield the requested data
            for i, id in enumerate(keys):
                # Extract the indices of requested ids in the catalog 
                idx = sort_index[np.searchsorted(sorted_ids, id)]
                row = catalog[idx]
                key = row['TARGETID']

                example = {
                    'spectrum':  np.stack([data['flux'][idx],
                                                    data['ivar'][idx]], axis=1).astype('float32'),# TODO: add correct values
                    'lambda_min':  0.,  # TODO: add correct values
                    'lambda_max':  1.,  # TODO: add correct values
                    'resolution':  0.1,
                    'z':  row['Z'],
                    'ebv':  row['EBV'],
                }

                # Checking that we are retriving the correct data
                assert (key == keys[i]) & (data['target_ids'][idx] == keys[i]) , ("There was an indexing error when reading desi spectra", (key, keys[i]))

                yield str(key), example