
import itertools

import datasets
import h5py

from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict


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
_DESCRIPTION = """
SDSS-IV MaNGA IFU dataset.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"


class MaNGA(datasets.GeneratorBasedBuilder):

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="manga",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['out/healpix=*/*.hdf5']}),
                               description="SDSS MaNGA IFU log data cubes"),
    ]

    DEFAULT_CONFIG_NAME = "manga"

    _image_size = 96
    _image_filters = ['G', 'R', 'I', 'Z']
    _spectrum_size = 4563

    @classmethod
    def _info(cls):
        """ Defines features within the dataset """

        features = {}

        # add metadata features
        features['object_id'] = Value("string")
        features['ra'] = Value("float32")
        features['dec'] = Value("float32")
        features['healpix'] = Value("int16")
        features['z'] = Value("float32")
        features['spaxel_size'] = Value("float32")
        features['spaxel_size_units'] = Value("string")

        features['test'] = [Value('float32')]
        features['test2'] = Sequence({'a': Value('float32')})

        # add the spaxel features
        features['spaxels'] = [{
            "flux": [Value('float32')],
            "ivar": [Value('float32')],
            "mask": [Value('int64')],
            "lsf": [Value('float32')],
            "lamdba": [Value('float32')],
            "x": Value('int8'),
            "y": Value('int8'),
            "flux_units": Value('string'),
            "lambda_units": Value('string')
        }]

        # add the reconstructed image features
        features['images'] = [{
            'filter': Value('string'),
            'array': Array2D(shape=(cls._image_size, cls._image_size), dtype='float32'),
            'array_units': Value('string'),
            'psf': Array2D(shape=(cls._image_size, cls._image_size), dtype='float32'),
            'psf_units': Value('string'),
            'scale': Value('float32'),
            'scale_units': Value('string')
        }]

        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=Features(features),
            # Homepage of the dataset for documentation
            homepage=_HOMEPAGE,
            # License for the dataset if available
            license=_LICENSE,
            # Citation for the dataset
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager):
        if not self.config.data_files:
            raise ValueError(f"At least one data file must be specified, but got data_files={self.config.data_files}")

        data_files = dl_manager.download_and_extract(self.config.data_files)

        if isinstance(data_files, (str, list, tuple)):
            files = data_files
            if isinstance(files, str):
                files = [files]

            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]

            return [
                datasets.SplitGenerator(
                    name=datasets.Split.TRAIN, gen_kwargs={"files": files}
                )
            ]

        splits = []
        for split_name, files in data_files.items():
            if isinstance(files, str):
                files = [files]

            # Use `dl_manager.iter_files` to skip hidden files in an extracted archive
            files = [dl_manager.iter_files(file) for file in files]
            splits.append(
                datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})
            )
        return splits

    def _generate_examples(self, files, object_ids=None):
        """ Yields examples as (key, example) tuples.
        """

        for file in itertools.chain.from_iterable(files):
            with h5py.File(file, "r") as data:

                # loop over groups in hdf file
                for key in data.keys():
                    if object_ids and key not in object_ids:
                        continue

                    grp = data[key]
                    objid = grp['object_id'].asstr()[()]

                    example = {
                        'object_id': objid,
                        'ra': grp['ra'][()],
                        'dec': grp['dec'][()],
                        'healpix': grp['healpix'][()],
                        'z': grp['z'][()],
                        'spaxel_size': grp['spaxel_size'][()],
                        'spaxel_size_units': grp['spaxel_size_unit'].asstr()[()]

                    }

                    example['test'] = [1, 2, 3, 4]
                    example['test2'] = [{'a': 1}, {'a': 2}, {'a': 3}, {'a': 4}]

                    #spax_cols = ('flux', 'ivar', 'mask', 'lsf', 'lambda', 'x', 'y', 'flux_units', 'lambda_units')
                    #example['spaxels'] = [dict(zip(spax_cols, i)) for i in grp['spaxels']]

                    im_cols = ('filter', 'array', 'array_units', 'psf', 'psf_units', 'scale', 'scale_units')
                    example['images'] = [dict(zip(im_cols, i)) for i in grp['images']]

                    yield objid, example
