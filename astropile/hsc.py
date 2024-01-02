# Copyright 2020 The HuggingFace Datasets Authors and the current dataset script contributor.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main dataset script to generate different versions of the AstroPile by
cross-matching different parent samples."""

import os
import numpy as np
import datasets
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
_DESCRIPTION = """\
Image dataset based on HSC SSP PRD3.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

# Download URLs for different variants of the dataset
# TODO: these files should be versionned 
_URLS = {
    "pdr3_dud_22.5": {'catalog': "https://users.flatironinstitute.org/~flanusse/pdr3_dud_22.5.fits",
                      'data':    "https://users.flatironinstitute.org/~flanusse/cutouts_pdr3_dud_rev_coadd.hdf"}
}

_VERSION = "0.0.1"

logger = get_logger(__name__)

class HSCReader:
    """
    A reader for HSC SSP cutouts data prepared using AstroPile scripts.
    """
    _bands = ['G', 'R', 'I', 'Z', 'Y']
    _image_size = 144
    _pixel_scale = 0.168
    

    def __init__(self, 
                 catalog_path: str,
                    data_path: str):
        from astropy.table import Table
        
        self._catalog = Table.read(catalog_path)
        self._data_path = data_path 

    @classmethod
    def urls():
        return _URLS

    @classmethod
    def features(cls):
        features = {
            'image': Sequence(feature={
                'band': Value('string'),
                'array': Array2D(shape=(cls._image_size, cls._image_size), dtype='float32'),
                'psf_shape11': Value('float32'),
                'psf_shape12': Value('float32'),
                'psf_shape22': Value('float32'),
                'scale': Value('float32'),
            })
        }
        for band in cls._bands:
            band = band.lower()
            features[f'a_{band}'] = Value('float32')
            features[f'mag_{band}'] = Value('float32')
        return Features(features)

    @property
    def catalog(self):
        return self._catalog

    def get_examples(self, keys = None):
        import h5py

        # If no keys are provided, return all the examples
        if keys is None:
            keys = self._catalog['object_id']

        # Preparing an index for fast searching through the catalog
        sort_index = np.argsort(self._catalog['object_id'])
        sorted_ids = self._catalog['object_id'][sort_index]

        # count how many times we run into problems with the images
        n_problems = 0

        with h5py.File(self._data_path, 'r') as data:
            # Loop over the indices and yield the requested data
            for i, id in enumerate(keys):
                # Extract the indices of requested ids in the catalog 
                idx = sort_index[np.searchsorted(sorted_ids, id)]
                row = self._catalog[idx]
                key = str(row['object_id'])
                hdu = data[key]

                # Get the smallest shape among all images
                s_x = min([hdu[f'HSC-{band}']['HDU0']['DATA'].shape[0] for band in self._bands])
                s_y = min([hdu[f'HSC-{band}']['HDU0']['DATA'].shape[1] for band in self._bands])

                # Raise a warning if one of the images has a different shape than 'smallest_shape'
                for band in self._bands:
                    if hdu[f'HSC-{band}']['HDU0']['DATA'].shape != (s_x, s_y):
                        logger.warning(f"The image for object {id} has a different shape depending on the band. It's the {n_problems+1}th time this happens.")
                        n_problems += 1
                        break

                # Crop the images to the smallest shape
                image = np.stack([
                    hdu[f'HSC-{band}']['HDU0']['DATA'][:s_x, :s_y].astype(np.float32)
                    for band in self._bands
                ], axis=0)
                
                # Cutout the center of the image to desired size
                s = image.shape
                center_x = s[1] // 2
                start_x = center_x - self._image_size // 2
                center_y = s[2] // 2
                start_y = center_y - self._image_size // 2
                image = image[:, 
                            start_x:start_x+self._image_size, 
                            start_y:start_y+self._image_size]
                assert image.shape == (5, self._image_size, self._image_size), ("There was an error in reshaping the image to desired size", image.shape, s )

                # Initialize the example with image data
                example = {
                    'image': [{
                        'band': b.lower(),
                        'array': image[k],
                        'psf_shape11': row[f'{b.lower()}_sdssshape_psf_shape11'],
                        'psf_shape12': row[f'{b.lower()}_sdssshape_psf_shape12'],
                        'psf_shape22': row[f'{b.lower()}_sdssshape_psf_shape22'],
                        'scale': self._pixel_scale,
                    } for k, b in enumerate(self._bands)],
                }

                # Add additional catalog information
                for band in self._bands:
                    band = band.lower()
                    example[f'a_{band}'] = row[f'a_{band}']
                    example[f'mag_{band}'] = row[f'{band}_cmodel_mag']
                    # and so on...

                # Checking that we are retriving the correct data
                assert (row['object_id'] == keys[i]), ("There was an indexing error when reading the hsc cutouts", (row['object_id'], ids[i]))

                yield f'hsc_{keys[i]}', example


class HSC(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="pdr3_dud_22.5", version=VERSION, 
                               description="Deep / Ultra Deep sample from PDR3 up to 22.5 imag."),
    ]

    DEFAULT_CONFIG_NAME = "pdr3_dud_22.5"

    def _info(self):
        """ Defines the features available in this dataset.
        """
        return datasets.DatasetInfo(
            # This is the description that will appear on the datasets page.
            description=_DESCRIPTION,
            # This defines the different columns of the dataset and their types
            features=HSCReader.features(),
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
            data_dir = {k: os.path.join(data_dir, _URLS[self.config.name][k].split('/')[-1]) 
                        for k in _URLS[self.config.name]}
        else:
            logger.warning("We recommend downloading data manually through GLOBUS" 
                           "and specifying the manual_dir argument to pass to the dataset builder."
                           "Downloading data automatically through the dataset builder will proceed but is not recommended.")
            data_dir = dl_manager.download_and_extract(_URLS[self.config.name])

        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={**data_dir}
            )
        ]

    def _generate_examples(self, catalog, data):
        """ Yields examples as (key, example) tuples.
        """
        reader = HSCReader(catalog, data)
        for key, example in reader.get_examples():
            yield key, example