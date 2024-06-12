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
import datasets
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
import h5py
import numpy as np

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

_VERSION = "0.0.1"

_FLOAT_FEATURES = [
    'a_g',
    'a_r',
    'a_i',
    'a_z',
    'a_y',
    'g_extendedness_value',
    'r_extendedness_value',
    'i_extendedness_value',
    'z_extendedness_value',
    'y_extendedness_value',
    # 'g_variance_value',
    # 'r_variance_value',
    # 'i_variance_value',
    # 'z_variance_value',
    # 'y_variance_value',
    'g_cmodel_mag',
    'g_cmodel_magerr',
    'r_cmodel_mag',
    'r_cmodel_magerr',
    'i_cmodel_mag',
    'i_cmodel_magerr',
    'z_cmodel_mag',
    'z_cmodel_magerr',
    'y_cmodel_mag',
    'y_cmodel_magerr',
    # 'g_cmodel_flux',
    # 'g_cmodel_fluxerr',
    # 'r_cmodel_flux',
    # 'r_cmodel_fluxerr',
    # 'i_cmodel_flux',
    # 'i_cmodel_fluxerr',
    # 'z_cmodel_flux',
    # 'z_cmodel_fluxerr',
    # 'y_cmodel_flux',
    # 'y_cmodel_fluxerr',
    'g_sdssshape_psf_shape11',
    'g_sdssshape_psf_shape22',
    'g_sdssshape_psf_shape12',
    'r_sdssshape_psf_shape11',
    'r_sdssshape_psf_shape22',
    'r_sdssshape_psf_shape12',
    'i_sdssshape_psf_shape11',
    'i_sdssshape_psf_shape22',
    'i_sdssshape_psf_shape12',
    'z_sdssshape_psf_shape11',
    'z_sdssshape_psf_shape22',
    'z_sdssshape_psf_shape12',
    'y_sdssshape_psf_shape11',
    'y_sdssshape_psf_shape22',
    'y_sdssshape_psf_shape12',
    'g_sdssshape_shape11',
    'g_sdssshape_shape22',
    'g_sdssshape_shape12',
    'r_sdssshape_shape11',
    'r_sdssshape_shape22',
    'r_sdssshape_shape12',
    'i_sdssshape_shape11',
    'i_sdssshape_shape22',
    'i_sdssshape_shape12',
    'z_sdssshape_shape11',
    'z_sdssshape_shape22',
    'z_sdssshape_shape12',
    'y_sdssshape_shape11',
    'y_sdssshape_shape22',
    'y_sdssshape_shape12'
    ]


class HSC(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="pdr3_dud_22.5", 
                               version=VERSION, 
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['pdr3_dud_22.5/healpix=*/*.hdf5']}),
                               description="Deep / Ultra Deep sample from PDR3 up to 22.5 imag."),
    ]

    DEFAULT_CONFIG_NAME = "pdr3_dud_22.5"

    _image_size = 160

    _bands = ['G', 'R', 'I', 'Z', 'Y']

    @classmethod
    def _info(self):
        """ Defines the features available in this dataset.
        """
        # Starting with all features common to image datasets
        features = {
            'image': Sequence(feature={
                'band': Value('string'),
                'array': Array2D(shape=(self._image_size, self._image_size), dtype='float32'),
                'ivar': Array2D(shape=(self._image_size, self._image_size), dtype='float32'),
                'mask': Array2D(shape=(self._image_size, self._image_size), dtype='bool'),
                'psf_fwhm': Value('float32'),
                'scale': Value('float32'),
            })
        }
        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value('float32')

        features["object_id"] = Value("string")

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
        """We handle string, list and dicts in datafiles"""
        if not self.config.data_files:
            raise ValueError(f"At least one data file must be specified, but got data_files={self.config.data_files}")
        splits = []
        for split_name, files in self.config.data_files.items():
            if isinstance(files, str):
                files = [files]
            splits.append(datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})) 
        return splits

    def _generate_examples(self, files, object_ids=None):
        """ Yields examples as (key, example) tuples.
        """
        for j, file in enumerate(files):
            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[j]
                else:
                    keys = data["object_id"]
                
                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["object_id"])
                sorted_ids = data["object_id"][:][sort_index]

                for k in keys:
                    # Extract the indices of requested ids in the catalog 
                    i = sort_index[np.searchsorted(sorted_ids, k)]
                    # Parse image data
                    example = {'image':  [{'band': data['image_band'][i][j].decode('utf-8'),
                               'array': data['image_array'][i][j],
                               'ivar': data['image_ivar'][i][j],
                               'mask': data['image_mask'][i][j],
                               'psf_fwhm': data['image_psf_fwhm'][i][j],
                               'scale': data['image_scale'][i][j]} for j, _ in enumerate( self._bands )]
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype('float32')
                    
                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data['object_id'][i]), example