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
# TODO: Address all TODOs and remove all explanatory comments
"""TODO: Add a description here."""


import csv
import json
import os
import datasets
from datasets.data_files import DataFilesPatternsDict
from datasets import Features, Value, Array2D, Sequence
import itertools
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
The VIPERS spectra catalog
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"

_FLOAT_FEATURES = [
    'ra',
    'dec',
    'redshift', 
    'redflag', 
    'exptime', 
    'norm', 
    'mag'
]

class VIPERS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = datasets.Version("1.1.0")

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="vipers_w1",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['./vipers_w1/healpix=*/*.h5']}),
                               description="VIPERS W1 Catalog"),
        datasets.BuilderConfig(name="vipers_w4",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['./vipers_w4/healpix=*/*.h5']}),
                               description="VIPERS W4 Catalog"),
        datasets.BuilderConfig(name="all",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['./*/healpix=*/*.h5']}),
                               description="VIPERS Full Catalog")
    ]

    DEFAULT_CONFIG_NAME = "all"  # It's not mandatory to have a default configuration. Just use one if it make sense.

    def _info(self):
        """Defines the dataset info."""
        features = datasets.Features(
            {
                "spectrum": Sequence({
                    "flux": Value(dtype="float32"),
                    "ivar": Value(dtype="float32"),
                    "lambda": Value(dtype="float32"),
                    "mask": Value(dtype="float32")
                })
            }
        )

        for key in _FLOAT_FEATURES:
            features[key] = datasets.Value("float64")

        features['object_id'] = Value(dtype="string")

        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features=features,
            homepage=_HOMEPAGE,
            license=_LICENSE,
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
        """Yeilds examples from the dataset"""
        for j, file_path in enumerate(files):
            with h5py.File(file_path, "r") as data:    
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

                    example = {
                        "spectrum": {
                            "flux": data["spectrum_flux"][i] * 1e17, # normalize
                            "ivar": 1/(data["spectrum_noise"][i] * 1e34), # normalize
                            "lambda": data["spectrum_wave"][i],
                            "mask": data["spectrum_mask"][i]
                        }
                    }

                    for key in _FLOAT_FEATURES:
                        example[key] = data[key][i].astype(np.float64)

                    # Add object id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example