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
The PROVABGS catalog from Hahn, et al. (2022) is a catalog of galaxy properties derived using a state-of-the-art SED modeling of DESI
spectroscopy and photometry. More details are located here: https://github.com/changhoonhahn/provabgs
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"

_FLOAT_FEATURES = [
    'Z_HP',
    'LOG_MSTAR',
    'Z_MW',
    't_ageMW',
    'SFR',
    'ZERR',
    'TSNR2_BGS',
    'MAG_G',
    'MAG_R',
    'MAG_Z',
    'MAG_W1',
    'FIBMAG_R',
    'HPIX_64',
    'PROVABGS_Z_MAX',
    'SCHLEGEL_COLOR',
    'PROVABGS_W_ZFAIL',
    'PROVABGS_W_FIBASSIGN',
]

_BOOL_FEATURES = [
    'IS_BGS_BRIGHT',
    'IS_BGS_FAINT',
]

class PROVABGS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = datasets.Version("1.1.0")

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="provabgs",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['datafiles/healpix=*/*.h5']}),
                               description="PROVABGS catalog from Hahn, et al. (2022)"),
    ]

    DEFAULT_CONFIG_NAME = "provabgs"  # It's not mandatory to have a default configuration. Just use one if it make sense.

    def _info(self):
        """Defines the dataset info."""
        features = datasets.Features(
            {
                "ra": datasets.Value("float32"),
                "dec": datasets.Value("float32"),
                "object_id": datasets.Value("string"),
                'PROVABGS_MCMC': datasets.Array2D(shape=(100, 13), dtype="float64"),
                'PROVABGS_THETA_BF': datasets.Sequence(datasets.Value("float64")),
                'PROVABGS_LOGMSTAR': datasets.Sequence(datasets.Value("float64")),
            }
        )

        for key in _FLOAT_FEATURES:
            features[key] = datasets.Value("float64")

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
                        "ra": data["ra"][i].astype(np.float32),
                        "dec": data["dec"][i].astype(np.float32),
                        "object_id": str(data["object_id"][i]),
                        'PROVABGS_MCMC': data['PROVABGS_MCMC'][i].astype(np.float64),
                        'PROVABGS_THETA_BF': data['PROVABGS_THETA_BF'][i].astype(np.float64),
                        'PROVABGS_LOGMSTAR': data['PROVABGS_LOGMSTAR'][i].astype(np.float64),
                    }

                    for key in _FLOAT_FEATURES:
                        example[key] = data[key][i].astype(np.float64)

                    # Add object id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example