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
from datasets import Features, Value, Sequence
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
Spectra dataset based on SDSS-IV.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"

# Full list of features available here:
# https://data.sdss.org/datamodel/files/SPECTRO_REDUX/specObj.html
_FLOAT_FEATURES = [
    "VDISP",
    "VDISP_ERR",
    "Z",
    "Z_ERR",
]

# Features that correspond to ugriz fluxes
_FLUX_FEATURES = [
    "SPECTROFLUX",
    "SPECTROFLUX_IVAR",
    "SPECTROSYNFLUX",
    "SPECTROSYNFLUX_IVAR",
]

_BOOL_FEATURES = [
    "ZWARNING"
]

class SDSS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="all",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["*/healpix=*/*.hdf5"]}
            ),
            description="All SDSS-IV spectra.",
        ),
        datasets.BuilderConfig(
            name="sdss",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["sdss/healpix=*/*.hdf5"]}
            ),
            description="SDSS Legacy survey spectra.",
        ),
        datasets.BuilderConfig(
            name="segue1",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["segue1/healpix=*/*.hdf5"]}
            ),
            description="SEGUE-1 spectra.",
        ),
        datasets.BuilderConfig(
            name="segue2",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["segue2/healpix=*/*.hdf5"]}
            ),
            description="SEGUE-2 spectra.",
        ),
        datasets.BuilderConfig(
            name="boss",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["boss/healpix=*/*.hdf5"]}
            ),
            description="BOSS spectra.",
        ),
        datasets.BuilderConfig(
            name="eboss",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["eboss/healpix=*/*.hdf5"]}
            ),
            description="eBOSS spectra.",
        )
    ]

    DEFAULT_CONFIG_NAME = "all"

    _flux_filters = ['U', 'G', 'R', 'I', 'Z']

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to image datasets
        features = {
            "spectrum": Sequence(feature={
                "flux": Value(dtype="float32"),
                "ivar": Value(dtype="float32"),
                "lsf_sigma":  Value(dtype="float32"),
                "lambda": Value(dtype="float32"),
                "mask": Value(dtype="bool"),
            })
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding all boolean flags
        for f in _BOOL_FEATURES:
            features[f] = Value("bool")

        # Adding all flux values from the catalog
        for f in _FLUX_FEATURES:
            for b in self._flux_filters:
                features[f"{f}_{b}"] = Value("float32")
        
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
            raise ValueError(
                f"At least one data file must be specified, but got data_files={self.config.data_files}"
            )
        splits = []
        for split_name, files in self.config.data_files.items():
            if isinstance(files, str):
                files = [files]
            splits.append(
                datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})
            )
        return splits

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(files):
            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[j]
                else:
                    keys = data["object_id"][:]

                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["object_id"][:])
                sorted_ids = data["object_id"][:][sort_index]

                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]

                    # Parse spectrum data
                    example = {
                        "spectrum": {
                            "flux": data["spectrum_flux"][i].reshape([-1,1]),
                            "ivar": data["spectrum_ivar"][i].reshape([-1,1]),
                            "lsf_sigma": data["spectrum_lsf_sigma"][i].reshape([-1,1]),
                            "lambda": data["spectrum_lambda"][i].reshape([-1,1]),
                            "mask": data["spectrum_mask"][i].reshape([-1,1]),
                        }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32").newbyteorder('=')

                    # Add all other requested features
                    for f in _FLUX_FEATURES:
                        for n, b in enumerate(self._flux_filters):
                            example[f"{f}_{b}"] = data[f"{f}"][i][n].astype("float32").newbyteorder('=')

                    # Add all boolean flags
                    for f in _BOOL_FEATURES:
                        example[f] = bool(data[f][i])

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
