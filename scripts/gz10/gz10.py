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
The GZ10 catalog from Leung et al. (2018) is a dataset of 17,736 galaxies with labels from the Galaxy Zoo 2 project.
The catalog includes the following features for each galaxy: right ascension, declination, redshift, and a label from the Galaxy Zoo 2 project.
https://astronn.readthedocs.io/en/latest/galaxy10.html
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"


# TODO: Name of the dataset usually matches the script name with CamelCase instead of snake_case
class GZ10(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = datasets.Version("1.1.0")

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="gz10_with_healpix",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['datafiles/healpix=*/*.h5']}),
                               description="GZ-10 Catalog loaded with Healpix indices (using NSIDE=16). Images not included."),
        datasets.BuilderConfig(name="gz10_images",
                                version=VERSION,
                                data_files=DataFilesPatternsDict.from_patterns({'train': ['Galaxy10_DECals.h5']}),
                                description="GZ-10 Catalog loaded from one HDF5 file."),
        datasets.BuilderConfig(name="gz10_with_healpix_with_images",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['datafiles/healpix=*/*.h5']}),
                               description="GZ-10 Catalog loaded with Healpix indices (using NSIDE=16). Images included.")
    ]

    DEFAULT_CONFIG_NAME = "gz10_with_healpix"  # It's not mandatory to have a default configuration. Just use one if it make sense.

    _image_size = 256
    _n_samples = 17736

    def _info(self):
        """Defines the dataset info."""
        features = datasets.Features(
            {
                "gz10_label": datasets.Value("int32"),
                "ra": datasets.Value("float32"),
                "dec": datasets.Value("float32"),
                "object_id": datasets.Value("string"),
            }
        )

        if self.config.name == "gz10_images" or self.config.name == "gz10_with_healpix_with_images":
            features["images"] = datasets.Array3D(shape=(self._image_size, self._image_size, 3), dtype="uint8")
            features["pixel_scale"] = datasets.Value("float32")

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
                        "gz10_label": data["ans"][i].astype(np.int32),
                        "dec": data["dec"][i].astype(np.float32),
                        "ra": data["ra"][i].astype(np.float32),
                    }

                    if self.config.name == "gz10_images" or self.config.name == "gz10_with_healpix_with_images":
                        example["images"] = data["images"][i]
                        example["pixel_scale"] = data["pxscale"][i]

                    # Add object id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example