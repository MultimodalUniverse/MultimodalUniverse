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
import itertools

import datasets
import h5py
import numpy as np
from datasets import Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict

_CITATION = r"""% CITATION
@ARTICLE{Zhao2012,
       author = {{Zhao}, Gang and {Zhao}, Yong-Heng and {Chu}, Yao-Quan and {Jing}, Yi-Peng and {Deng}, Li-Cai},
        title = "{LAMOST spectral survey {\textemdash} An overview}",
      journal = {Research in Astronomy and Astrophysics},
         year = 2012,
        month = jul,
       volume = {12},
       number = {7},
        pages = {723-734},
          doi = {10.1088/1674-4527/12/7/002},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2012RAA....12..723Z},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
From the LAMOST website (http://www.lamost.org/):

The LAMOST (Large Sky Area Multi-Object Fiber Spectroscopic Telescope) is a 
National Major Scientific Project built by the Chinese Academy of Sciences.
Funding for the project has been provided by the National Development and Reform 
Commission. LAMOST is operated and managed by the National Astronomical 
Observatories, Chinese Academy of Sciences.

We acknowledge the use of data from the LAMOST (Large Sky Area Multi-Object 
Fiber Spectroscopic Telescope) survey. LAMOST is a National Major Scientific 
Project built by the Chinese Academy of Sciences. Funding for the project has 
been provided by the National Development and Reform Commission. LAMOST is 
operated and managed by the National Astronomical Observatories, Chinese 
Academy of Sciences.
"""

_DESCRIPTION = """\
The Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST) is a 
Chinese national scientific research facility operated by the National Astronomical 
Observatories, Chinese Academy of Sciences. LAMOST can observe about 4000 celestial 
objects simultaneously in each exposure with its 4000 optical fibers. The spectral 
resolution is R ~ 1800, and the wavelength coverage is 3700-9000 Å. This dataset 
contains optical spectra from the LAMOST survey with associated stellar parameters 
and classifications.
"""

_HOMEPAGE = "http://www.lamost.org/"

_LICENSE = "Custom - see LAMOST data policy"

_VERSION = "1.0.0"

_FLOAT_FEATURES = [
    "ra",
    "dec",
    "snrg",
    "snri",
    "snrz",
    "snrr",
    "snru",
    "teff",
    "logg",
    "feh",
    "rv",
    "teff_err",
    "logg_err",
    "feh_err",
    "rv_err"
]

_STRING_FEATURES = [
    "obsid",
    "designation",
]

_BOOL_FEATURES = [
    "restframe"
]


class LAMOST(datasets.GeneratorBasedBuilder):
    """
    Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST)
    """

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="lamost",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["./lamost/healpix=*/*.hdf5"]}
            ),
            description="LAMOST survey optical spectra.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "lamost"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to spectral dataset
        features = {
            
                    "spectrum_flux": Value(dtype="float32"),
                    "spectrum_wavelength": Value(dtype="float32")
                }
            

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding string features
        for f in _STRING_FEATURES:
            features[f] = Value("string")

        # Adding all boolean flags
        for f in _BOOL_FEATURES:
            features[f] = Value("bool")

        features["object_id"] = Value("string")

        # Format acknowledgements to have % at the beginning of each line
        ACKNOWLEDGEMENTS = "\n".join([f"% {line}" for line in _ACKNOWLEDGEMENTS.split("\n")])

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
            citation=ACKNOWLEDGEMENTS + "\n" + _CITATION,
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
                            "spectrum_flux": data['spectrum_flux'][i],
                            "spectrum_wavelength": data['spectrum_wavelength'][i]
                            }
                    
                    # Add all float features
                    for f in _FLOAT_FEATURES:
                        print(f"Adding feature {f} with value {data[f][i]}")
                        example[f] = data[f][i].astype("float32")

                    # Add all string features  
                    for f in _STRING_FEATURES:
                        example[f] = str(data[f][i])

                    # Add all boolean features
                    for f in _BOOL_FEATURES:
                        example[f] = bool(data[f][i])

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    print(f"Yielding example for object_id {example['object_id']} with features: {list(example.keys())}")

                    yield str(data["object_id"][i]), example