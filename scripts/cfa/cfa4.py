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
from datasets import Array2D, Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict

_CITATION = """\
@ARTICLE{2012ApJS..200...12H,
    author = {{Hicken}, Malcolm and {Challis}, Peter and {Kirshner}, Robert P. and {Rest}, Armin and {Cramer}, Claire E. and {Wood-Vasey}, W. Michael and {Bakos}, Gaspar and {Berlind}, Perry and {Brown}, Warren R. and {Caldwell}, Nelson and {Calkins}, Mike and {Currie}, Thayne and {de Kleer}, Kathy and {Esquerdo}, Gil and {Everett}, Mark and {Falco}, Emilio and {Fernandez}, Jose and {Friedman}, Andrew S. and {Groner}, Ted and {Hartman}, Joel and {Holman}, Matthew J. and {Hutchins}, Robert and {Keys}, Sonia and {Kipping}, David and {Latham}, Dave and {Marion}, George H. and {Narayan}, Gautham and {Pahre}, Michael and {Pal}, Andras and {Peters}, Wayne and {Perumpilly}, Gopakumar and {Ripman}, Ben and {Sipocz}, Brigitta and {Szentgyorgyi}, Andrew and {Tang}, Sumin and {Torres}, Manuel A.~P. and {Vaz}, Amali and {Wolk}, Scott and {Zezas}, Andreas},
    title = "{CfA4: Light Curves for 94 Type Ia Supernovae}",
    journal = {\apjs},
    keywords = {supernovae: general, Astrophysics - Cosmology and Nongalactic Astrophysics},
    year = 2012,
    month = jun,
    volume = {200},
    number = {2},
    eid = {12},
    pages = {12},
    doi = {10.1088/0067-0049/200/2/12},
    archivePrefix = {arXiv},
    eprint = {1205.4493},
    primaryClass = {astro-ph.CO},
    adsurl = {https://ui.adsabs.harvard.edu/abs/2012ApJS..200...12H},
    adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = """\
Time-series dataset from the Center for Astronomy 4 Data Release.
"""

_HOMEPAGE = "https://lweb.cfa.harvard.edu/supernova/"

_LICENSE = "CC BY 4.0"

_VERSION = "0.0.1"

_STR_FEATURES = ["object_id", "obj_type"]

_FLOAT_FEATURES = [
    "ra",
    "dec",
]


class CFA4(datasets.GeneratorBasedBuilder):
    """"""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="cfa4",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./*/*.hdf5"]}),
            description="Light curves from CFA 4",
        ),
    ]

    DEFAULT_CONFIG_NAME = "cfa4"

    _bands = ["U", "B", "V", "R", "I", "r'", "i'"]

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to image datasets
        features = {
            "lightcurve": Sequence(
                feature={
                    "band": Value("string"),
                    "time": Value("float32"),
                    "mag": Value("float32"),
                    "mag_err": Value("float32"),
                }
            ),
        }
        ######################################

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")
        for f in _STR_FEATURES:
            features[f] = Value("string")

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
        """Yields examples as (key, example) tuples."""
        for file_number, file in enumerate(itertools.chain.from_iterable(files)):
            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[file_number]
                else:
                    keys = [data["object_id"][()]]

                for k in keys:
                    # Parse data
                    idxs = np.arange(0, data["mag"].shape[0])
                    band_idxs = idxs.repeat(data["mag"].shape[-1]).reshape(
                        len([bstr.decode("utf-8") for bstr in data["bands"][()]]), -1
                    )
                    example = {
                        "lightcurve": {
                            "band": np.asarray(
                                [
                                    data["bands"][()][band_number]
                                    for band_number in band_idxs.flatten().astype(
                                        "int32"
                                    )
                                ]
                            ).astype("str"),
                            "time": np.asarray(data["time"])
                            .flatten()
                            .astype("float32"),
                            "mag": np.asarray(data["mag"]).flatten().astype("float32"),
                            "mag_err": np.asarray(data["mag_err"])
                            .flatten()
                            .astype("float32"),
                        },
                    }
                    # Add remaining features
                    for f in _FLOAT_FEATURES:
                        example[f] = np.asarray(data[f]).astype("float32")
                    for f in _STR_FEATURES:
                        # Add band names shared across dataset to each sample.
                        # I can't see a better way to do this.
                        if f == "bands":
                            example[f] = np.asarray(data[f]).astype("str")
                        else:
                            example[f] = data[f][()].astype("str")

                    yield str(data["object_id"][()]), example
