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

import numpy as np

import datasets
from datasets import Features, Value
from datasets.data_files import DataFilesPatternsDict
import h5py

_CITATION = """\
@ARTICLE{2023ApJS..268....9F,
       author = {{Fan}, Zhou and {Zhao}, Gang and {Wang}, Wei and {Zheng}, Jie and {Zhao}, Jingkun and {Li}, Chun and {Chen}, Yuqin and {Yuan}, Haibo and {Li}, Haining and {Tan}, Kefeng and {Song}, Yihan and {Zuo}, Fang and {Huang}, Yang and {Luo}, Ali and {Esamdin}, Ali and {Ma}, Lu and {Li}, Bin and {Song}, Nan and {Grupp}, Frank and {Zhao}, Haibin and {Ehgamberdiev}, Shuhrat A. and {Burkhonov}, Otabek A. and {Feng}, Guojie and {Bai}, Chunhai and {Zhang}, Xuan and {Niu}, Hubiao and {Khodjaev}, Alisher S. and {Khafizov}, Bakhodir M. and {Asfandiyarov}, Ildar M. and {Shaymanov}, Asadulla M. and {Karimov}, Rivkat G. and {Yuldashev}, Qudratillo and {Lu}, Hao and {Zhaori}, Getu and {Hong}, Renquan and {Hu}, Longfei and {Liu}, Yujuan and {Xu}, Zhijian},
        title = "{The Stellar Abundances and Galactic Evolution Survey (SAGES). I. General Description and the First Data Release (DR1)}",
      journal = {\apjs},
     keywords = {Galactic archaeology, Milky Way Galaxy physics, 2178, 1056, Astrophysics - Astrophysics of Galaxies, Astrophysics - Solar and Stellar Astrophysics},
         year = 2023,
        month = sep,
       volume = {268},
       number = {1},
          eid = {9},
        pages = {9},
          doi = {10.3847/1538-4365/ace04a},
archivePrefix = {arXiv},
       eprint = {2306.15611},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023ApJS..268....9F},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = """\
The Stellar Abundances and Galactic Evolution Survey (SAGES) of the northern sky is a specifically designed multiband photometric survey aiming to provide reliable stellar parameters with accuracy comparable to those from low-resolution optical spectra. 
"""

_HOMEPAGE = ""

_LICENSE = ""

_VERSION = "0.0.1"

_mapping = dict(
    ERR_U='float64',
    ERR_V='float64',
    FLAG_U='int32',
    FLAG_V='int32',
    MAG_U='float64',
    MAG_V='float64',
    OBS_TIME_U='float64',
    OBS_TIME_V='float64',
    dec='float64',
    healpix='int64',
    object_id='string',
    ra='float64',
)


class AllWISE(datasets.GeneratorBasedBuilder):
    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="dr1",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["./dr1/healpix=*/*.hdf5"]}
            ),
            description="SAGES DR1 catalog.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "dr1"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""

        features = {k: Value(dtype=v, id=None) for k, v in _mapping.items()}

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

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(itertools.chain.from_iterable(files)):
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

                    s_id = data["object_id"][i]

                    example = {k: data[k][i] for k in _mapping}

                    for k, v in example.items():
                        if isinstance(v, bytes):
                            example[k] = v.decode("utf-8")
                        if isinstance(v, float) and np.isnan(v):
                            example[k] = None

                    yield str(s_id), example

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
