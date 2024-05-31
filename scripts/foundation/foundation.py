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
import os

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = """
@ARTICLE{2019ApJ...881...19J,
       author = {{Jones}, D.~O. and {Scolnic}, D.~M. and {Foley}, R.~J. and {Rest}, A. and {Kessler}, R. and {Challis}, P.~M. and {Chambers}, K.~C. and {Coulter}, D.~A. and {Dettman}, K.~G. and {Foley}, M.~M. and {Huber}, M.~E. and {Jha}, S.~W. and {Johnson}, E. and {Kilpatrick}, C.~D. and {Kirshner}, R.~P. and {Manuel}, J. and {Narayan}, G. and {Pan}, Y. -C. and {Riess}, A.~G. and {Schultz}, A.~S.~B. and {Siebert}, M.~R. and {Berger}, E. and {Chornock}, R. and {Flewelling}, H. and {Magnier}, E.~A. and {Smartt}, S.~J. and {Smith}, K.~W. and {Wainscoat}, R.~J. and {Waters}, C. and {Willman}, M.},
        title = "{The Foundation Supernova Survey: Measuring Cosmological Parameters with Supernovae from a Single Telescope}",
      journal = {\apj},
     keywords = {cosmology: observations, dark energy, supernovae: general, Astrophysics - Cosmology and Nongalactic Astrophysics},
         year = 2019,
        month = aug,
       volume = {881},
       number = {1},
          eid = {19},
        pages = {19},
          doi = {10.3847/1538-4357/ab2bec},
archivePrefix = {arXiv},
       eprint = {1811.09286},
 primaryClass = {astro-ph.CO},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2019ApJ...881...19J},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

"""


# You can copy an official description
_DESCRIPTION = """\
Time-series dataset from Foundation Data Release 1 (Foundation DR1).

Data Citations:

Foley et al. (2018) - https://ui.adsabs.harvard.edu/abs/2018MNRAS.475..193F

Jones et al. (2019) - https://ui.adsabs.harvard.edu/abs/2019ApJ...881...19J
"""


_HOMEPAGE = "https://github.com/djones1040/Foundation_DR1/tree/master"

_LICENSE = "https://cds.unistra.fr/vizier-org/licences_vizier.html"

_VERSION = "0.0.1"

_STR_FEATURES = [
    "object_id",
    "obj_type"
]

_FLOAT_FEATURES = [
    "ra", 
    "dec", 
    "redshift",
    "host_log_mass"
]


class FoundationDR1(datasets.GeneratorBasedBuilder):
    """"""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="foundation_dr1",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./healpix=*/*.hdf5"]}), # This seems fairly inflexible. Probably a massive failure point.
            description="Light curves from Foundation DR1",
        ),
    ]

    DEFAULT_CONFIG_NAME = "foundation_dr1"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to light curve datasets
        features = {
            'lightcurve': Sequence(feature={
                'band': Value('string'),
                'flux': Value('float32'),
                'flux_err': Value('float32'),
                'time': Value('float32'),
            }),
        }

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
            raise ValueError(f"At least one data file must be specified, but got data_files={self.config.data_files}")
        splits = []
        for split_name, files in self.config.data_files.items():
            if isinstance(files, str):
                files = [files]
            splits.append(datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})) 
        return splits

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        if object_ids is not None:
            files = [f for f in files if os.path.split(f)[-1][:-5] in object_ids]
            # Filter files by object_id
        for file in files:
            with h5py.File(file, "r") as data:
                # Parse data
                idxs = np.arange(0, data["flux"].shape[0])
                band_idxs = idxs.repeat(data["flux"].shape[-1]).reshape(
                    len(data["bands"][()].decode('utf-8').split(",")), -1
                )
                bands = data["bands"][()].decode('utf-8').split(",")
                example = {
                    "lightcurve": {
                        "band": np.asarray([bands[band_number] for band_number in band_idxs.flatten().astype("int32")]).astype("str"),
                        "time": np.asarray(data["time"]).flatten().astype("float32"),
                        "flux": np.asarray(data["flux"]).flatten().astype("float32"),
                        "flux_err": np.asarray(data["flux_err"]).flatten().astype("float32"),
                    }
                }
                    
                # Add remaining features
                for f in _FLOAT_FEATURES:
                    example[f] = np.asarray(data[f]).astype("float32")
                for f in _STR_FEATURES:
                    example[f] = data[f][()].decode('utf-8')

                yield str(data["object_id"][()]), example