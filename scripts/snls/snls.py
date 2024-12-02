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
_CITATION = r"""% CITATION
@ARTICLE{2010A&A...523A...7G,
       author = {{Guy}, J. and {Sullivan}, M. and {Conley}, A. and {Regnault}, N. and {Astier}, P. and {Balland}, C. and {Basa}, S. and {Carlberg}, R.~G. and {Fouchez}, D. and {Hardin}, D. and {Hook}, I.~M. and {Howell}, D.~A. and {Pain}, R. and {Palanque-Delabrouille}, N. and {Perrett}, K.~M. and {Pritchet}, C.~J. and {Rich}, J. and {Ruhlmann-Kleider}, V. and {Balam}, D. and {Baumont}, S. and {Ellis}, R.~S. and {Fabbro}, S. and {Fakhouri}, H.~K. and {Fourmanoit}, N. and {Gonz{\'a}lez-Gait{\'a}n}, S. and {Graham}, M.~L. and {Hsiao}, E. and {Kronborg}, T. and {Lidman}, C. and {Mourao}, A.~M. and {Perlmutter}, S. and {Ripoche}, P. and {Suzuki}, N. and {Walker}, E.~S.},
        title = "{The Supernova Legacy Survey 3-year sample: Type Ia supernovae photometric distances and cosmological constraints}",
      journal = {\aap},
     keywords = {supernovae: general, cosmology: observations, Astrophysics - Cosmology and Nongalactic Astrophysics},
         year = 2010,
        month = nov,
       volume = {523},
          eid = {A7},
        pages = {A7},
          doi = {10.1051/0004-6361/201014468},
archivePrefix = {arXiv},
       eprint = {1010.4743},
 primaryClass = {astro-ph.CO},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2010A&A...523A...7G},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
The SNLS is an International Collaboration of physicists and astronomers from various institutions in Canada, EU and US.

Institution/group representatives (Collaboration Board) are: P. Astier (IN2P3/LPNHE, Fr), S. Basa (INSU/LAM, Fr), R. Carlberg (U. Toronto, Ca), I. Hook (U. Oxford, UK), R. Pain (CNRS/IN2P3, Fr; Chair), S. Perlmutter (LBNL, US), C. Pritchet (U. Victoria, Ca) and J. Rich (CEA/DAPNIA, Fr).

Irfu/SPP (Saclay), IN2P3/LPNHE (Jussieu), INSU/LAM (Marseille), IN2P3/CPPM (Marseille), University of Toronto (Canada), University of Victoria (Canada)
"""

# You can copy an official description
_DESCRIPTION = """\
Time-series dataset from the Supernova Legacy Survey (SNLS).

Data Citations:

Guy et al. (2010)
"""

_HOMEPAGE = "https://www.aanda.org/articles/aa/full_html/2010/15/aa14468-10/aa14468-10.html"

_LICENSE = "GNU General Public License v3.0"

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


class SNLS(datasets.GeneratorBasedBuilder):
    """"""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="snls",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./healpix=*/*.hdf5"]}), # This seems fairly inflexible. Probably a massive failure point.
            description="Light curves from SNLS",
        ),
    ]

    DEFAULT_CONFIG_NAME = "snls"

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