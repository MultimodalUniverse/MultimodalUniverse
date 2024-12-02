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

# Basic numeric and plotting imports
# Astropile adaptation for Chandra data by Rafael Martinez-Galarza

import datasets
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
import itertools
import h5py
import numpy as np

_CITATION = """% CITATION
@ARTICLE{2010ApJS..189...37E,
       author = {{Evans}, Ian N. and {Primini}, Francis A. and {Glotfelty}, Kenny J. and {Anderson}, Craig S. and {Bonaventura}, Nina R. and {Chen}, Judy C. and {Davis}, John E. and {Doe}, Stephen M. and {Evans}, Janet D. and {Fabbiano}, Giuseppina and {Galle}, Elizabeth C. and {Gibbs}, Danny G., II and {Grier}, John D. and {Hain}, Roger M. and {Hall}, Diane M. and {Harbo}, Peter N. and {He}, Xiangqun Helen and {Houck}, John C. and {Karovska}, Margarita and {Kashyap}, Vinay L. and {Lauer}, Jennifer and {McCollough}, Michael L. and {McDowell}, Jonathan C. and {Miller}, Joseph B. and {Mitschang}, Arik W. and {Morgan}, Douglas L. and {Mossman}, Amy E. and {Nichols}, Joy S. and {Nowak}, Michael A. and {Plummer}, David A. and {Refsdal}, Brian L. and {Rots}, Arnold H. and {Siemiginowska}, Aneta and {Sundheim}, Beth A. and {Tibbetts}, Michael S. and {Van Stone}, David W. and {Winkelman}, Sherry L. and {Zografou}, Panagoula},
        title = "{The Chandra Source Catalog}",
      journal = {\apjs},
     keywords = {catalogs, X-rays: general, Astrophysics - High Energy Astrophysical Phenomena, Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2010,
        month = jul,
       volume = {189},
       number = {1},
        pages = {37-82},
          doi = {10.1088/0067-0049/189/1/37},
archivePrefix = {arXiv},
       eprint = {1005.4665},
 primaryClass = {astro-ph.HE},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2010ApJS..189...37E},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
From https://cxc.cfa.harvard.edu/csc/cite.html :

Users are kindly requested to acknowledge their use of the Chandra Source Catalog in any resulting publications.

This will help us greatly to keep track of catalog usage, information that is essential for providing full accountability of our work and services, as well as for planning future services.

The following language is suggested:

This research has made use of data obtained from the Chandra Source Catalog, provided by the Chandra X-ray Center (CXC) as part of the Chandra Data Archive.
"""

_DESCRIPTION = """\
Spectra from the Chandra Source Catalog. Processed from pulse height
amplitude (PHA) files modified by instrumental response files ARF and RMF.
"""

_HOMEPAGE = "https://cxc.cfa.harvard.edu/csc/"

_LICENSE = ""

_VERSION = "1.0.0"

_FLOAT_FEATURES = [
    "flux_aper_b",
    "flux_bb_aper_b",
    "flux_significance_b",
    "hard_hm",
    "hard_hs",
    "hard_ms",
    "var_index_b",
    "var_prob_b",
]

class CHANDRA(datasets.GeneratorBasedBuilder):
    """Chandra Source Catalog 2.1 dataset for X-ray spectral data."""

    VERSION = _VERSION
    
    
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="spectra",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ['spectra/healpix=*/*.hdf5']}
            ),
            description="X-Ray spectral data.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "spectra"

    #_spectrum_length = 7781

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to image datasets
        features = {
            "spectrum": Sequence({
                "ene_center_bin": Value(dtype="float32"),
                "ene_high_bin": Value(dtype="float32"),
                "ene_low_bin":  Value(dtype="float32"),
                "flux": Value(dtype="float32"),
                "flux_error": Value(dtype="float32"),
            })
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        features["name"] = Value("string")

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

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
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
                    
                    # Parse spectrum data
                    example = {
                        "spectrum": 
                            {
                                "ene_center_bin": data['spectrum_ene'][i], 
                                "ene_high_bin": data['spectrum_ene_hi'][i],
                                "ene_low_bin": data['spectrum_ene_lo'][i],
                                "flux": data['spectrum_flux'][i],
                                "flux_error": data['spectrum_flux_err'][i],
                            }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    assert data['object_id'][i] == k

                    # Add object_id
                    example["object_id"] = k

                    yield k, example



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
