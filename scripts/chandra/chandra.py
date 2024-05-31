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

_CITATION = """\
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

_DESCRIPTION = """\
Spectra from the Chandra Source Catalog. Processed from pulse height
amplitude (PHA) files modified by instrumental response files ARF and RMF.
"""

_HOMEPAGE = "https://cxc.cfa.harvard.edu/csc/"

_LICENSE = ""

_VERSION = "2.1.0"

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
            name="chandra_spectra",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["./output_data/parent*.hdf5"]}
            ),
            description="X-Ray spectral data.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "chandra_spectra"

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
            }, )
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding all boolean flags
        #for f in _BOOL_FEATURES:
        #    features[f] = Value("bool")

        features["name"] = Value("string")

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
                    object_ids = [f"{name.decode('utf-8')}_{obsid}_{obi}" for name, obsid, obi in zip(data["name"][:], data["obsid"][:], data["obi"][:])]
                    keys = object_ids

                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(object_ids)
                sorted_ids = np.array(object_ids)[sort_index]

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

                    # Add all boolean flags
                    #for f in _BOOL_FEATURES:
                    #    example[f] = not bool(data[f][i])    # if flag is 0, then no problem

                    # Add object_id
                    example["object_id"] = str(object_ids[i])

                    yield str(object_ids[i]), example


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
