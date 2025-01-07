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

import csv
import json
import os
import datasets
from datasets.data_files import DataFilesPatternsDict
import itertools
import h5py
import numpy as np

_CITATION = r"""% CITATION
@article{Adams_2022,
    doi = {10.3847/1538-4357/ac6512},
    url = {https://dx.doi.org/10.3847/1538-4357/ac6512},
    year = {2022},
    month = {may},
    publisher = {The American Astronomical Society},
    volume = {931},
    number = {1},
    pages = {16},
    author = {Dominic Adams and Vihang Mehta and Hugh Dickinson and Claudia Scarlata and Lucy Fortson and Sandor Kruk and Brooke Simmons and Chris Lintott},
    title = {Galaxy Zoo: Clump Scout: Surveying the Local Universe for Giant Star-forming Clumps},
    journal = {The Astrophysical Journal},
    abstract = {Massive, star-forming clumps are a common feature of high-redshift star-forming galaxies. How they formed, and why they are so rare at low redshift, remains unclear. In this paper we identify the largest sample yet of clumpy galaxies (7050) at low redshift using data from the citizen science project Galaxy Zoo: Clump Scout, in which volunteers classified 58,550 Sloan Digital Sky Survey (SDSS) galaxies spanning redshift 0.02 &lt; z &lt; 0.15. We apply a robust completeness correction by comparing with simulated clumps identified by the same method. Requiring that the ratio of clump to galaxy flux in the SDSS u band be greater than 8% (similar to clump definitions used by other works), we estimate the fraction of local star-forming galaxies hosting at least one clump (f clumpy) to be . We also compute the same fraction with a less stringent relative flux cut of 3% (), as the higher number count and lower statistical noise of this fraction permit finer comparison with future low-redshift clumpy galaxy studies. Our results reveal a sharp decline in f clumpy over 0 &lt; z &lt; 0.5. The minor merger rate remains roughly constant over the same span, so we suggest that minor mergers are unlikely to be the primary driver of clump formation. Instead, the rate of galaxy turbulence is a better tracer for f clumpy over 0 &lt; z &lt; 1.5 for galaxies of all masses, which supports the idea that clump formation is primarily driven by violent disk instability for all galaxy populations during this period.}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
Article data from Adams et al. 2022, DOI 10.3847/1538-4357/ac6512.

Data is collected as part of the Galaxy Zoo project, originally described in Lintott et al. 2008.

The Legacy Surveys consist of three individual and complementary projects: the Dark Energy Camera Legacy Survey (DECaLS; Proposal ID #2014B-0404; PIs: David Schlegel and Arjun Dey), the Beijing-Arizona Sky Survey (BASS; NOAO Prop. ID #2015A-0801; PIs: Zhou Xu and Xiaohui Fan), and the Mayall z-band Legacy Survey (MzLS; Prop. ID #2016A-0453; PI: Arjun Dey). DECaLS, BASS and MzLS together include data obtained, respectively, at the Blanco telescope, Cerro Tololo Inter-American Observatory, NSF’s NOIRLab; the Bok telescope, Steward Observatory, University of Arizona; and the Mayall telescope, Kitt Peak National Observatory, NOIRLab. The Legacy Surveys project is honored to be permitted to conduct astronomical research on Iolkam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation.
"""

_DESCRIPTION = r"""The GZClumps catalog is a dataset of 14596 labels of clumps in SDSS galaxies collected as part of the wider Galaxy Zoo project.
The catalog includes the following features for each galaxy: Host galaxy right ascension and declination, clump right ascension and declination, pixel position of clump in host galaxy (X and Y), and shape parameters.
"""

_HOMEPAGE = "https://astronn.readthedocs.io/en/latest/galaxy10.html"

_LICENSE = "MIT License"

_VERSION = "1.0.0"

_FLOAT_FEATURES = [
    "redshift",
    "completeness",
    "pixel_scale",
]

_BOOL_FEATURES = [
    "unusual",
]

class GZClumps(datasets.GeneratorBasedBuilder):
    """GZClumps Catalog loaded with Healpix indices (using NSIDE=16)."""

    VERSION = _VERSION
    
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="gzclumps",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["gzclumps/healpix=*/*.h5"]}
            ),
            description="GZClumps Catalog loaded with Healpix indices (using NSIDE=16).",
        ),
    ]

    DEFAULT_CONFIG_NAME = "gzclumps"

    _n_samples = 14596

    def _info(self):
        """Defines the dataset info."""
        features = datasets.Features(
            {
                "ra": datasets.Value("float32"),
                "dec": datasets.Value("float32"),
                "ra_clump": datasets.Value("float32"),
                "dec_clump": datasets.Value("float32"),
                "X": datasets.Value("int32"),
                "Y": datasets.Value("int32"),
                "shape_r": datasets.Value("float32"),
                "shape_e1": datasets.Value("float32"),
                "shape_e2": datasets.Value("float32"),
                "object_id": datasets.Value("string"),
            }
        )

        for f in _FLOAT_FEATURES:
            features[f] = datasets.Value("float32")

        for f in _BOOL_FEATURES:
            features[f] = datasets.Value("bool")

        ACKNOWLEDGEMENTS = "\n".join([f"% {line}" for line in _ACKNOWLEDGEMENTS.split("\n")])

        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features=features,
            homepage=_HOMEPAGE,
            license=_LICENSE,
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
                # Try to find the correct key for object IDs
                id_key = "object_id"  # You might need to change this to match the actual key name
                if id_key not in data:
                    raise KeyError(f"'{id_key}' not found in H5 file. Available keys: {list(data.keys())}")

                if object_ids is not None:
                    keys = object_ids[j]
                else:
                    keys = data[id_key][:]

                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["object_id"][:])
                sorted_ids = data["object_id"][:][sort_index]

                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]

                    example = {
                        "ra": data["ra"][i].astype(np.float32),
                        "dec": data["dec"][i].astype(np.float32),
                        "ra_clump": data["ra_clump"][i].astype(np.float32),
                        "dec_clump": data["dec_clump"][i].astype(np.float32),
                        "X": data["X"][i].astype(np.int32),
                        "Y": data["Y"][i].astype(np.int32),
                        "shape_r": data["shape_r"][i].astype(np.float32),
                        "shape_e1": data["shape_e1"][i].astype(np.float32),
                        "shape_e2": data["shape_e2"][i].astype(np.float32),
                    }

                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32").newbyteorder('=')

                    # Add all boolean flags
                    for f in _BOOL_FEATURES:
                        example[f] = bool(data[f][i])

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
