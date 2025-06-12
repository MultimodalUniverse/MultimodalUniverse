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
import h5py
import numpy as np
from datasets import Array2D, Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@ARTICLE{2019ComAC...6....2N,
       author = {{Nelson}, Dylan and {Springel}, Volker and {Pillepich}, Annalisa and {Rodriguez-Gomez}, Vicente and {Torrey}, Paul and {Genel}, Shy and {Vogelsberger}, Mark and {Pakmor}, Ruediger and {Marinacci}, Federico and {Weinberger}, Rainer and {Kelley}, Luke and {Lovell}, Mark and {Diemer}, Benedikt and {Hernquist}, Lars},
        title = "{The IllustrisTNG simulations: public data release}",
      journal = {Computational Astrophysics and Cosmology},
     keywords = {Methods: data analysis, Methods: numerical, Galaxies: formation, Galaxies: evolution, Data management systems, Data access methods, Distributed architectures, Astrophysics - Astrophysics of Galaxies, Astrophysics - Cosmology and Nongalactic Astrophysics, Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2019,
        month = may,
       volume = {6},
       number = {1},
          eid = {2},
        pages = {2},
          doi = {10.1186/s40668-019-0028-x},
archivePrefix = {arXiv},
       eprint = {1812.05609},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2019ComAC...6....2N},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2023ApJ...946...71C,
       author = {{Costantin}, Luca and {P{\'e}rez-Gonz{\'a}lez}, Pablo G. and {Vega-Ferrero}, Jes{\'u}s and {Huertas-Company}, Marc and {Bisigello}, Laura and {Buitrago}, Fernando and {Bagley}, Micaela B. and {Cleri}, Nikko J. and {Cooper}, Michael C. and {Finkelstein}, Steven L. and {Holwerda}, Benne W. and {Kartaltepe}, Jeyhan S. and {Koekemoer}, Anton M. and {Nelson}, Dylan and {Papovich}, Casey and {Pillepich}, Annalisa and {Pirzkal}, Nor and {Tacchella}, Sandro and {Yung}, L.~Y. Aaron},
        title = "{Expectations of the Size Evolution of Massive Galaxies at 3 {\ensuremath{\leq}} z {\ensuremath{\leq}} 6 from the TNG50 Simulation: The CEERS/JWST View}",
      journal = {\apj},
     keywords = {Galaxies, Galactic and extragalactic astronomy, High-redshift galaxies, Late-type galaxies, Irregular galaxies, Galaxy classification systems, Magnetohydrodynamical simulations, Radiative transfer simulations, 573, 563, 734, 907, 864, 582, 1966, 1967, Astrophysics - Astrophysics of Galaxies},
         year = 2023,
        month = apr,
       volume = {946},
       number = {2},
          eid = {71},
        pages = {71},
          doi = {10.3847/1538-4357/acb926},
archivePrefix = {arXiv},
       eprint = {2208.00007},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023ApJ...946...71C},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% From: https://www.tng-project.org/data/docs/specifications/#sec5v
"""

_DESCRIPTION = """\
Image dataset based on TNG's JWST-CEERS mock galaxy images
"""

_HOMEPAGE = "https://www.tng-project.org/data/docs/specifications/#sec5v"

_LICENSE = ""

_VERSION = "0.0.1"

class CustomBuilderConfig(datasets.BuilderConfig):
    def __init__(self, image_size=96, 
                 bands=['f200w', 'f356w'], 
                 **kwargs):
        """Custom builder config for CEERS TNG dataset.

        Args:
            image_size: The size of the images.
            bands: A list of bands for the dataset.
            **kwargs: Keyword arguments forwarded to super.
        """
        super().__init__(**kwargs)
        self.image_size = image_size
        self.bands = bands


class TNG_CEERS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        CustomBuilderConfig(
            name="all",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["*/healpix=*/*.hdf5"]}
            ),
            description="All TNG CEERS data",
        ),
    ]

    DEFAULT_CONFIG_NAME = "all"

    _float_features = [
        'mag_auto', 
        'flux_radius', 
        'flux_auto',
        'fluxerr_auto',
        'cxx_image', 
        'cyy_image', 
        'cxy_image'
    ]
    
    def _info(self):
        """Defines the features available in this dataset."""

        # Starting with all features common to image datasets
        features = {
            "image": Sequence(
                feature={
                    "band": Value("string"),
                    "flux": Array2D(
                        shape=(self.config.image_size, self.config.image_size), dtype="float32"
                    ),
                    "ivar": Array2D(
                        shape=(self.config.image_size, self.config.image_size), dtype="float32"
                    ),
                    "mask": Array2D(
                        shape=(self.config.image_size, self.config.image_size), dtype="bool"
                    ),
                    "psf_fwhm": Value("float32"),
                    "scale": Value("float32"),
                }
            )
        }
        # Adding all values from the catalog
        for f in self._float_features:
            features[f] = Value("float32")

        features["object_id"] = Value("string")

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
            print(f"Processing file: {file}")
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
                    
                    # Check if the found object_id matches the requested one
                    if data["object_id"][i] != k:
                        print(f"Warning: Object {k} not found in this chunk. Skipping.")
                        continue

                    # Parse image data
                    example = {
                        "image": [
                            {
                                "band": data["image_band"][i][j].decode("utf-8"),
                                "flux": data["image_flux"][i][j],
                                "ivar": data["image_ivar"][i][j],
                                "mask": data["image_mask"][i][j].astype(bool),
                                "psf_fwhm": data["image_psf_fwhm"][i][j],
                                "scale": data["image_scale"][i][j],
                            }
                            for j in range(len(data["image_band"][i]))
                        ]
                    }

                    # Add all other requested features
                    for f in self._float_features:
                        try:
                            value = data[f][i]
                            example[f] = float(value) if np.isscalar(value) else 0.0
                        except KeyError:
                            ######## print(f"Warning: Feature '{f}' not found in the dataset.")
                            example[f] = 0.0

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
