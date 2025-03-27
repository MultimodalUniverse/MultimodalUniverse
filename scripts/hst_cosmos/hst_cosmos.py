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

from build_parent_sample import NUMPIX

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION

# COSMOS Survey Paper
@article{Koekemoer_2007,
   title={The COSMOS Survey:
                    Hubble Space Telescope
                    Advanced Camera for Surveys Observations and Data Processing},
   volume={172},
   ISSN={1538-4365},
   url={http://dx.doi.org/10.1086/520086},
   DOI={10.1086/520086},
   number={1},
   journal={The Astrophysical Journal Supplement Series},
   publisher={American Astronomical Society},
   author={Koekemoer, A. M. and Aussel, H. and Calzetti, D. and Capak, P. and Giavalisco, M. and Kneib, J.‐P. and Leauthaud, A. and Le Fevre, O. and McCracken, H. J. and Massey, R. and Mobasher, B. and Rhodes, J. and Scoville, N. and Shopbell, P. L.},
   year={2007},
   month=sep, pages={196–202} }

# Galaxy Zoo Hubble
@article{Willett_2016,
   title={Galaxy Zoo: morphological classifications for 120 000 galaxies inHSTlegacy imaging},
   volume={464},
   ISSN={1365-2966},
   url={http://dx.doi.org/10.1093/mnras/stw2568},
   DOI={10.1093/mnras/stw2568},
   number={4},
   journal={Monthly Notices of the Royal Astronomical Society},
   publisher={Oxford University Press (OUP)},
   author={Willett, Kyle W. and Galloway, Melanie A. and Bamford, Steven P. and Lintott, Chris J. and Masters, Karen L. and Scarlata, Claudia and Simmons, B. D. and Beck, Melanie and Cardamone, Carolin N. and Cheung, Edmond and Edmondson, Edward M. and Fortson, Lucy F. and Griffith, Roger L. and Häußler, Boris and Han, Anna and Hart, Ross and Melvin, Thomas and Parrish, Michael and Schawinski, Kevin and Smethurst, R. J. and Smith, Arfon M.},
   year={2016},
   month=oct, pages={4176–4203} }

"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% 

#TODO!!

"""

_DESCRIPTION = """\
Image dataset based on HST COSMOS
"""

_HOMEPAGE = "TODO!"

_LICENSE = "TODO!"

_VERSION = "1.1.0"

class CustomBuilderConfig(datasets.BuilderConfig):
    def __init__(self, image_size=NUMPIX, 
                 bands=['f814w'], 
                 **kwargs):
        """Custom builder config for HST Cosmos dataset.

        Args:
            image_size: The size of the images.
            bands: A list of bands for the dataset.
            **kwargs: Keyword arguments forwarded to super.
        """
        super().__init__(**kwargs)
        self.image_size = image_size
        self.bands = bands


class HST_COSMOS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        CustomBuilderConfig(
            name="hst-cosmos",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["COSMOS/healpix=*/*.hdf5"]}
            ),
            description="HST-COSMOS",
        )
    ]

    DEFAULT_CONFIG_NAME = "all"

    # these are keys in the original catalog that got dumped in the .h5 file
    _float_features = ['FLUX_BEST_HI', 'FLUX_RADIUS_HI', 'MAG_BEST_HI', 'KRON_RADIUS_HI']

    
    def _info(self):
        """Defines the features available in this dataset."""

        # Starting with all features common to image datasets
        features = {
            "image": Sequence(
                # TODO: make sure all of these are tracked in build_parent_sample
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
        """Yields examples as (key, example) tuples.
        
        If objects_ids=None, uses all object_ids found in the file
        """
        for j, file in enumerate(files):
            print(f"Processing file: {file}")
            with h5py.File(file, "r") as data:

                # user can provide object IDs to look for in this file
                if object_ids is not None:
                    keys = object_ids[j]

                # by default: loops through all object IDs in the file
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
                                "band": data["image_band"][i].decode("utf-8"),
                                "flux": data["image_flux"][i],
                                "ivar": data["image_ivar"][i],
                                "mask": data["image_mask"][i].astype(bool), 
                                "scale": data["pixel_scale"][i],
                            }
                        ] # list of length 1 b/c one filter
                    }

                    # Add all other requested features
                    for f in self._float_features:
                        try:
                            value = data[f][i]
                            example[f] = float(value) if np.isscalar(value) else 0.0
                        except KeyError:
                            print(f"Warning: Feature '{f}' not found in the dataset.")
                            example[f] = 0.0

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield example["object_id"], example
