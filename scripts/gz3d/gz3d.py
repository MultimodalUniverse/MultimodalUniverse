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
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
import h5py
import numpy as np


_CITATION = """\
@article{10.1093/mnras/stab2282,
    author = {Masters, Karen L and Krawczyk, Coleman and Shamsi, Shoaib and Todd, Alexander and Finnegan, Daniel and Bershady, Matthew and Bundy, Kevin and Cherinka, Brian and Fraser-McKelvie, Amelia and Krishnarao, Dhanesh and Kruk, Sandor and Lane, Richard R and Law, David and Lintott, Chris and Merrifield, Michael and Simmons, Brooke and Weijmans, Anne-Marie and Yan, Renbin},
    title = "{Galaxy Zoo: 3D - crowdsourced bar, spiral, and foreground star masks for MaNGA target galaxies}",
    journal = {Monthly Notices of the Royal Astronomical Society},
    volume = {507},
    number = {3},
    pages = {3923-3935},
    year = {2021},
    month = {08},
    abstract = "{The challenge of consistent identification of internal structure in galaxies - in particular disc galaxy components like spiral arms, bars, and bulges – has hindered our ability to study the physical impact of such structure across large samples. In this paper we present Galaxy Zoo: 3D (GZ:3D) a crowdsourcing project built on the Zooniverse platform that we used to create spatial pixel (spaxel) maps that identify galaxy centres, foreground stars, galactic bars, and spiral arms for 29 831 galaxies that were potential targets of the MaNGA survey (Mapping Nearby Galaxies at Apache Point Observatory, part of the fourth phase of the Sloan Digital Sky Surveys or SDSS-IV), including nearly all of the 10 010 galaxies ultimately observed. Our crowdsourced visual identification of asymmetric internal structures provides valuable insight on the evolutionary role of non-axisymmetric processes that is otherwise lost when MaNGA data cubes are azimuthally averaged. We present the publicly available GZ:3D catalogue alongside validation tests and example use cases. These data may in the future provide a useful training set for automated identification of spiral arm features. As an illustration, we use the spiral masks in a sample of 825 galaxies to measure the enhancement of star formation spatially linked to spiral arms, which we measure to be a factor of three over the background disc, and how this enhancement increases with radius.}",
    issn = {0035-8711},
    doi = {10.1093/mnras/stab2282},
    url = {https://doi.org/10.1093/mnras/stab2282},
    eprint = {https://academic.oup.com/mnras/article-pdf/507/3/3923/40346078/stab2282.pdf},
}
"""

_DESCRIPTION = """
A dataset of 30k human anotated masks for MaNGA galaxies and the respective PNG of the target 
used. It is published as a Value Added Catalog as part of SDSS data release 17. Each segmentation 
is an aggregate of 15 human annotations. The associated catalog contains summaries of the respective 
classifications as collected by the Galaxy Zoo collaboration. The original data is available at
https://data.sdss.org/sas/dr17/env/MANGA_MORPHOLOGY/galaxyzoo3d/v4_0_0/
"""

_HOMEPAGE = "https://www.zooniverse.org/projects/klmasters/galaxy-zoo-3d"

_CODE = "https://github.com/CKrawczyk/GZ3D_production/"

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = "BSD-3-Clause"

_VERSION = "4.0.0"

class GZ3D(datasets.GeneratorBasedBuilder):

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="gz3d",
                               version=VERSION,
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['gz3d/healpix=*/*.hdf5']}),
                               description="SDSS MaNGA Galaxy Zoo 3D Segmentation Masks",),
    ]

    DEFAULT_CONFIG_NAME = "gz3d"

    _image_size = 525
    _classes = ['center', 'star', 'spiral', 'bar']

    @classmethod
    def _info(self):
        """ Defines the features available in this dataset.
        """
        # Starting with all features common to image datasets
        features = {
            # Metadata
            'total_classifications': Value("uint8"),
            # Image data
            'image': Array2D(shape=(self._image_size, self._image_size), dtype='uint8'),
            'segmentation': Sequence(feature={
                'class': Value('string'),
                'vote_fraction': Value('float32'),
                'array': Array2D(shape=(self._image_size, self._image_size), dtype='uint8')
            }),
        }

        features["object_id"] = Value("string")

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
        """ Yields examples as (key, example) tuples.
        """
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

                    example = {
                        'total_classifications': data["gz_total_classifications"][i].astype(np.uint8),
                        'image': data["false_color"][i].astype(np.uint8),
                        'segmentation': [{
                            'class': str(_class),
                            'array': data[_class][i].astype(np.uint8)
                        } for _class in self._classes
                    ]}

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data['object_id'][i]), example