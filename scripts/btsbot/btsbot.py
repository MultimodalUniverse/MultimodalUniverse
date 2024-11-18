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
import itertools
import h5py
import numpy as np

_CITATION = r"""% CITATION
@misc{rehemtulla2024zwicky,
      title={The Zwicky Transient Facility Bright Transient Survey. III. $\texttt{BTSbot}$: Automated Identification and Follow-up of Bright Transients with Deep Learning}, 
      author={Nabeel Rehemtulla and Adam A. Miller and Theophile Jegou Du Laz and Michael W. Coughlin and Christoffer Fremling and Daniel A. Perley and Yu-Jing Qin and Jesper Sollerman and Ashish A. Mahabal and Russ R. Laher and Reed Riddle and Ben Rusholme and Shrinivas R. Kulkarni},
      year={2024},
      eprint={2401.15167},
      archivePrefix={arXiv},
      primaryClass={astro-ph.IM}
}

@ARTICLE{Rehemtulla2024BTWBot,
       author = {{Rehemtulla}, Nabeel and {Miller}, Adam A. and {Jegou Du Laz}, Theophile and {Coughlin}, Michael W. and {Fremling}, Christoffer and {Perley}, Daniel A. and {Qin}, Yu-Jing and {Sollerman}, Jesper and {Mahabal}, Ashish A. and {Laher}, Russ R. and {Riddle}, Reed and {Rusholme}, Ben and {Kulkarni}, Shrinivas R.},
        title = "{The Zwicky Transient Facility Bright Transient Survey. III. BTSbot: Automated Identification and Follow-up of Bright Transients with Deep Learning}",
      journal = {\apj},
     keywords = {Time domain astronomy, Sky surveys, Supernovae, Convolutional neural networks, 2109, 1464, 1668, 1938, Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2024,
        month = sep,
       volume = {972},
       number = {1},
          eid = {7},
        pages = {7},
          doi = {10.3847/1538-4357/ad5666},
archivePrefix = {arXiv},
       eprint = {2401.15167},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2024ApJ...972....7R},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% Based on the Acknowledgements in Rehemtulla et al. (2024). We suggest including a variant of the following in your acknowledgements:
A great number of people have contributed to BTS and BTS scanning over the years. We thank the following people who have saved 10 or more sources to internal BTS catalogs on Fritz as of October 2023: Ivan Altunin, Raphael Baer-Way, Pallas A. Beddow, Ofek Bengiat, Joshua S. Bloom, Ola Bochenek, Emma Born, Kate Bostow, Victoria Mei Brendel, Rachel Bruch, Vidhi Chander, Matthew Chu, Elma Chuang, Aishwarya Dahiwale, Asia deGraw, Dmitry Duev, Kingsley Ehrich, Eli Gendreau-Distler, Nachiket Girish, Xander Hall, KRyan Hinds, Ido Irani, Cooper Jacobus, Connor Jennings, Joel Johansson, Snehaa Ganesh Kumar, Michael May, William Meynardie, Shaunak Modak, Kishore Patra, Neil Pichay, Sophia Risin, Yashvi Sharma, Gabrielle Stewart, Nora Linn Strotjohann, James Sunseri, Edgar Vidal, Jacob Wise, Abel Yagubyan, Yoomee Zeng, and Erez A. Zimmerman.

We also thank Jakob Nordin for discussions relating to AMPEL.

The material contained in this document is based upon work supported by a National Aeronautics and Space Administration (NASA) grant or cooperative agreement. Any opinions, findings, conclusions, or recommendations expressed in this material are those of the author and do not necessarily reflect the views of NASA. This work was supported through a NASA grant awarded to the Illinois/NASA Space Grant Consortium. This research was supported in part through the computational resources and staff contributions provided for the Quest high performance computing facility at Northwestern University which is jointly supported by the Office of the Provost, the Office for Research, and Northwestern University Information Technology.

Based on observations obtained with the Samuel Oschin Telescope 48-inch and the 60-inch Telescope at the Palomar Observatory as part of the Zwicky Transient Facility project. ZTF is supported by the National Science Foundation under Grants No. AST-1440341 and AST-2034437 and a collaboration including current partners Caltech, IPAC, the Oskar Klein Center at Stockholm University, the University of Maryland, University of California, Berkeley , the University of Wisconsin at Milwaukee, University of Warwick, Ruhr University, Cornell University, Northwestern University and Drexel University. Operations are conducted by COO, IPAC, and UW.
"""

_DESCRIPTION = """\
This is the production version of the BTSbot training set, limited to public (programid=1) ZTF alerts.
Original codebase: https://github.com/nabeelre/BTSbot
"""

_HOMEPAGE = "https://zenodo.org/records/10839691"

_LICENSE = "CC BY 4.0"

_VERSION = "10.0.0"

_FLOAT_FEATURES = [
    'jd',
    'diffmaglim',
    'ra',
    'dec',
    'magpsf',
    'sigmapsf',
    'chipsf',
    'magap',
    'sigmagap',
    'distnr',
    'magnr',
    'chinr',
    'sharpnr',
    'sky',
    'magdiff',
    'fwhm',
    'classtar',
    'mindtoedge',
    'seeratio',
    'magapbig',
    'sigmagapbig',
    'sgmag1',
    'srmag1',
    'simag1',
    'szmag1',
    'sgscore1',
    'distpsnr1',
    'jdstarthist',
    'scorr',
    'sgmag2',
    'srmag2',
    'simag2',
    'szmag2',
    'sgscore2',
    'distpsnr2',
    'sgmag3',
    'srmag3',
    'simag3',
    'szmag3',
    'sgscore3',
    'distpsnr3',
    'jdstartref',
    'dsnrms',
    'ssnrms',
    'magzpsci',
    'magzpsciunc',
    'magzpscirms',
    'clrcoeff',
    'clrcounc',
    'neargaia',
    'neargaiabright',
    'maggaia',
    'maggaiabright',
    'exptime',
    'drb',
    'acai_h',
    'acai_v',
    'acai_o',
    'acai_n',
    'acai_b',
    'new_drb',
    'peakmag',
    'maxmag',
    'peakmag_so_far',
    'maxmag_so_far',
    'age',
    'days_since_peak',
    'days_to_peak',
]

_INT_FEATURES = [
    'label',
    'fid',
    'programid',
    'object_id',
    'field',
    'nneg',
    'nbad',
    'ndethist',
    'ncovhist',
    'nmtchps',
    'nnotdet',
    'N',
    'healpix',
]

_BOOL_FEATURES = [
    'isdiffpos',
    'is_SN',
    'near_threshold',
    'is_rise',
]

_STRING_FEATURES = [
    'OBJECT_ID_',
    'source_set',
    'split',
]


class BTSbot(datasets.GeneratorBasedBuilder):
    """BTSbot training set"""

    VERSION = _VERSION
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="BTSbot", 
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({
                'train': ['./data/healpix=*/train_001-of-001.hdf5'],
                'val': ['./data/healpix=*/val_001-of-001.hdf5'],
                'test': ['./data/healpix=*/test_001-of-001.hdf5'],
                }),
            description="BTSbot dataset with train, val, and test splits."
            ),
    ]

    DEFAULT_CONFIG_NAME = "BTSbot"

    _image_size = 63
    _views = ['science', 'reference', 'difference']

    @classmethod
    def _info(self):
        """ Defines the features available in this dataset.
        """
        # Starting with all features common to image datasets
        features = {
            'image': Sequence(feature={
                'band': Value('string'),
                'view': Value('string'),
                'array': Array2D(shape=(self._image_size, self._image_size), dtype='float32'),
                'scale': Value('float32'),
            })
        }
        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value('float32')
        for f in _INT_FEATURES:
            # NOTE: includes object_id
            features[f] = Value('int64')
        for f in _BOOL_FEATURES:
            features[f] = Value('bool')
        for f in _STRING_FEATURES:
            features[f] = Value('string')

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
                    # Parse image data
                    example = {
                        'image': [
                            {
                                'band': data['band'],
                                'view': view,
                                'array': data['image_triplet'][i, :, :, j],
                                'scale': data['image_scale'],
                            }
                            for j, view in enumerate(self._views)
                        ]
                    }
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype('float32')
                    for f in _INT_FEATURES:
                        # NOTE: includes object_id
                        example[f] = data[f][i].astype('int64')
                    for f in _BOOL_FEATURES:
                        example[f] = data[f][i].astype('bool')
                    for f in _STRING_FEATURES:
                        example[f] = data[f][i].astype('str')

                    yield str(data['object_id'][i]), example