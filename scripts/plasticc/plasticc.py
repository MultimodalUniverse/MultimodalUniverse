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
from pathlib import Path
import itertools
import h5py
import numpy as np

_CITATION = r"""% CITATION
@article{Kessler_2019,
   title={Models and Simulations for the Photometric LSST Astronomical Time Series Classification Challenge (PLAsTiCC)},
   volume={131},
   ISSN={1538-3873},
   url={http://dx.doi.org/10.1088/1538-3873/ab26f1},
   DOI={10.1088/1538-3873/ab26f1},
   number={1003},
   journal={Publications of the Astronomical Society of the Pacific},
   publisher={IOP Publishing},
   author={Kessler, R. and Narayan, G. and Avelino, A. and Bachelet, E. and Biswas, R. and Brown, P. J. and Chernoff, D. F. and Connolly, A. J. and Dai, M. and Daniel, S. and Stefano, R. Di and Drout, M. R. and Galbany, L. and González-Gaitán, S. and Graham, M. L. and Hložek, R. and Ishida, E. E. O. and Guillochon, J. and Jha, S. W. and Jones, D. O. and Mandel, K. S. and Muthukrishna, D. and O’Grady, A. and Peters, C. M. and Pierel, J. R. and Ponder, K. A. and Prša, A. and Rodney, S. and Villar, V. A.},
   year={2019},
   month=jul, pages={094501} }
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
PLAsTiCC is funded through LSST Corporation Grant Award # 2017-03 and administered by the University of Toronto. Financial support for LSST comes from the National Science Foundation (NSF) through Cooperative Agreement No. 1258333, the Department of Energy (DOE) Office of Science under Contract No. DE-AC02-76SF00515, and private funding raised by the LSST Corporation. The NSF-funded LSST Project Office for construction was established as an operating center under management of the Association of Universities for Research in Astronomy (AURA). The DOE-funded effort to build the LSST camera is managed by the SLAC National Accelerator Laboratory (SLAC).

The National Science Foundation (NSF) is an independent federal agency created by Congress in 1950 to promote the progress of science. NSF supports basic research and people to create knowledge that transforms the future
"""

_DESCRIPTION = """\
The Photometric LSST Astronomical Time-Series Classification Challenge (PLAsTiCC) is a community-wide challenge to spur development of algorithms to classify astronomical transients. The Large Synoptic Survey Telescope (LSST) will discover tens of thousands of transient phenomena every single night. To deal with this massive onset of data, automated algorithms to classify and sort astronomical transients are crucial.
"""

_HOMEPAGE = "https://zenodo.org/records/2539456"

_LICENSE = "CC BY 4.0"

_VERSION = "1.0.0"

_FLOAT_FEATURES = [
        'hostgal_photoz',
        'hostgal_specz',
        'redshift',
    ]

_STR_FEATURES = [
        'obj_type',
    ]

_CLASS_MAPPING = {
    90: "SNIa",
    67: "SNIa-91bg",
    52: "SNIax",
    42: "SNII",
    62: "SNIbc",
    95: "SLSN-I",
    15: "TDE",
    64: "KN",
    88: "AGN",
    92: "RRL",
    65: "M-dwarf",
    16: "EB",
    53: "Mira",
    6: "MicroLens-Single",
    99: "extra",
    991: "MicroLens-Binary",
    992: "ILOT",
    993: "CaRT",
    994: "PISN",
    995: "MicroLens-String"
}

_BANDS = ['u', 'g', 'r', 'i', 'z', 'Y']

class PLAsTiCC(datasets.GeneratorBasedBuilder):

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="plasticc",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["data/healpix=*/train*.hdf5"], "test": ["data/healpix=*/test*.hdf5"]}),
            description="train: plasticc train (spectroscopic), test: plasticc test (photometric)",
        ),
        datasets.BuilderConfig(name="train_only",
                                version=VERSION,
                                data_files=DataFilesPatternsDict.from_patterns({"train": ["data/healpix=*/train*.hdf5"]}),
                                description="load train (spectroscopic) data only"),
        datasets.BuilderConfig(name="test_only",
                                version=VERSION,
                                data_files=DataFilesPatternsDict.from_patterns({"train": ["data/healpix=*/test*.hdf5"]}),
                                description="load test (photometric) data only"),
    ]

    DEFAULT_CONFIG_NAME = "train_only"

    @classmethod
    def _info(self):
        """ Defines the features available in this dataset.
        """
        # Starting with all features common to time series datasets
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
            features[f] = Value('float32')
        for f in _STR_FEATURES:
            features[f] = Value('string')
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
                    # data['lightcurve'][i] is a single lightcurve of shape n_bands x 3 x seq_len
                    lightcurve = data['lightcurve'][i]
                    n_bands, _, seq_len = lightcurve.shape
                    band_arr = np.array([np.ones(seq_len) * band for band in range(n_bands)]).flatten().astype('int')
                    # convert to dict of lists
                    example = {'lightcurve':  {
                                    "band": np.array(_BANDS)[band_arr],
                                    "time": lightcurve[:, 0].flatten(),
                                    "flux": lightcurve[:, 1].flatten(),
                                    "flux_err": lightcurve[:, 2].flatten(),
                        }}
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype('float32')
                    for f in _STR_FEATURES:
                        if f == "obj_type":
                            example[f] = _CLASS_MAPPING[data[f][i]]
                        else:
                            example[f] = data[f][i].astype('str')

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data['object_id'][i]), example
