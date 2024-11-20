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
import itertools

import datasets
import h5py
import numpy as np
from datasets import Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = """\
@ARTICLE{2017AJ....154...28B,
       author = {{Blanton}, Michael R. and et al.},
        title = "{Sloan Digital Sky Survey IV: Mapping the Milky Way, Nearby Galaxies, and the Distant Universe}",
      journal = {\aj},
     keywords = {cosmology: observations, galaxies: general, Galaxy: general, instrumentation: spectrographs, stars: general, surveys, Astrophysics - Astrophysics of Galaxies},
         year = 2017,
        month = jul,
       volume = {154},
       number = {1},
          eid = {28},
        pages = {28},
          doi = {10.3847/1538-3881/aa7567},
archivePrefix = {arXiv},
       eprint = {1703.00052},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2017AJ....154...28B},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2022ApJS..259...35A,
       author = {{Abdurro'uf} and et al.},
        title = "{The Seventeenth Data Release of the Sloan Digital Sky Surveys: Complete Release of MaNGA, MaStar, and APOGEE-2 Data}",
      journal = {\apjs},
     keywords = {Astronomy data acquisition, Astronomy databases, Surveys, 1860, 83, 1671, Astrophysics - Astrophysics of Galaxies, Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2022,
        month = apr,
       volume = {259},
       number = {2},
          eid = {35},
        pages = {35},
          doi = {10.3847/1538-4365/ac4414},
archivePrefix = {arXiv},
       eprint = {2112.02026},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2022ApJS..259...35A},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2017AJ....154...94M,
       author = {{Majewski}, Steven R. and {Schiavon}, Ricardo P. and {Frinchaboy}, Peter M. and {Allende Prieto}, Carlos and {Barkhouser}, Robert and {Bizyaev}, Dmitry and {Blank}, Basil and {Brunner}, Sophia and {Burton}, Adam and {Carrera}, Ricardo and {Chojnowski}, S. Drew and {Cunha}, K{\'a}tia and {Epstein}, Courtney and {Fitzgerald}, Greg and {Garc{\'\i}a P{\'e}rez}, Ana E. and {Hearty}, Fred R. and {Henderson}, Chuck and {Holtzman}, Jon A. and {Johnson}, Jennifer A. and {Lam}, Charles R. and {Lawler}, James E. and {Maseman}, Paul and {M{\'e}sz{\'a}ros}, Szabolcs and {Nelson}, Matthew and {Nguyen}, Duy Coung and {Nidever}, David L. and {Pinsonneault}, Marc and {Shetrone}, Matthew and {Smee}, Stephen and {Smith}, Verne V. and {Stolberg}, Todd and {Skrutskie}, Michael F. and {Walker}, Eric and {Wilson}, John C. and {Zasowski}, Gail and {Anders}, Friedrich and {Basu}, Sarbani and {Beland}, Stephane and {Blanton}, Michael R. and {Bovy}, Jo and {Brownstein}, Joel R. and {Carlberg}, Joleen and {Chaplin}, William and {Chiappini}, Cristina and {Eisenstein}, Daniel J. and {Elsworth}, Yvonne and {Feuillet}, Diane and {Fleming}, Scott W. and {Galbraith-Frew}, Jessica and {Garc{\'\i}a}, Rafael A. and {Garc{\'\i}a-Hern{\'a}ndez}, D. An{\'\i}bal and {Gillespie}, Bruce A. and {Girardi}, L{\'e}o and {Gunn}, James E. and {Hasselquist}, Sten and {Hayden}, Michael R. and {Hekker}, Saskia and {Ivans}, Inese and {Kinemuchi}, Karen and {Klaene}, Mark and {Mahadevan}, Suvrath and {Mathur}, Savita and {Mosser}, Beno{\^\i}t and {Muna}, Demitri and {Munn}, Jeffrey A. and {Nichol}, Robert C. and {O'Connell}, Robert W. and {Parejko}, John K. and {Robin}, A.~C. and {Rocha-Pinto}, Helio and {Schultheis}, Matthias and {Serenelli}, Aldo M. and {Shane}, Neville and {Silva Aguirre}, Victor and {Sobeck}, Jennifer S. and {Thompson}, Benjamin and {Troup}, Nicholas W. and {Weinberg}, David H. and {Zamora}, Olga},
        title = "{The Apache Point Observatory Galactic Evolution Experiment (APOGEE)}",
      journal = {\aj},
     keywords = {Galaxy: abundances, Galaxy: evolution, Galaxy: formation, Galaxy: kinematics and dynamics, Galaxy: stellar content, Galaxy: structure, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Astrophysics of Galaxies},
         year = 2017,
        month = sep,
       volume = {154},
       number = {3},
          eid = {94},
        pages = {94},
          doi = {10.3847/1538-3881/aa784d},
archivePrefix = {arXiv},
       eprint = {1509.05420},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2017AJ....154...94M},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2019PASP..131e5001W,
       author = {{Wilson}, J.~C. and {Hearty}, F.~R. and {Skrutskie}, M.~F. and {Majewski}, S.~R. and {Holtzman}, J.~A. and {Eisenstein}, D. and {Gunn}, J. and {Blank}, B. and {Henderson}, C. and {Smee}, S. and {Nelson}, M. and {Nidever}, D. and {Arns}, J. and {Barkhouser}, R. and {Barr}, J. and {Beland}, S. and {Bershady}, M.~A. and {Blanton}, M.~R. and {Brunner}, S. and {Burton}, A. and {Carey}, L. and {Carr}, M. and {Colque}, J.~P. and {Crane}, J. and {Damke}, G.~J. and {Davidson}, J.~W., Jr. and {Dean}, J. and {Di Mille}, F. and {Don}, K.~W. and {Ebelke}, G. and {Evans}, M. and {Fitzgerald}, G. and {Gillespie}, B. and {Hall}, M. and {Harding}, A. and {Harding}, P. and {Hammond}, R. and {Hancock}, D. and {Harrison}, C. and {Hope}, S. and {Horne}, T. and {Karakla}, J. and {Lam}, C. and {Leger}, F. and {MacDonald}, N. and {Maseman}, P. and {Matsunari}, J. and {Melton}, S. and {Mitcheltree}, T. and {O'Brien}, T. and {O'Connell}, R.~W. and {Patten}, A. and {Richardson}, W. and {Rieke}, G. and {Rieke}, M. and {Roman-Lopes}, A. and {Schiavon}, R.~P. and {Sobeck}, J.~S. and {Stolberg}, T. and {Stoll}, R. and {Tembe}, M. and {Trujillo}, J.~D. and {Uomoto}, A. and {Vernieri}, M. and {Walker}, E. and {Weinberg}, D.~H. and {Young}, E. and {Anthony-Brumfield}, B. and {Bizyaev}, D. and {Breslauer}, B. and {De Lee}, N. and {Downey}, J. and {Halverson}, S. and {Huehnerhoff}, J. and {Klaene}, M. and {Leon}, E. and {Long}, D. and {Mahadevan}, S. and {Malanushenko}, E. and {Nguyen}, D.~C. and {Owen}, R. and {S{\'a}nchez-Gallego}, J.~R. and {Sayres}, C. and {Shane}, N. and {Shectman}, S.~A. and {Shetrone}, M. and {Skinner}, D. and {Stauffer}, F. and {Zhao}, B.},
        title = "{The Apache Point Observatory Galactic Evolution Experiment (APOGEE) Spectrographs}",
      journal = {\pasp},
     keywords = {Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2019,
        month = may,
       volume = {131},
       number = {999},
        pages = {055001},
          doi = {10.1088/1538-3873/ab0075},
archivePrefix = {arXiv},
       eprint = {1902.00928},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2019PASP..131e5001W},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2016AJ....151..144G,
       author = {{Garc{\'\i}a P{\'e}rez}, Ana E. and {Allende Prieto}, Carlos and {Holtzman}, Jon A. and {Shetrone}, Matthew and {M{\'e}sz{\'a}ros}, Szabolcs and {Bizyaev}, Dmitry and {Carrera}, Ricardo and {Cunha}, Katia and {Garc{\'\i}a-Hern{\'a}ndez}, D.~A. and {Johnson}, Jennifer A. and {Majewski}, Steven R. and {Nidever}, David L. and {Schiavon}, Ricardo P. and {Shane}, Neville and {Smith}, Verne V. and {Sobeck}, Jennifer and {Troup}, Nicholas and {Zamora}, Olga and {Weinberg}, David H. and {Bovy}, Jo and {Eisenstein}, Daniel J. and {Feuillet}, Diane and {Frinchaboy}, Peter M. and {Hayden}, Michael R. and {Hearty}, Fred R. and {Nguyen}, Duy C. and {O'Connell}, Robert W. and {Pinsonneault}, Marc H. and {Wilson}, John C. and {Zasowski}, Gail},
        title = "{ASPCAP: The APOGEE Stellar Parameter and Chemical Abundances Pipeline}",
      journal = {\aj},
     keywords = {Galaxy: center, Galaxy: structure, methods: data analysis, stars: abundances, stars: atmospheres, Astrophysics - Solar and Stellar Astrophysics},
         year = 2016,
        month = jun,
       volume = {151},
       number = {6},
          eid = {144},
        pages = {144},
          doi = {10.3847/0004-6256/151/6/144},
archivePrefix = {arXiv},
       eprint = {1510.07635},
 primaryClass = {astro-ph.SR},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2016AJ....151..144G},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = """\
Apache Point Observatory Galactic Evolution Experiment (APOGEE) within the Sloan Digital Sky Survey (SDSS) is a high-resolution (R~22,000), 
high signal-to-noise (>100 per pixel typically) stellar spectroscopic survey with 2.5-m telescopes in northern and southern hemisphere in the near infrared H-band wavelength region.
"""

_HOMEPAGE = "https://www.sdss4.org/dr17/irspec/"

_LICENSE = "https://www.sdss4.org/collaboration/citing-sdss/"

_VERSION = "1.0.0"

# Full list of features available here:
# https://data.sdss.org/datamodel/files/APOGEE_ASPCAP/APRED_VERS/ASPCAP_VERS/allStar.html
_FLOAT_FEATURES = [
    "teff",
    "logg",
    "m_h",
    "alpha_m",
    "teff_err",
    "logg_err",
    "m_h_err",
    "alpha_m_err",
    "radial_velocity",
]

# Features that correspond to ugriz fluxes
_FLUX_FEATURES = []
_BOOL_FEATURES = ["restframe"]


class APOGEE(datasets.GeneratorBasedBuilder):
    """
    Apache Point Observatory Galactic Evolution Experiment (APOGEE)
    """

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="apogee",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["./apogee/healpix=*/*.hdf5"]}
            ),
            description="SDSS APOGEE survey spectra.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "apogee"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to spectral dataset
        features = {
            "spectrum": Sequence(
                {
                    "flux": Value(dtype="float32"),
                    "ivar": Value(dtype="float32"),
                    "lsf_sigma": Value(dtype="float32"),
                    "lambda": Value(dtype="float32"),
                    "bitmask": Value(dtype="int64"),
                    "pseudo_continuum_flux": Value(dtype="float32"),
                    "pseudo_continuum_ivar": Value(dtype="float32"),
                }
            )
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding all boolean flags
        for f in _BOOL_FEATURES:
            features[f] = Value("bool")

        # # Adding all flux values from the catalog
        # for f in _FLUX_FEATURES:
        #     for b in self._flux_filters:
        #         features[f"{f}_{b}"] = Value("float32")

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

                    # Parse spectrum data
                    example = {
                        "spectrum": {
                            "flux": data["spectrum_flux"][i],
                            "ivar": data["spectrum_ivar"][i],
                            "lsf_sigma": data["spectrum_lsf_sigma"][i],
                            "lambda": data["spectrum_lambda"][i],
                            "bitmask": data["spectrum_bitmask"][i],
                            "pseudo_continuum_flux": data[
                                "spectrum_pseudo_continuum_flux"
                            ][i],
                            "pseudo_continuum_ivar": data[
                                "spectrum_pseudo_continuum_ivar"
                            ][i],
                        }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    # Add all other requested features
                    for f in _FLUX_FEATURES:
                        for n, b in enumerate(self._flux_filters):
                            example[f"{f}_{b}"] = data[f"{f}"][i][n].astype("float32")

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
