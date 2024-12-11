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

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@ARTICLE{2021MNRAS.506..150B,
       author = {{Buder}, Sven and {Sharma}, Sanjib and {Kos}, Janez and {Amarsi}, Anish M. and {Nordlander}, Thomas and {Lind}, Karin and {Martell}, Sarah L. and {Asplund}, Martin and {Bland-Hawthorn}, Joss and {Casey}, Andrew R. and {de Silva}, Gayandhi M. and {D'Orazi}, Valentina and {Freeman}, Ken C. and {Hayden}, Michael R. and {Lewis}, Geraint F. and {Lin}, Jane and {Schlesinger}, Katharine J. and {Simpson}, Jeffrey D. and {Stello}, Dennis and {Zucker}, Daniel B. and {Zwitter}, Toma{\v{z}} and {Beeson}, Kevin L. and {Buck}, Tobias and {Casagrande}, Luca and {Clark}, Jake T. and {{\v{C}}otar}, Klemen and {da Costa}, Gary S. and {de Grijs}, Richard and {Feuillet}, Diane and {Horner}, Jonathan and {Kafle}, Prajwal R. and {Khanna}, Shourya and {Kobayashi}, Chiaki and {Liu}, Fan and {Montet}, Benjamin T. and {Nandakumar}, Govind and {Nataf}, David M. and {Ness}, Melissa K. and {Spina}, Lorenzo and {Tepper-Garc{\'\i}a}, Thor and {Ting}, Yuan-Sen and {Traven}, Gregor and {Vogrin{\v{c}}i{\v{c}}}, Rok and {Wittenmyer}, Robert A. and {Wyse}, Rosemary F.~G. and {{\v{Z}}erjal}, Maru{\v{s}}a and {Galah Collaboration}},
        title = "{The GALAH+ survey: Third data release}",
      journal = {\mnras},
     keywords = {methods: data analysis, methods: observational, surveys, stars: abundances, stars: fundamental parameters, Astrophysics - Astrophysics of Galaxies, Astrophysics - Solar and Stellar Astrophysics},
         year = 2021,
        month = sep,
       volume = {506},
       number = {1},
        pages = {150-201},
          doi = {10.1093/mnras/stab1242},
archivePrefix = {arXiv},
       eprint = {2011.02505},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2021MNRAS.506..150B},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% From: https://www.galah-survey.org/dr3/cite_us/
This work made use of the Third Data Release of the GALAH Survey (Buder et al. 2021). The GALAH Survey is based on data acquired through the Australian Astronomical Observatory, under programs: A/2013B/13 (The GALAH pilot survey); A/2014A/25, A/2015A/19, A2017A/18 (The GALAH survey phase 1); A2018A/18 (Open clusters with HERMES); A2019A/1 (Hierarchical star formation in Ori OB1); A2019A/15 (The GALAH survey phase 2); A/2015B/19, A/2016A/22, A/2016B/10, A/2017B/16, A/2018B/15 (The HERMES-TESS program); and A/2015A/3, A/2015B/1, A/2015B/19, A/2016A/22, A/2016B/12, A/2017A/14 (The HERMES K2-follow-up program). We acknowledge the traditional owners of the land on which the AAT stands, the Gamilaraay people, and pay our respects to elders past and present. This paper includes data that has been provided by AAO Data Central (datacentral.org.au).

This work has made use of data from the European Space Agency (ESA) mission Gaia (https://www.cosmos.esa.int/gaia), processed by the Gaia Data Processing and Analysis Consortium (DPAC, https://www.cosmos.esa.int/web/gaia/dpac/consortium). Funding for the DPAC has been provided by national institutions, in particular the institutions participating in the Gaia Multilateral Agreement.
"""

# You can copy an official description
_DESCRIPTION = """\
Spectra based on the Third Data Release of the Galactic Archaeology with HERMES (GALAH) survey provides one-dimensional spectra, stellar atmospheric parameters and individual elemental abundances for 678,423 spectra of 588,571 mostly nearby stars. They were observed with the HERMES spectrograph at the Anglo-Australian Telescope between November 2013 and February 2019. The release is fully described in Buder et al. (2021)
"""

_HOMEPAGE = "https://www.galah-survey.org/dr3/overview/"

_LICENSE = "CC BY 4.0"  # No license provided publically.

_VERSION = "3.0.0"

# see available data columns here: https://www.galah-survey.org/dr3/table_schema/
_FLOAT_FEATURES = [
    "timestamp",
    "ra",
    "dec",
    "rv",
    "e_rv",
    "log_lum",
    "m_act",
    "age",
    "distance",
    "radius",
    "e_log_lum",
    "e_m_act",
    "e_age",
    "e_distance",
    "e_radius",
    "teff",
    "logg",
    "fe_h",
    "vbroad",
    "alpha_fe",
    "vmic",
    "e_teff",
    "e_logg",
    "e_fe_h",
    "e_vbroad",
    "e_alpha_fe",
    "ebv",
    "Li_fe",
    "C_fe",
    "O_fe",
    "Na_fe",
    "Mg_fe",
    "Al_fe",
    "Si_fe",
    "K_fe",
    "Ca_fe",
    "Sc_fe",
    "Sc2_fe",
    "Ti_fe",
    "Ti2_fe",
    "V_fe",
    "Cr_fe",
    "Cr2_fe",
    "Mn_fe",
    "Co_fe",
    "Ni_fe",
    "Cu_fe",
    "Zn_fe",
    "Rb_fe",
    "Sr_fe",
    "Y_fe",
    "Zr_fe",
    "Mo_fe",
    "Ru_fe",
    "Ba_fe",
    "La_fe",
    "Ce_fe",
    "Nd_fe",
    "Sm_fe",
    "Eu_fe",
    "Li_fe",
    "C_fe",
    "O_fe",
    "Na_fe",
    "Mg_fe",
    "Al_fe",
    "Si_fe",
    "K_fe",
    "Ca_fe",
    "Sc_fe",
    "Sc2_fe",
    "Ti_fe",
    "Ti2_fe",
    "V_fe",
    "Cr_fe",
    "Cr2_fe",
    "Mn_fe",
    "Co_fe",
    "Ni_fe",
    "Cu_fe",
    "Zn_fe",
    "Rb_fe",
    "Sr_fe",
    "Y_fe",
    "Zr_fe",
    "Mo_fe",
    "Ru_fe",
    "Ba_fe",
    "La_fe",
    "Ce_fe",
    "Nd_fe",
    "Sm_fe",
    "Eu_fe",
    "e_Li_fe",
    "e_C_fe",
    "e_O_fe",
    "e_Na_fe",
    "e_Mg_fe",
    "e_Al_fe",
    "e_Si_fe",
    "e_K_fe",
    "e_Ca_fe",
    "e_Sc_fe",
    "e_Sc2_fe",
    "e_Ti_fe",
    "e_Ti2_fe",
    "e_V_fe",
    "e_Cr_fe",
    "e_Cr2_fe",
    "e_Mn_fe",
    "e_Co_fe",
    "e_Ni_fe",
    "e_Cu_fe",
    "e_Zn_fe",
    "e_Rb_fe",
    "e_Sr_fe",
    "e_Y_fe",
    "e_Zr_fe",
    "e_Mo_fe",
    "e_Ru_fe",
    "e_Ba_fe",
    "e_La_fe",
    "e_Ce_fe",
    "e_Nd_fe",
    "e_Sm_fe",
    "e_Eu_fe",
    "e_Li_fe",
    "e_C_fe",
    "e_O_fe",
    "e_Na_fe",
    "e_Mg_fe",
    "e_Al_fe",
    "e_Si_fe",
    "e_K_fe",
    "e_Ca_fe",
    "e_Sc_fe",
    "e_Sc2_fe",
    "e_Ti_fe",
    "e_Ti2_fe",
    "e_V_fe",
    "e_Cr_fe",
    "e_Cr2_fe",
    "e_Mn_fe",
    "e_Co_fe",
    "e_Ni_fe",
    "e_Cu_fe",
    "e_Zn_fe",
    "e_Rb_fe",
    "e_Sr_fe",
    "e_Y_fe",
    "e_Zr_fe",
    "e_Mo_fe",
    "e_Ru_fe",
    "e_Ba_fe",
    "e_La_fe",
    "e_Ce_fe",
    "e_Nd_fe",
    "e_Sm_fe",
    "e_Eu_fe",
]


_INT_FEATURES = [
    "healpix",
    "flag_sp",
    "flag_fe_h",
    "flag_Li_fe",
    "flag_C_fe",
    "flag_O_fe",
    "flag_Na_fe",
    "flag_Mg_fe",
    "flag_Al_fe",
    "flag_Si_fe",
    "flag_K_fe",
    "flag_Ca_fe",
    "flag_Sc_fe",
    "flag_Sc2_fe",
    "flag_Ti_fe",
    "flag_Ti2_fe",
    "flag_V_fe",
    "flag_Cr_fe",
    "flag_Cr2_fe",
    "flag_Mn_fe",
    "flag_Co_fe",
    "flag_Ni_fe",
    "flag_Cu_fe",
    "flag_Zn_fe",
    "flag_Rb_fe",
    "flag_Sr_fe",
    "flag_Y_fe",
    "flag_Zr_fe",
    "flag_Mo_fe",
    "flag_Ru_fe",
    "flag_Ba_fe",
    "flag_La_fe",
    "flag_Ce_fe",
    "flag_Nd_fe",
    "flag_Sm_fe",
    "flag_Eu_fe",
    "flag_Li_fe",
    "flag_C_fe",
    "flag_O_fe",
    "flag_Na_fe",
    "flag_Mg_fe",
    "flag_Al_fe",
    "flag_Si_fe",
    "flag_K_fe",
    "flag_Ca_fe",
    "flag_Sc_fe",
    "flag_Sc2_fe",
    "flag_Ti_fe",
    "flag_Ti2_fe",
    "flag_V_fe",
    "flag_Cr_fe",
    "flag_Cr2_fe",
    "flag_Mn_fe",
    "flag_Co_fe",
    "flag_Ni_fe",
    "flag_Cu_fe",
    "flag_Zn_fe",
    "flag_Rb_fe",
    "flag_Sr_fe",
    "flag_Y_fe",
    "flag_Zr_fe",
    "flag_Mo_fe",
    "flag_Ru_fe",
    "flag_Ba_fe",
    "flag_La_fe",
    "flag_Ce_fe",
    "flag_Nd_fe",
    "flag_Sm_fe",
    "flag_Eu_fe",
]


class GALAH(datasets.GeneratorBasedBuilder):
    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="dr3",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["dr3/healpix=*/*.hdf5"]}
            ),
            description="GALAH DR3",
        ),
    ]

    DEFAULT_CONFIG_NAME = "dr3"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to image datasets
        features = {
            "spectrum": Sequence(
                {
                    "flux": Value(dtype="float32"),
                    "ivar": Value(dtype="float32"),
                    "lsf": Value(dtype="float32"),
                    "lsf_sigma": Value(dtype="float32"),
                    "lambda": Value(dtype="float32"),
                    "norm_flux": Value(dtype="float32"),
                    "norm_ivar": Value(dtype="float32"),
                    "norm_lambda": Value(dtype="float32"),
                }
            ),
            "filter_indices": {
                "B_start": Value(dtype="int32"),
                "B_end": Value(dtype="int32"),
                "G_start": Value(dtype="int32"),
                "G_end": Value(dtype="int32"),
                "R_start": Value(dtype="int32"),
                "R_end": Value(dtype="int32"),
                "I_start": Value(dtype="int32"),
                "I_end": Value(dtype="int32"),
            },
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        for f in _INT_FEATURES:
            features[f] = Value("int32")

        features["object_id"] = Value("string")
        features["flux_unit"] = Value("string")

        ACKNOWLEDGEMENTS = "\n".join(
            [f"% {line}" for line in _ACKNOWLEDGEMENTS.split("\n")]
        )

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

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(itertools.chain.from_iterable(files)):
            with h5py.File(file, "r+") as data:
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
                            "norm_flux": data["spectrum_norm_flux"][i],
                            "norm_ivar": data["spectrum_norm_ivar"][i],
                            "norm_lambda": data["spectrum_norm_lambda"][i],
                        }
                    }
                    example["filter_indices"] = {
                        "B_start": data["spectrum_B_ind_start"][i].astype("int32"),
                        "B_end": data["spectrum_B_ind_end"][i].astype("int32"),
                        "G_start": data["spectrum_G_ind_start"][i].astype("int32"),
                        "G_end": data["spectrum_G_ind_end"][i].astype("int32"),
                        "R_start": data["spectrum_R_ind_start"][i].astype("int32"),
                        "R_end": data["spectrum_R_ind_end"][i].astype("int32"),
                        "I_start": data["spectrum_I_ind_start"][i].astype("int32"),
                        "I_end": data["spectrum_I_ind_end"][i].astype("int32"),
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    for f in _INT_FEATURES:
                        example[f] = data[f][i].astype("int32")

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])
                    example["flux_unit"] = "erg/cm^2/A/s"

                    yield str(data["object_id"][i]), example
