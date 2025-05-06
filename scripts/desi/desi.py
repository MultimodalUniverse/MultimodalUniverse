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
import h5py
import numpy as np

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@misc{desicollaboration2025datarelease1dark,
      title={Data Release 1 of the Dark Energy Spectroscopic Instrument}, 
      author={DESI Collaboration and M. Abdul-Karim and A. G. Adame and D. Aguado and J. Aguilar and S. Ahlen and S. Alam and G. Aldering and D. M. Alexander and R. Alfarsy and L. Allen and C. Allende Prieto and O. Alves and A. Anand and U. Andrade and E. Armengaud and S. Avila and A. Aviles and H. Awan and S. Bailey and A. Baleato Lizancos and O. Ballester and A. Bault and J. Bautista and S. BenZvi and L. Beraldo e Silva and J. R. Bermejo-Climent and F. Beutler and D. Bianchi and C. Blake and R. Blum and A. S. Bolton and M. Bonici and S. Brieden and A. Brodzeller and D. Brooks and E. Buckley-Geer and E. Burtin and R. Canning and A. Carnero Rosell and A. Carr and P. Carrilho and L. Casas and F. J. Castander and R. Cereskaite and J. L. Cervantes-Cota and E. Chaussidon and J. Chaves-Montero and S. Chen and X. Chen and T. Claybaugh and S. Cole and A. P. Cooper and M. -C. Cousinou and A. Cuceu and T. M. Davis and K. S. Dawson and R. de Belsunce and R. de la Cruz and A. de la Macorra and A. de Mattia and N. Deiosso and J. Della Costa and R. Demina and U. Demirbozan and J. DeRose and A. Dey and B. Dey and J. Ding and Z. Ding and P. Doel and K. Douglass and M. Dowicz and H. Ebina and J. Edelstein and D. J. Eisenstein and W. Elbers and N. Emas and S. Escoffier and P. Fagrelius and X. Fan and K. Fanning and V. A. Fawcett and E. Fernández-García and S. Ferraro and N. Findlay and A. Font-Ribera and J. E. Forero-Romero and D. Forero-Sánchez and C. S. Frenk and B. T. Gänsicke and L. Galbany and J. García-Bellido and C. Garcia-Quintero and L. H. Garrison and E. Gaztañaga and H. Gil-Marín and O. Y. Gnedin and S. Gontcho A Gontcho and A. X. Gonzalez-Morales and V. Gonzalez-Perez and C. Gordon and O. Graur and D. Green and D. Gruen and R. Gsponer and C. Guandalin and G. Gutierrez and J. Guy and C. Hahn and J. J. Han and J. Han and S. He and H. K. Herrera-Alcantar and K. Honscheid and J. Hou and C. Howlett and D. Huterer and V. Iršič and M. Ishak and A. Jacques and J. Jimenez and Y. P. Jing and B. Joachimi and S. Joudaki and R. Joyce and E. Jullo and S. Juneau and N. G. Karaçaylı and T. Karim and R. Kehoe and S. Kent and A. Khederlarian and D. Kirkby and T. Kisner and F. -S. Kitaura and N. Kizhuprakkat and H. Kong and S. E. Koposov and A. Kremin and A. Krolewski and O. Lahav and Y. Lai and C. Lamman and T. -W. Lan and M. Landriau and D. Lang and J. U. Lange and J. Lasker and J. M. Le Goff and L. Le Guillou and A. Leauthaud and M. E. Levi and S. Li and T. S. Li and K. Lodha and M. Lokken and Y. Luo and C. Magneville and M. Manera and C. J. Manser and D. Margala and P. Martini and M. Maus and J. McCullough and P. McDonald and G. E. Medina and L. Medina-Varela and A. Meisner and J. Mena-Fernández and A. Menegas and M. Mezcua and R. Miquel and P. Montero-Camacho and J. Moon and J. Moustakas and A. Muñoz-Gutiérrez and D. Muñoz-Santos and A. D. Myers and J. Myles and S. Nadathur and J. Najita and L. Napolitano and J. A. Newman and F. Nikakhtar and R. Nikutta and G. Niz and H. E. Noriega and N. Padmanabhan and E. Paillas and N. Palanque-Delabrouille and A. Palmese and J. Pan and Z. Pan and D. Parkinson and J. Peacock and W. J. Percival and A. Pérez-Fernández and I. Pérez-Ràfols and P. Peterson and J. Piat and M. M. Pieri and M. Pinon and C. Poppett and A. Porredon and F. Prada and R. Pucha and F. Qin and D. Rabinowitz and A. Raichoor and C. Ramírez-Pérez and S. Ramirez-Solano and M. Rashkovetskyi and C. Ravoux and A. H. Riley and A. Rocher and C. Rockosi and J. Rohlf and A. J. Ross and G. Rossi and R. Ruggeri and V. Ruhlmann-Kleider and C. G. Sabiu and K. Said and A. Saintonge and L. Samushia and E. Sanchez and N. Sanders and C. Saulder and E. F. Schlafly and D. Schlegel and D. Scholte and M. Schubnell and H. Seo and A. Shafieloo and R. Sharples and J. Silber and M. Siudek and A. Smith and D. Sprayberry and J. Suárez-Pérez and J. Swanson and T. Tan and G. Tarlé and P. Taylor and G. Thomas and R. Tojeiro and R. J. Turner and W. Turner and L. A. Ureña-López and R. Vaisakh and M. Valluri and M. Vargas-Magaña and L. Verde and M. Walther and B. Wang and M. S. Wang and W. Wang and B. A. Weaver and N. Weaverdyck and R. H. Wechsler and M. White and M. Wolfson and J. Yang and C. Yèche and S. Youles and J. Yu and S. Yuan and E. A. Zaborowski and P. Zarrouk and H. Zhang and C. Zhao and R. Zhao and Z. Zheng and R. Zhou and H. Zou and S. Zou and Y. Zu},
      year={2025},
      eprint={2503.14745},
      archivePrefix={arXiv},
      primaryClass={astro-ph.CO},
      url={https://arxiv.org/abs/2503.14745}, 
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
From https://data.desi.lbl.gov/doc/acknowledgments/ : 

This research used data obtained with the Dark Energy Spectroscopic Instrument (DESI). DESI construction and operations is managed by the Lawrence Berkeley National Laboratory. This material is based upon work supported by the U.S. Department of Energy, Office of Science, Office of High-Energy Physics, under Contract No. DE–AC02–05CH11231, and by the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility under the same contract. Additional support for DESI was provided by the U.S. National Science Foundation (NSF), Division of Astronomical Sciences under Contract No. AST-0950945 to the NSF’s National Optical-Infrared Astronomy Research Laboratory; the Science and Technology Facilities Council of the United Kingdom; the Gordon and Betty Moore Foundation; the Heising-Simons Foundation; the French Alternative Energies and Atomic Energy Commission (CEA); the National Council of Humanities, Science and Technology of Mexico (CONAHCYT); the Ministry of Science and Innovation of Spain (MICINN), and by the DESI Member Institutions: www.desi.lbl.gov/collaborating-institutions. The DESI collaboration is honored to be permitted to conduct scientific research on I’oligam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the U.S. National Science Foundation, the U.S. Department of Energy, or any of the listed funding agencies.
"""

# You can copy an official description
_DESCRIPTION = """Spectra dataset based on DESI DR1."""

_HOMEPAGE = "https://data.desi.lbl.gov/doc"

_LICENSE = "CC BY 4.0"

_VERSION = "1.0.0"

# Full data model here:
# https://desidatamodel.readthedocs.io/en/latest/DESI_SPECTRO_REDUX/SPECPROD/zcatalog/zpix-SURVEY-PROGRAM.html

_BOOL_FEATURES = [
    "ZWARN"
]

_FLOAT_FEATURES = [
    "Z",
    "ZERR",
    "EBV",
    "FLUX_G",
    "FLUX_R",
    "FLUX_Z",
    "FLUX_IVAR_G",
    "FLUX_IVAR_R",
    "FLUX_IVAR_Z",
    "FIBERFLUX_G",
    "FIBERFLUX_R",
    "FIBERFLUX_Z",
    "FIBERTOTFLUX_G",
    "FIBERTOTFLUX_R",
    "FIBERTOTFLUX_Z",
]

class DESI(datasets.GeneratorBasedBuilder):
    """Spectra from the Dark Energy Spectroscopic Instrument (DESI)."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="dr1_main",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["dr1_main/healpix=*/*.hdf5"]}
            ),
            description="DESI DR1 main sample.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "dr1_main"

    _spectrum_length = 7781

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to image datasets
        features = {
            "spectrum": Sequence(feature={
                "flux": Value(dtype="float32"),
                "ivar": Value(dtype="float32"),
                "lsf_sigma":  Value(dtype="float32"),
                "lambda": Value(dtype="float32"),
                "mask": Value(dtype="bool"),
            }, length=self._spectrum_length)
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding all boolean flags
        for f in _BOOL_FEATURES:
            features[f] = Value("bool")

        features["object_id"] = Value("string")

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
                        "spectrum": 
                            {
                                "flux": data['spectrum_flux'][i], 
                                "ivar": data['spectrum_ivar'][i],
                                "lsf_sigma": data['spectrum_lsf_sigma'][i],
                                "lambda": data['spectrum_lambda'][i],
                                "mask": data['spectrum_mask'][i],
                            }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    # Add all boolean flags
                    for f in _BOOL_FEATURES:
                        # if flag is 0, then no problem
                        example[f] = not bool(data[f][i])  

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example


    def _split_generators(self, dl_manager):
        """We handle string, list and dicts in datafiles"""
        if not self.config.data_files:
            raise ValueError(
                "At least one data file must be specified, "
                f"but got data_files={self.config.data_files}"
            )
        splits = []
        for split_name, files in self.config.data_files.items():
            if isinstance(files, str):
                files = [files]
            splits.append(
                datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})
            )
        return splits
