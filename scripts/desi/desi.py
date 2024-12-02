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
import itertools
import h5py
import numpy as np

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@article{2024,
   title={The Early Data Release of the Dark Energy Spectroscopic Instrument},
   volume={168},
   ISSN={1538-3881},
   url={http://dx.doi.org/10.3847/1538-3881/ad3217},
   DOI={10.3847/1538-3881/ad3217},
   number={2},
   journal={The Astronomical Journal},
   publisher={American Astronomical Society},
   author={Adame, A. G. and Aguilar, J. and Ahlen, S. and Alam, S. and Aldering, G. and Alexander, D. M. and Alfarsy, R. and Allende Prieto, C. and Alvarez, M. and Alves, O. and Anand, A. and Andrade-Oliveira, F. and Armengaud, E. and Asorey, J. and Avila, S. and Aviles, A. and Bailey, S. and Balaguera-Antolínez, A. and Ballester, O. and Baltay, C. and Bault, A. and Bautista, J. and Behera, J. and Beltran, S. F. and BenZvi, S. and Beraldo e Silva, L. and Bermejo-Climent, J. R. and Berti, A. and Besuner, R. and Beutler, F. and Bianchi, D. and Blake, C. and Blum, R. and Bolton, A. S. and Brieden, S. and Brodzeller, A. and Brooks, D. and Brown, Z. and Buckley-Geer, E. and Burtin, E. and Cabayol-Garcia, L. and Cai, Z. and Canning, R. and Cardiel-Sas, L. and Carnero Rosell, A. and Castander, F. J. and Cervantes-Cota, J. L. and Chabanier, S. and Chaussidon, E. and Chaves-Montero, J. and Chen, S. and Chen, X. and Chuang, C. and Claybaugh, T. and Cole, S. and Cooper, A. P. and Cuceu, A. and Davis, T. M. and Dawson, K. and de Belsunce, R. and de la Cruz, R. and de la Macorra, A. and Della Costa, J. and de Mattia, A. and Demina, R. and Demirbozan, U. and DeRose, J. and Dey, A. and Dey, B. and Dhungana, G. and Ding, J. and Ding, Z. and Doel, P. and Doshi, R. and Douglass, K. and Edge, A. and Eftekharzadeh, S. and Eisenstein, D. J. and Elliott, A. and Ereza, J. and Escoffier, S. and Fagrelius, P. and Fan, X. and Fanning, K. and Fawcett, V. A. and Ferraro, S. and Flaugher, B. and Font-Ribera, A. and Forero-Romero, J. E. and Forero-Sánchez, D. and Frenk, C. S. and Gänsicke, B. T. and García, L. Á. and García-Bellido, J. and Garcia-Quintero, C. and Garrison, L. H. and Gil-Marín, H. and Golden-Marx, J. and Gontcho A Gontcho, S. and Gonzalez-Morales, A. X. and Gonzalez-Perez, V. and Gordon, C. and Graur, O. and Green, D. and Gruen, D. and Guy, J. and Hadzhiyska, B. and Hahn, C. and Han, J. J. and Hanif, M. M. S and Herrera-Alcantar, H. K. and Honscheid, K. and Hou, J. and Howlett, C. and Huterer, D. and Iršič, V. and Ishak, M. and Jacques, A. and Jana, A. and Jiang, L. and Jimenez, J. and Jing, Y. P. and Joudaki, S. and Joyce, R. and Jullo, E. and Juneau, S. and Karaçaylı, N. G. and Karim, T. and Kehoe, R. and Kent, S. and Khederlarian, A. and Kim, S. and Kirkby, D. and Kisner, T. and Kitaura, F. and Kizhuprakkat, N. and Kneib, J. and Koposov, S. E. and Kovács, A. and Kremin, A. and Krolewski, A. and L’Huillier, B. and Lahav, O. and Lambert, A. and Lamman, C. and Lan, T.-W. and Landriau, M. and Lang, D. and Lange, J. U. and Lasker, J. and Leauthaud, A. and Le Guillou, L. and Levi, M. E. and Li, T. S. and Linder, E. and Lyons, A. and Magneville, C. and Manera, M. and Manser, C. J. and Margala, D. and Martini, P. and McDonald, P. and Medina, G. E. and Medina-Varela, L. and Meisner, A. and Mena-Fernández, J. and Meneses-Rizo, J. and Mezcua, M. and Miquel, R. and Montero-Camacho, P. and Moon, J. and Moore, S. and Moustakas, J. and Mueller, E. and Mundet, J. and Muñoz-Gutiérrez, A. and Myers, A. D. and Nadathur, S. and Napolitano, L. and Neveux, R. and Newman, J. A. and Nie, J. and Nikutta, R. and Niz, G. and Norberg, P. and Noriega, H. E. and Paillas, E. and Palanque-Delabrouille, N. and Palmese, A. and Pan, Z. and Parkinson, D. and Penmetsa, S. and Percival, W. J. and Pérez-Fernández, A. and Pérez-Ràfols, I. and Pieri, M. and Poppett, C. and Porredon, A. and Pothier, S. and Prada, F. and Pucha, R. and Raichoor, A. and Ramírez-Pérez, C. and Ramirez-Solano, S. and Rashkovetskyi, M. and Ravoux, C. and Rocher, A. and Rockosi, C. and Ross, A. J. and Rossi, G. and Ruggeri, R. and Ruhlmann-Kleider, V. and Sabiu, C. G. and Said, K. and Saintonge, A. and Samushia, L. and Sanchez, E. and Saulder, C. and Schaan, E. and Schlafly, E. F. and Schlegel, D. and Scholte, D. and Schubnell, M. and Seo, H. and Shafieloo, A. and Sharples, R. and Sheu, W. and Silber, J. and Sinigaglia, F. and Siudek, M. and Slepian, Z. and Smith, A. and Soumagnac, M. T. and Sprayberry, D. and Stephey, L. and Suárez-Pérez, J. and Sun, Z. and Tan, T. and Tarlé, G. and Tojeiro, R. and Ureña-López, L. A. and Vaisakh, R. and Valcin, D. and Valdes, F. and Valluri, M. and Vargas-Magaña, M. and Variu, A. and Verde, L. and Walther, M. and Wang, B. and Wang, M. S. and Weaver, B. A. and Weaverdyck, N. and Wechsler, R. H. and White, M. and Xie, Y. and Yang, J. and Yèche, C. and Yu, J. and Yuan, S. and Zhang, H. and Zhang, Z. and Zhao, C. and Zheng, Z. and Zhou, R. and Zhou, Z. and Zou, H. and Zou, S. and Zu, Y.},
   year={2024},
   month=jul, pages={58} }
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
From https://data.desi.lbl.gov/doc/acknowledgments/ : 

The Dark Energy Spectroscopic Instrument (DESI) data are licensed under the Creative Commons Attribution 4.0 International License (“CC BY 4.0”, Summary, Full Legal Code). Users are free to share, copy, redistribute, adapt, transform and build upon the DESI data available through this website for any purpose, including commercially.

When DESI data are used, the appropriate credit is required by using both the following reference and acknowledgments text.

If you are using DESI data, you must cite the following reference and clearly state any changes made to these data:

DESI Collaboration et al. (2023b), “The Early Data Release of the Dark Energy Spectroscopic Instrument”

Also consider citing publications from the Technical Papers section if they cover any material used in your work.

As part of the license attributes, you are obliged to include the following acknowledgments:

This research used data obtained with the Dark Energy Spectroscopic Instrument (DESI). DESI construction and operations is managed by the Lawrence Berkeley National Laboratory. This material is based upon work supported by the U.S. Department of Energy, Office of Science, Office of High-Energy Physics, under Contract No. DE–AC02–05CH11231, and by the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility under the same contract. Additional support for DESI was provided by the U.S. National Science Foundation (NSF), Division of Astronomical Sciences under Contract No. AST-0950945 to the NSF’s National Optical-Infrared Astronomy Research Laboratory; the Science and Technology Facilities Council of the United Kingdom; the Gordon and Betty Moore Foundation; the Heising-Simons Foundation; the French Alternative Energies and Atomic Energy Commission (CEA); the National Council of Science and Technology of Mexico (CONACYT); the Ministry of Science and Innovation of Spain (MICINN), and by the DESI Member Institutions: www.desi.lbl.gov/collaborating-institutions. The DESI collaboration is honored to be permitted to conduct scientific research on Iolkam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation. Any opinions, findings, and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the U.S. National Science Foundation, the U.S. Department of Energy, or any of the listed funding agencies.
"""

# You can copy an official description
_DESCRIPTION = """\
Spectra dataset based on DESI EDR SV3.
"""

_HOMEPAGE = "https://data.desi.lbl.gov/doc"

_LICENSE = "CC BY 4.0"

_VERSION = "0.0.1"

# Full data model here:
# https://desidatamodel.readthedocs.io/en/latest/DESI_SPECTRO_REDUX/SPECPROD/zcatalog/zpix-SURVEY-PROGRAM.html

_BOOL_FEATURES = [
    "ZWARN"
]

_FLOAT_FEATURES = [
    "z",
    "zerr",
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
            name="edr_sv3",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["edr_sv3/healpix=*/*.hdf5"]}
            ),
            description="One percent survey of the DESI Early Data Release.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "edr_sv3"

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
                        example[f] = not bool(data[f][i])    # if flag is 0, then no problem

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example


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
