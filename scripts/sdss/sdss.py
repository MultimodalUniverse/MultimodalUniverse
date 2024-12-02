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

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@ARTICLE{2022ApJS..259...35A,
       author = {{Abdurro'uf} and {Accetta}, Katherine and {Aerts}, Conny and {Silva Aguirre}, V{\'\i}ctor and {Ahumada}, Romina and {Ajgaonkar}, Nikhil and {Filiz Ak}, N. and {Alam}, Shadab and {Allende Prieto}, Carlos and {Almeida}, Andr{\'e}s and {Anders}, Friedrich and {Anderson}, Scott F. and {Andrews}, Brett H. and {Anguiano}, Borja and {Aquino-Ort{\'\i}z}, Erik and {Arag{\'o}n-Salamanca}, Alfonso and {Argudo-Fern{\'a}ndez}, Maria and {Ata}, Metin and {Aubert}, Marie and {Avila-Reese}, Vladimir and {Badenes}, Carles and {Barb{\'a}}, Rodolfo H. and {Barger}, Kat and {Barrera-Ballesteros}, Jorge K. and {Beaton}, Rachael L. and {Beers}, Timothy C. and {Belfiore}, Francesco and {Bender}, Chad F. and {Bernardi}, Mariangela and {Bershady}, Matthew A. and {Beutler}, Florian and {Bidin}, Christian Moni and {Bird}, Jonathan C. and {Bizyaev}, Dmitry and {Blanc}, Guillermo A. and {Blanton}, Michael R. and {Boardman}, Nicholas Fraser and {Bolton}, Adam S. and {Boquien}, M{\'e}d{\'e}ric and {Borissova}, Jura and {Bovy}, Jo and {Brandt}, W.~N. and {Brown}, Jordan and {Brownstein}, Joel R. and {Brusa}, Marcella and {Buchner}, Johannes and {Bundy}, Kevin and {Burchett}, Joseph N. and {Bureau}, Martin and {Burgasser}, Adam and {Cabang}, Tuesday K. and {Campbell}, Stephanie and {Cappellari}, Michele and {Carlberg}, Joleen K. and {Wanderley}, F{\'a}bio Carneiro and {Carrera}, Ricardo and {Cash}, Jennifer and {Chen}, Yan-Ping and {Chen}, Wei-Huai and {Cherinka}, Brian and {Chiappini}, Cristina and {Choi}, Peter Doohyun and {Chojnowski}, S. Drew and {Chung}, Haeun and {Clerc}, Nicolas and {Cohen}, Roger E. and {Comerford}, Julia M. and {Comparat}, Johan and {da Costa}, Luiz and {Covey}, Kevin and {Crane}, Jeffrey D. and {Cruz-Gonzalez}, Irene and {Culhane}, Connor and {Cunha}, Katia and {Dai}, Y. Sophia and {Damke}, Guillermo and {Darling}, Jeremy and {Davidson}, James W., Jr. and {Davies}, Roger and {Dawson}, Kyle and {De Lee}, Nathan and {Diamond-Stanic}, Aleksandar M. and {Cano-D{\'\i}az}, Mariana and {S{\'a}nchez}, Helena Dom{\'\i}nguez and {Donor}, John and {Duckworth}, Chris and {Dwelly}, Tom and {Eisenstein}, Daniel J. and {Elsworth}, Yvonne P. and {Emsellem}, Eric and {Eracleous}, Mike and {Escoffier}, Stephanie and {Fan}, Xiaohui and {Farr}, Emily and {Feng}, Shuai and {Fern{\'a}ndez-Trincado}, Jos{\'e} G. and {Feuillet}, Diane and {Filipp}, Andreas and {Fillingham}, Sean P. and {Frinchaboy}, Peter M. and {Fromenteau}, Sebastien and {Galbany}, Llu{\'\i}s and {Garc{\'\i}a}, Rafael A. and {Garc{\'\i}a-Hern{\'a}ndez}, D.~A. and {Ge}, Junqiang and {Geisler}, Doug and {Gelfand}, Joseph and {G{\'e}ron}, Tobias and {Gibson}, Benjamin J. and {Goddy}, Julian and {Godoy-Rivera}, Diego and {Grabowski}, Kathleen and {Green}, Paul J. and {Greener}, Michael and {Grier}, Catherine J. and {Griffith}, Emily and {Guo}, Hong and {Guy}, Julien and {Hadjara}, Massinissa and {Harding}, Paul and {Hasselquist}, Sten and {Hayes}, Christian R. and {Hearty}, Fred and {Hern{\'a}ndez}, Jes{\'u}s and {Hill}, Lewis and {Hogg}, David W. and {Holtzman}, Jon A. and {Horta}, Danny and {Hsieh}, Bau-Ching and {Hsu}, Chin-Hao and {Hsu}, Yun-Hsin and {Huber}, Daniel and {Huertas-Company}, Marc and {Hutchinson}, Brian and {Hwang}, Ho Seong and {Ibarra-Medel}, H{\'e}ctor J. and {Chitham}, Jacob Ider and {Ilha}, Gabriele S. and {Imig}, Julie and {Jaekle}, Will and {Jayasinghe}, Tharindu and {Ji}, Xihan and {Johnson}, Jennifer A. and {Jones}, Amy and {J{\"o}nsson}, Henrik and {Katkov}, Ivan and {Khalatyan}, Arman, Dr. and {Kinemuchi}, Karen and {Kisku}, Shobhit and {Knapen}, Johan H. and {Kneib}, Jean-Paul and {Kollmeier}, Juna A. and {Kong}, Miranda and {Kounkel}, Marina and {Kreckel}, Kathryn and {Krishnarao}, Dhanesh and {Lacerna}, Ivan and {Lane}, Richard R. and {Langgin}, Rachel and {Lavender}, Ramon and {Law}, David R. and {Lazarz}, Daniel and {Leung}, Henry W. and {Leung}, Ho-Hin and {Lewis}, Hannah M. and {Li}, Cheng and {Li}, Ran and {Lian}, Jianhui and {Liang}, Fu-Heng and {Lin}, Lihwai and {Lin}, Yen-Ting and {Lin}, Sicheng and {Lintott}, Chris and {Long}, Dan and {Longa-Pe{\~n}a}, Pen{\'e}lope and {L{\'o}pez-Cob{\'a}}, Carlos and {Lu}, Shengdong and {Lundgren}, Britt F. and {Luo}, Yuanze and {Mackereth}, J. Ted and {de la Macorra}, Axel and {Mahadevan}, Suvrath and {Majewski}, Steven R. and {Manchado}, Arturo and {Mandeville}, Travis and {Maraston}, Claudia and {Margalef-Bentabol}, Berta and {Masseron}, Thomas and {Masters}, Karen L. and {Mathur}, Savita and {McDermid}, Richard M. and {Mckay}, Myles and {Merloni}, Andrea and {Merrifield}, Michael and {Meszaros}, Szabolcs and {Miglio}, Andrea and {Di Mille}, Francesco and {Minniti}, Dante and {Minsley}, Rebecca and {Monachesi}, Antonela and {Moon}, Jeongin and {Mosser}, Benoit and {Mulchaey}, John and {Muna}, Demitri and {Mu{\~n}oz}, Ricardo R. and {Myers}, Adam D. and {Myers}, Natalie and {Nadathur}, Seshadri and {Nair}, Preethi and {Nandra}, Kirpal and {Neumann}, Justus and {Newman}, Jeffrey A. and {Nidever}, David L. and {Nikakhtar}, Farnik and {Nitschelm}, Christian and {O'Connell}, Julia E. and {Garma-Oehmichen}, Luis and {Luan Souza de Oliveira}, Gabriel and {Olney}, Richard and {Oravetz}, Daniel and {Ortigoza-Urdaneta}, Mario and {Osorio}, Yeisson and {Otter}, Justin and {Pace}, Zachary J. and {Padilla}, Nelson and {Pan}, Kaike and {Pan}, Hsi-An and {Parikh}, Taniya and {Parker}, James and {Peirani}, Sebastien and {Pe{\~n}a Ram{\'\i}rez}, Karla and {Penny}, Samantha and {Percival}, Will J. and {Perez-Fournon}, Ismael and {Pinsonneault}, Marc and {Poidevin}, Fr{\'e}d{\'e}rick and {Poovelil}, Vijith Jacob and {Price-Whelan}, Adrian M. and {B{\'a}rbara de Andrade Queiroz}, Anna and {Raddick}, M. Jordan and {Ray}, Amy and {Rembold}, Sandro Barboza and {Riddle}, Nicole and {Riffel}, Rogemar A. and {Riffel}, Rog{\'e}rio and {Rix}, Hans-Walter and {Robin}, Annie C. and {Rodr{\'\i}guez-Puebla}, Aldo and {Roman-Lopes}, Alexandre and {Rom{\'a}n-Z{\'u}{\~n}iga}, Carlos and {Rose}, Benjamin and {Ross}, Ashley J. and {Rossi}, Graziano and {Rubin}, Kate H.~R. and {Salvato}, Mara and {S{\'a}nchez}, Seb{\'a}stian F. and {S{\'a}nchez-Gallego}, Jos{\'e} R. and {Sanderson}, Robyn and {Santana Rojas}, Felipe Antonio and {Sarceno}, Edgar and {Sarmiento}, Regina and {Sayres}, Conor and {Sazonova}, Elizaveta and {Schaefer}, Adam L. and {Schiavon}, Ricardo and {Schlegel}, David J. and {Schneider}, Donald P. and {Schultheis}, Mathias and {Schwope}, Axel and {Serenelli}, Aldo and {Serna}, Javier and {Shao}, Zhengyi and {Shapiro}, Griffin and {Sharma}, Anubhav and {Shen}, Yue and {Shetrone}, Matthew and {Shu}, Yiping and {Simon}, Joshua D. and {Skrutskie}, M.~F. and {Smethurst}, Rebecca and {Smith}, Verne and {Sobeck}, Jennifer and {Spoo}, Taylor and {Sprague}, Dani and {Stark}, David V. and {Stassun}, Keivan G. and {Steinmetz}, Matthias and {Stello}, Dennis and {Stone-Martinez}, Alexander and {Storchi-Bergmann}, Thaisa and {Stringfellow}, Guy S. and {Stutz}, Amelia and {Su}, Yung-Chau and {Taghizadeh-Popp}, Manuchehr and {Talbot}, Michael S. and {Tayar}, Jamie and {Telles}, Eduardo and {Teske}, Johanna and {Thakar}, Ani and {Theissen}, Christopher and {Tkachenko}, Andrew and {Thomas}, Daniel and {Tojeiro}, Rita and {Hernandez Toledo}, Hector and {Troup}, Nicholas W. and {Trump}, Jonathan R. and {Trussler}, James and {Turner}, Jacqueline and {Tuttle}, Sarah and {Unda-Sanzana}, Eduardo and {V{\'a}zquez-Mata}, Jos{\'e} Antonio and {Valentini}, Marica and {Valenzuela}, Octavio and {Vargas-Gonz{\'a}lez}, Jaime and {Vargas-Maga{\~n}a}, Mariana and {Alfaro}, Pablo Vera and {Villanova}, Sandro and {Vincenzo}, Fiorenzo and {Wake}, David and {Warfield}, Jack T. and {Washington}, Jessica Diane and {Weaver}, Benjamin Alan and {Weijmans}, Anne-Marie and {Weinberg}, David H. and {Weiss}, Achim and {Westfall}, Kyle B. and {Wild}, Vivienne and {Wilde}, Matthew C. and {Wilson}, John C. and {Wilson}, Robert F. and {Wilson}, Mikayla and {Wolf}, Julien and {Wood-Vasey}, W.~M. and {Yan}, Renbin and {Zamora}, Olga and {Zasowski}, Gail and {Zhang}, Kai and {Zhao}, Cheng and {Zheng}, Zheng and {Zheng}, Zheng and {Zhu}, Kai},
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
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% From: https://www.sdss4.org/collaboration/citing-sdss/

Funding for the Sloan Digital Sky Survey IV has been provided by the Alfred P. Sloan Foundation, the U.S. Department of Energy Office of Science, and the Participating Institutions. SDSS acknowledges support and resources from the Center for High-Performance Computing at the University of Utah. The SDSS web site is www.sdss4.org.

SDSS is managed by the Astrophysical Research Consortium for the Participating Institutions of the SDSS Collaboration including the Brazilian Participation Group, the Carnegie Institution for Science, Carnegie Mellon University, Center for Astrophysics | Harvard & Smithsonian (CfA), the Chilean Participation Group, the French Participation Group, Instituto de Astrofísica de Canarias, The Johns Hopkins University, Kavli Institute for the Physics and Mathematics of the Universe (IPMU) / University of Tokyo, the Korean Participation Group, Lawrence Berkeley National Laboratory, Leibniz Institut für Astrophysik Potsdam (AIP), Max-Planck-Institut für Astronomie (MPIA Heidelberg), Max-Planck-Institut für Astrophysik (MPA Garching), Max-Planck-Institut für Extraterrestrische Physik (MPE), National Astronomical Observatories of China, New Mexico State University, New York University, University of Notre Dame, Observatório Nacional / MCTI, The Ohio State University, Pennsylvania State University, Shanghai Astronomical Observatory, United Kingdom Participation Group, Universidad Nacional Autónoma de México, University of Arizona, University of Colorado Boulder, University of Oxford, University of Portsmouth, University of Utah, University of Virginia, University of Washington, University of Wisconsin, Vanderbilt University, and Yale University.

In addition, the appropriate SDSS acknowledgment(s) for the survey and data releases that were used should be included in the Acknowledgments section: 

Funding for the Sloan Digital Sky Survey IV has been provided by the 
Alfred P. Sloan Foundation, the U.S. Department of Energy Office of 
Science, and the Participating Institutions. 

SDSS-IV acknowledges support and resources from the Center for High 
Performance Computing  at the University of Utah. The SDSS 
website is www.sdss4.org.

SDSS-IV is managed by the Astrophysical Research Consortium 
for the Participating Institutions of the SDSS Collaboration including 
the Brazilian Participation Group, the Carnegie Institution for Science, 
Carnegie Mellon University, Center for Astrophysics | Harvard \& 
Smithsonian, the Chilean Participation Group, the French Participation Group, 
Instituto de Astrof\'isica de Canarias, The Johns Hopkins 
University, Kavli Institute for the Physics and Mathematics of the 
Universe (IPMU) / University of Tokyo, the Korean Participation Group, 
Lawrence Berkeley National Laboratory, Leibniz Institut f\"ur Astrophysik 
Potsdam (AIP),  Max-Planck-Institut f\"ur Astronomie (MPIA Heidelberg), 
Max-Planck-Institut f\"ur Astrophysik (MPA Garching), 
Max-Planck-Institut f\"ur Extraterrestrische Physik (MPE), 
National Astronomical Observatories of China, New Mexico State University, 
New York University, University of Notre Dame, Observat\'ario 
Nacional / MCTI, The Ohio State University, Pennsylvania State 
University, Shanghai Astronomical Observatory, United 
Kingdom Participation Group, Universidad Nacional Aut\'onoma 
de M\'exico, University of Arizona, University of Colorado Boulder, 
University of Oxford, University of Portsmouth, University of Utah, 
University of Virginia, University of Washington, University of 
Wisconsin, Vanderbilt University, and Yale University.
"""

ACKNOWLEDGEMENTS = "\n".join([f"% {line}" for line in _ACKNOWLEDGEMENTS.split("\n")])

_DESCRIPTION = """\
Spectra dataset based on SDSS-IV.
"""

_HOMEPAGE = "https://www.sdss.org/"

_LICENSE = ""

_VERSION = "1.0.0"

# Full list of features available here:
# https://data.sdss.org/datamodel/files/SPECTRO_REDUX/specObj.html
_FLOAT_FEATURES = [
    "VDISP",
    "VDISP_ERR",
    "Z",
    "Z_ERR",
]

# Features that correspond to ugriz fluxes
_FLUX_FEATURES = [
    "SPECTROFLUX",
    "SPECTROFLUX_IVAR",
    "SPECTROSYNFLUX",
    "SPECTROSYNFLUX_IVAR",
]

_BOOL_FEATURES = [
    "ZWARNING"
]

class SDSS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="all",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["*/healpix=*/*.hdf5"]}
            ),
            description="All SDSS-IV spectra.",
        ),
        datasets.BuilderConfig(
            name="sdss",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["sdss/healpix=*/*.hdf5"]}
            ),
            description="SDSS Legacy survey spectra.",
        ),
        datasets.BuilderConfig(
            name="segue1",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["segue1/healpix=*/*.hdf5"]}
            ),
            description="SEGUE-1 spectra.",
        ),
        datasets.BuilderConfig(
            name="segue2",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["segue2/healpix=*/*.hdf5"]}
            ),
            description="SEGUE-2 spectra.",
        ),
        datasets.BuilderConfig(
            name="boss",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["boss/healpix=*/*.hdf5"]}
            ),
            description="BOSS spectra.",
        ),
        datasets.BuilderConfig(
            name="eboss",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["eboss/healpix=*/*.hdf5"]}
            ),
            description="eBOSS spectra.",
        )
    ]

    DEFAULT_CONFIG_NAME = "all"

    _flux_filters = ['U', 'G', 'R', 'I', 'Z']

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
            })
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding all boolean flags
        for f in _BOOL_FEATURES:
            features[f] = Value("bool")

        # Adding all flux values from the catalog
        for f in _FLUX_FEATURES:
            for b in self._flux_filters:
                features[f"{f}_{b}"] = Value("float32")
        
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
                            "flux": data["spectrum_flux"][i].reshape([-1,1]),
                            "ivar": data["spectrum_ivar"][i].reshape([-1,1]),
                            "lsf_sigma": data["spectrum_lsf_sigma"][i].reshape([-1,1]),
                            "lambda": data["spectrum_lambda"][i].reshape([-1,1]),
                            "mask": data["spectrum_mask"][i].reshape([-1,1]),
                        }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32").newbyteorder('=')

                    # Add all other requested features
                    for f in _FLUX_FEATURES:
                        for n, b in enumerate(self._flux_filters):
                            example[f"{f}_{b}"] = data[f"{f}"][i][n].astype("float32").newbyteorder('=')

                    # Add all boolean flags
                    for f in _BOOL_FEATURES:
                        example[f] = bool(data[f][i])

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
