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
from datasets import Features, Value, Array2D, Sequence, Image
from datasets.data_files import DataFilesPatternsDict
import h5py
import numpy as np

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@ARTICLE{2019AJ....157..168D,
       author = {{Dey}, Arjun and {Schlegel}, David J. and {Lang}, Dustin and {Blum}, Robert and {Burleigh}, Kaylan and {Fan}, Xiaohui and {Findlay}, Joseph R. and {Finkbeiner}, Doug and {Herrera}, David and {Juneau}, St{\'e}phanie and {Landriau}, Martin and {Levi}, Michael and {McGreer}, Ian and {Meisner}, Aaron and {Myers}, Adam D. and {Moustakas}, John and {Nugent}, Peter and {Patej}, Anna and {Schlafly}, Edward F. and {Walker}, Alistair R. and {Valdes}, Francisco and {Weaver}, Benjamin A. and {Y{\`e}che}, Christophe and {Zou}, Hu and {Zhou}, Xu and {Abareshi}, Behzad and {Abbott}, T.~M.~C. and {Abolfathi}, Bela and {Aguilera}, C. and {Alam}, Shadab and {Allen}, Lori and {Alvarez}, A. and {Annis}, James and {Ansarinejad}, Behzad and {Aubert}, Marie and {Beechert}, Jacqueline and {Bell}, Eric F. and {BenZvi}, Segev Y. and {Beutler}, Florian and {Bielby}, Richard M. and {Bolton}, Adam S. and {Brice{\~n}o}, C{\'e}sar and {Buckley-Geer}, Elizabeth J. and {Butler}, Karen and {Calamida}, Annalisa and {Carlberg}, Raymond G. and {Carter}, Paul and {Casas}, Ricard and {Castander}, Francisco J. and {Choi}, Yumi and {Comparat}, Johan and {Cukanovaite}, Elena and {Delubac}, Timoth{\'e}e and {DeVries}, Kaitlin and {Dey}, Sharmila and {Dhungana}, Govinda and {Dickinson}, Mark and {Ding}, Zhejie and {Donaldson}, John B. and {Duan}, Yutong and {Duckworth}, Christopher J. and {Eftekharzadeh}, Sarah and {Eisenstein}, Daniel J. and {Etourneau}, Thomas and {Fagrelius}, Parker A. and {Farihi}, Jay and {Fitzpatrick}, Mike and {Font-Ribera}, Andreu and {Fulmer}, Leah and {G{\"a}nsicke}, Boris T. and {Gaztanaga}, Enrique and {George}, Koshy and {Gerdes}, David W. and {Gontcho}, Satya Gontcho A. and {Gorgoni}, Claudio and {Green}, Gregory and {Guy}, Julien and {Harmer}, Diane and {Hernandez}, M. and {Honscheid}, Klaus and {Huang}, Lijuan Wendy and {James}, David J. and {Jannuzi}, Buell T. and {Jiang}, Linhua and {Joyce}, Richard and {Karcher}, Armin and {Karkar}, Sonia and {Kehoe}, Robert and {Kneib}, Jean-Paul and {Kueter-Young}, Andrea and {Lan}, Ting-Wen and {Lauer}, Tod R. and {Le Guillou}, Laurent and {Le Van Suu}, Auguste and {Lee}, Jae Hyeon and {Lesser}, Michael and {Perreault Levasseur}, Laurence and {Li}, Ting S. and {Mann}, Justin L. and {Marshall}, Robert and {Mart{\'\i}nez-V{\'a}zquez}, C.~E. and {Martini}, Paul and {du Mas des Bourboux}, H{\'e}lion and {McManus}, Sean and {Meier}, Tobias Gabriel and {M{\'e}nard}, Brice and {Metcalfe}, Nigel and {Mu{\~n}oz-Guti{\'e}rrez}, Andrea and {Najita}, Joan and {Napier}, Kevin and {Narayan}, Gautham and {Newman}, Jeffrey A. and {Nie}, Jundan and {Nord}, Brian and {Norman}, Dara J. and {Olsen}, Knut A.~G. and {Paat}, Anthony and {Palanque-Delabrouille}, Nathalie and {Peng}, Xiyan and {Poppett}, Claire L. and {Poremba}, Megan R. and {Prakash}, Abhishek and {Rabinowitz}, David and {Raichoor}, Anand and {Rezaie}, Mehdi and {Robertson}, A.~N. and {Roe}, Natalie A. and {Ross}, Ashley J. and {Ross}, Nicholas P. and {Rudnick}, Gregory and {Safonova}, Sasha and {Saha}, Abhijit and {S{\'a}nchez}, F. Javier and {Savary}, Elodie and {Schweiker}, Heidi and {Scott}, Adam and {Seo}, Hee-Jong and {Shan}, Huanyuan and {Silva}, David R. and {Slepian}, Zachary and {Soto}, Christian and {Sprayberry}, David and {Staten}, Ryan and {Stillman}, Coley M. and {Stupak}, Robert J. and {Summers}, David L. and {Sien Tie}, Suk and {Tirado}, H. and {Vargas-Maga{\~n}a}, Mariana and {Vivas}, A. Katherina and {Wechsler}, Risa H. and {Williams}, Doug and {Yang}, Jinyi and {Yang}, Qian and {Yapici}, Tolga and {Zaritsky}, Dennis and {Zenteno}, A. and {Zhang}, Kai and {Zhang}, Tianmeng and {Zhou}, Rongpu and {Zhou}, Zhimin},
        title = "{Overview of the DESI Legacy Imaging Surveys}",
      journal = {\aj},
     keywords = {catalogs, surveys, Astrophysics - Instrumentation and Methods for Astrophysics},
         year = 2019,
        month = may,
       volume = {157},
       number = {5},
          eid = {168},
        pages = {168},
          doi = {10.3847/1538-3881/ab089d},
archivePrefix = {arXiv},
       eprint = {1804.08657},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2019AJ....157..168D},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
Data Release 10 (DR10) is the tenth public data release of the Legacy Surveys.

When using data from the Legacy Surveys in papers, please use the following acknowledgment:

The Legacy Surveys consist of three individual and complementary projects: the Dark Energy Camera Legacy Survey (DECaLS; Proposal ID #2014B-0404; PIs: David Schlegel and Arjun Dey), the Beijing-Arizona Sky Survey (BASS; NOAO Prop. ID #2015A-0801; PIs: Zhou Xu and Xiaohui Fan), and the Mayall z-band Legacy Survey (MzLS; Prop. ID #2016A-0453; PI: Arjun Dey). DECaLS, BASS and MzLS together include data obtained, respectively, at the Blanco telescope, Cerro Tololo Inter-American Observatory, NSF’s NOIRLab; the Bok telescope, Steward Observatory, University of Arizona; and the Mayall telescope, Kitt Peak National Observatory, NOIRLab. Pipeline processing and analyses of the data were supported by NOIRLab and the Lawrence Berkeley National Laboratory (LBNL). The Legacy Surveys project is honored to be permitted to conduct astronomical research on Iolkam Du’ag (Kitt Peak), a mountain with particular significance to the Tohono O’odham Nation.

NOIRLab is operated by the Association of Universities for Research in Astronomy (AURA) under a cooperative agreement with the National Science Foundation. LBNL is managed by the Regents of the University of California under contract to the U.S. Department of Energy.

This project used data obtained with the Dark Energy Camera (DECam), which was constructed by the Dark Energy Survey (DES) collaboration. Funding for the DES Projects has been provided by the U.S. Department of Energy, the U.S. National Science Foundation, the Ministry of Science and Education of Spain, the Science and Technology Facilities Council of the United Kingdom, the Higher Education Funding Council for England, the National Center for Supercomputing Applications at the University of Illinois at Urbana-Champaign, the Kavli Institute of Cosmological Physics at the University of Chicago, Center for Cosmology and Astro-Particle Physics at the Ohio State University, the Mitchell Institute for Fundamental Physics and Astronomy at Texas A&M University, Financiadora de Estudos e Projetos, Fundacao Carlos Chagas Filho de Amparo, Financiadora de Estudos e Projetos, Fundacao Carlos Chagas Filho de Amparo a Pesquisa do Estado do Rio de Janeiro, Conselho Nacional de Desenvolvimento Cientifico e Tecnologico and the Ministerio da Ciencia, Tecnologia e Inovacao, the Deutsche Forschungsgemeinschaft and the Collaborating Institutions in the Dark Energy Survey. The Collaborating Institutions are Argonne National Laboratory, the University of California at Santa Cruz, the University of Cambridge, Centro de Investigaciones Energeticas, Medioambientales y Tecnologicas-Madrid, the University of Chicago, University College London, the DES-Brazil Consortium, the University of Edinburgh, the Eidgenossische Technische Hochschule (ETH) Zurich, Fermi National Accelerator Laboratory, the University of Illinois at Urbana-Champaign, the Institut de Ciencies de l’Espai (IEEC/CSIC), the Institut de Fisica d’Altes Energies, Lawrence Berkeley National Laboratory, the Ludwig Maximilians Universitat Munchen and the associated Excellence Cluster Universe, the University of Michigan, NSF’s NOIRLab, the University of Nottingham, the Ohio State University, the University of Pennsylvania, the University of Portsmouth, SLAC National Accelerator Laboratory, Stanford University, the University of Sussex, and Texas A&M University.

BASS is a key project of the Telescope Access Program (TAP), which has been funded by the National Astronomical Observatories of China, the Chinese Academy of Sciences (the Strategic Priority Research Program “The Emergence of Cosmological Structures” Grant # XDB09000000), and the Special Fund for Astronomy from the Ministry of Finance. The BASS is also supported by the External Cooperation Program of Chinese Academy of Sciences (Grant # 114A11KYSB20160057), and Chinese National Natural Science Foundation (Grant # 12120101003, # 11433005).

The Legacy Survey team makes use of data products from the Near-Earth Object Wide-field Infrared Survey Explorer (NEOWISE), which is a project of the Jet Propulsion Laboratory/California Institute of Technology. NEOWISE is funded by the National Aeronautics and Space Administration.

The Legacy Surveys imaging of the DESI footprint is supported by the Director, Office of Science, Office of High Energy Physics of the U.S. Department of Energy under Contract No. DE-AC02-05CH1123, by the National Energy Research Scientific Computing Center, a DOE Office of Science User Facility under the same contract; and by the U.S. National Science Foundation, Division of Astronomical Sciences under Contract No. AST-0950945 to NOAO.
"""

# You can copy an official description
_DESCRIPTION = """\
Image dataset from Legacy Survey DR10
"""

_HOMEPAGE = "https://www.legacysurvey.org/dr10/"

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"

_FLOAT_FEATURES = [
    "EBV",
    "FLUX_G",
    "FLUX_R",
    "FLUX_I",
    "FLUX_Z",
    "FLUX_W1",
    "FLUX_W2",
    "FLUX_W3",
    "FLUX_W4",
    "SHAPE_R",
    "SHAPE_E1",
    "SHAPE_E2",
]

CATALOG_FEATURES = [
    "FLUX_G",
    "FLUX_R",
    "FLUX_I",
    "FLUX_Z",
    "TYPE",
    "SHAPE_R",
    "SHAPE_E1",
    "SHAPE_E2",
    "X",
    "Y",
]


class DECaLS(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="LegacySurvey",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["*/healpix=*/*.hdf5"]}
            ),
            description="LegacySurvey DR10 images.",
        ),        
        datasets.BuilderConfig(name="dr10_south_21", 
                                version=VERSION, 
                                data_files=DataFilesPatternsDict.from_patterns({'train': ['dr10_south_21/healpix=*/*.hdf5']}),
                                description="DR10 images from the southern sky, down to zmag 21"),
    ]

    DEFAULT_CONFIG_NAME = "LegacySurvey"

    _pixel_scale = 0.262
    _image_size = 160
    _bands = ['DES-G', 'DES-R', 'DES-I', 'DES-Z']

    @classmethod
    def _info(self):
        """ Defines the features available in this dataset.
        """
        # Starting with all features common to image datasets
        features = {
            "image": Sequence(
                feature={
                    "band": Value("string"),
                    "array": Array2D(
                        shape=(self._image_size, self._image_size), dtype="float32"
                    ),
                    "mask": Array2D(
                        shape=(self._image_size, self._image_size), dtype="bool"
                    ),
                    "ivar": Array2D(
                        shape=(self._image_size, self._image_size), dtype="float32"
                    ),
                    "psf_fwhm": Value("float32"),
                    "scale": Value("float32"),
                }
            ),
            "blobmodel": Image(),
            "rgb": Image(),
            "object_mask": Image(),
            "catalog": Sequence(
                feature={f: Value("float32") for f in CATALOG_FEATURES}
            ),
        }
        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
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
                    # Parse image data
                    example = {
                        "image": [
                            {
                                "band": data["image_band"][i][j].decode("utf-8"),
                                "array": data["image_array"][i][j],
                                "mask": data["image_mask"][i],
                                "ivar": data["image_ivar"][i][j],
                                "psf_fwhm": data["image_psf_fwhm"][i][j],
                                "scale": data["image_scale"][i][j],
                            }
                            for j, _ in enumerate(self._bands)
                        ],
                        "blobmodel": data["blobmodel"][i],
                        "rgb": data["image_rgb"][i],
                        "object_mask": data["object_mask"][i],
                        "catalog": {
                            key: data[f"catalog_{key}"][i] for key in CATALOG_FEATURES
                        },
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype('float32')
                    
                    # Add object type
                    example['TYPE'] = data['TYPE'][i].decode('utf-8')

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data['object_id'][i]), example