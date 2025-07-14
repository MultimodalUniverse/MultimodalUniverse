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

_CITATION = r"""% CITATION
@ARTICLE{2012RAA....12.1197C,
       author = {{Cui}, Xiang-Qun and {Zhao}, Yong-Heng and {Chu}, Yao-Quan and {Li}, Guo-Ping and {Li}, Qi and {Zhang}, Li-Ping and {Su}, Hong-Jun and {Yao}, Zheng-Qiu and {Wang}, Ya-Nan and {Xing}, Xiao-Zheng and {Li}, Xin-Nan and {Zhu}, Yong-Tian and {Wang}, Guang and {Feng}, Bao-Zhong and {Gong}, Bao-Zhong and {Hou}, Yong-Hui and {Zhu}, Zi-Huang and {Lu}, Hao-Tong and {Shi}, Jian-Rong and {Luo}, A-Li and {Shang}, Zhen-Hai and {Brandt}, Nils and {Schneider}, Donald P. and {Zhu}, Yong-Tian and {Zhao}, Jing-Kun and {Zhao}, Gang and {Jia}, Kai and {Li}, Yong-Chun and {Zhang}, Jie and {Zhang}, Shu-Qing and {Cao}, Zi-Huang and {Qiu}, Ya-Lin and {Yao}, Yu-Qing and {Deng}, Li-Cai and {Newberg}, Heidi Jo and {Willett}, Benjamin A. and {Yanny}, Brian and {Xie}, Xiao-Wei and {Heber}, Ulrich and {Edelmann}, Heinz and {Napiwotzki}, Ralf and {Altmann}, Martin and {Scholz}, Ralf-Dieter and {Suntzeff}, Nicholas B. and {Inada}, Naohisa and {Ebizuka}, Nobunari and {Ge}, Jian and {Zhao}, Huawei and {Zhang}, Ye and {Adelman-McCarthy}, Jennifer K. and {Allam}, Sahar S. and {Prieto}, Carlos Allende and {An}, Deokkeun and {Anderson}, Kurt S.~J. and {Anderson}, Scott F. and {Annis}, James and {Bahcall}, Neta A. and {Bailer-Jones}, Coryn A.~L. and {Barentine}, John C. and {Bassett}, Bruce A. and {Becker}, Andrew C. and {Beers}, Timothy C. and {Bell}, Eric F. and {Belokurov}, Vasily and {Berlind}, Andreas A. and {Berman}, Edo F. and {Bernardi}, Mariangela and {Bickerton}, Steven J. and {Bizyaev}, Dmitry and {Bland-Hawthorn}, Joss and {Blanton}, Michael R. and {Bochanski}, John J. and {Bolton}, Adam S. and {Bovy}, Jo and {Brandt}, W.~N. and {Brinkmann}, Jon and {Brown}, Peter J. and {Brownstein}, Joel R. and {Burger}, Dan and {Busca}, Nicolas G. and {Campbell}, Heather and {Carr}, Michael A. and {Chen}, Yanmei and {Chiappini}, Cristina and {Chojnowski}, S. Drew and {Chuang}, Chia-Hsun and {Clemens}, J. Christopher and {Colless}, Matthew and {Connolly}, Andrew J. and {Cooke}, Jeff and {Cooray}, Asantha and {Covey}, Kevin R. and {Croft}, Rupert A.~C. and {Cuesta}, Antonio J. and {da Costa}, Luiz N. and {Davenport}, James R.~A. and {Dawson}, Kyle and {de Lee}, Nathan and {de Mello}, Duilia F. and {Delubac}, Timoth{\'e}e and {Dhital}, Saurav and {Ealet}, Anne and {Ebelke}, Garrett L. and {Edmondson}, Edward M. and {Eiting}, Daniel J. and {Escoffier}, Stephanie and {Esposito}, Massimiliano and {Evans}, Michael L. and {Fan}, Xiaohui and {Femen{\'i}a Castell{\'a}}, Bel{\'e}n and {Ferreira}, Leticia Dutra and {Fitzgerald}, Greg and {Fleming}, Scott W. and {Font-Ribera}, Andreu and {Ford}, Eric B. and {Foreman-Mackey}, Daniel and {Frinchaboy}, Peter M. and {Fuentalba}, Dionisio A. and {Fukugita}, Masataka and {Gaensicke}, Boris T. and {Gallazzi}, Anna and {Gao}, Liang and {Garc{\'i}a P{\'e}rez}, Ana E. and {Garc{\'i}a-Hern{\'a}ndez}, D. An{\'\i}bal and {Gaulme}, Patrick and {Ge}, Jian and {Gillespie}, Bruce A. and {Gilmore}, Gerard F. and {Gonzalez-Perez}, Violeta and {Gott}, J. Richard and {Gould}, Andrew and {Grebel}, Eva K. and {Gunn}, James E. and {Guo}, Hong and {Harding}, Paul and {Harris}, David W. and {Hawley}, Suzanne L. and {Hearty}, Frederick R. and {Hinton}, Samuel and {Ho}, Shirley and {Hogg}, David W. and {Holtzman}, Jon A. and {Honscheid}, Klaus and {Hou}, Jinliang and {Hsieh}, Bau-Ching and {Hu}, Zhiping and {Ivans}, Inese I. and {Ivezi{\'c}}, {\v{Z}}eljko and {Jaehnig}, Kurt and {Jiang}, Linhua and {Johnson}, Jennifer A. and {Jordan}, Cathy and {Jordan}, Wendell P. and {Kauffmann}, Guinevere and {Kazin}, Eyal and {Kirkby}, David and {Klaene}, Mark A. and {Kneib}, Jean-Paul and {Knapp}, Gillian R. and {Kochanek}, Christopher S. and {Koesterke}, Lars and {Kollmeier}, Juna A. and {Kron}, Richard G. and {Lampeitl}, Hubert and {Lang}, Dustin and {Lawler}, James E. and {Le Goff}, Jean-Marc and {Lee}, Brian L. and {Lee}, Young Sun and {Leisenring}, Jarron M. and {Li}, Cheng and {Li}, Ning and {Lima}, Marcos and {Lin}, Yen-Ting and {Long}, Dan and {Loomis}, Craig P. and {Lupton}, Robert H. and {Ma}, Bo and {MacDonald}, Nicholas and {Madhusudhan}, Nikku and {Mahadevan}, Suvrath and {Maia}, Marcio A.~G. and {Majewski}, Steven R. and {Makler}, Mart{\'i}n and {Malanushenko}, Elena and {Malanushenko}, Viktor and {Mandelbaum}, Rachel and {Maraston}, Claudia and {Margala}, Daniel and {Maseman}, Paul and {Masters}, Karen L. and {McBride}, Cameron K. and {McGehee}, Peregrine M. and {McGreer}, Ian D. and {Medez}, Brice and {Mena}, Olga and {Miquel}, Ramon and {Montero-Dorta}, Antonio D. and {Montesano}, Francesco and {Morganson}, Eric and {Morrissey}, Patrick and {Morrison}, Heather L. and {Mullally}, Fergal and {Muna}, Demitri and {Myers}, Adam D. and {Naugle}, Tracy and {Neto}, Angelo Fausti and {Nguyen}, Duy Cuong and {Nichol}, Robert C. and {Nidever}, David L. and {Noterdaeme}, Pasquier and {Nunnally}, Sebastien E. and {Olmstead}, Matthew D. and {Oravetz}, Audrey and {Oravetz}, Daniel J. and {Osumi}, Keisuke and {Owen}, Russell and {Padilla}, Nelson and {Palanque-Delabrouille}, Nathalie and {Pan}, Kaike and {Parejko}, John K. and {P{\^a}ris}, Isabelle and {Park}, Changbom and {Pattarakijwanich}, Petchara and {Pellegrini}, Paulo and {Pepper}, Joshua and {Percival}, Will J. and {Petitjean}, Patrick and {Pfaffenberger}, Robert and {Pforr}, Janine and {Phleps}, Stefanie and {Pichon}, Christophe and {Pieres}, Adriano and {Pinsonneault}, Marc H. and {Pizagno}, James and {Pogge}, Richard W. and {Poleski}, Rados{\l}aw and {Prada}, Francisco and {Price-Whelan}, Adrian M. and {Raddick}, M. Jordan and {Ramos}, Beatriz H.~F. and {Reid}, I. Neill and {Rich}, James and {Richards}, Gordon T. and {Rieke}, George H. and {Rieke}, Marcia J. and {Rix}, Hans-Walter and {Robin}, Annie C. and {Rocha-Pinto}, Helio J. and {Rockosi}, Constance M. and {Roe}, Natalie A. and {Rollinde}, Emmanuel and {Ross}, Ashley J. and {Ross}, Nicholas P. and {Rossetto}, Bruno M. and {Ruan}, John J. and {Rykoff}, Eli S. and {Salim}, Samir and {Samushia}, Lado and {Sanchez}, Ariel G. and {Sayres}, Conor and {Schiavon}, Ricardo P. and {Schlegel}, David J. and {Schlesinger}, Katharine J. and {Schmidt}, Sarah J. and {Schneider}, Donald P. and {Schwope}, Axel D. and {Scott}, Cullen J. and {Sellgren}, Kimberly and {Sesar}, Branimir and {Shetrone}, Matthew and {Shu}, Yiping and {Silverman}, John D. and {Simmerer}, Jennifer and {Simmons}, Audrey E. and {Sivarani}, Thirupathi and {Skrutskie}, Michael F. and {Slosar}, An{\v{z}}e and {Smee}, Stephen and {Smith}, Verne V. and {Sobeck}, Jennifer S. and {Steinmetz}, Matthias and {Strauss}, Michael A. and {Streblyanska}, Alina and {Suzuki}, Nao and {Swanson}, Molly E.~C. and {Tan}, Xinyu and {Tayar}, Jamie and {Terrien}, Ryan C. and {Thakar}, Aniruddha R. and {Thomas}, Daniel and {Thompson}, Benjamin A. and {Tinker}, Jeremy L. and {Tojeiro}, Rita and {Troup}, Nicholas W. and {Trujillo-Gomez}, Sebastian and {Tucker}, Douglas L. and {Tumlinson}, Jason and {Valenzuela}, Octavio and {Vand der Marel}, Roeland P. and {Vargas-Maga{\~n}a}, Mariana and {Viel}, Matteo and {Vogt}, Nicole P. and {Wake}, David A. and {Wang}, Ji and {Weaver}, Benjamin A. and {Weinberg}, David H. and {Weiss}, Benjamin J. and {West}, Andrew A. and {White}, Martin and {Wilson}, John C. and {Wisniewski}, John P. and {Wood-Vasey}, W. Michael and {Yanny}, Brian and {Yche}, Christophe and {York}, Donald G. and {Young}, Elise and {Zasowski}, Gail and {Zehavi}, Idit and {Zhao}, Bo and {Zheng}, Zheng and {Zhu}, Guangyu and {Zinn}, Joel C. and {Zou}, Hu},
        title = "{The Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST)}",
      journal = {Research in Astronomy and Astrophysics},
     keywords = {surveys, instrumentation: spectrographs, techniques: spectroscopic},
         year = 2012,
        month = oct,
       volume = {12},
       number = {9},
        pages = {1197-1242},
          doi = {10.1088/1674-4527/12/9/003},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2012RAA....12.1197C},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2015ApJS..221....8Y,
       author = {{Yao}, Shuang and {Liu}, Chao and {Zhang}, Haotong and {Deng}, Licai and {Zhao}, Yongheng and {Carlin}, Jeffrey L. and {Newberg}, Heidi Jo and {Xiang}, Maosheng and {Zhang}, Yong and {Luo}, Ali},
        title = "{The LAMOST Survey of Background Quasars in the vicinity of the Andromeda and Triangulum Galaxies. I. Quasar Catalog}",
      journal = {\apjs},
     keywords = {galaxies: active, quasars: general, surveys, Astrophysics - Astrophysics of Galaxies},
         year = 2015,
        month = dec,
       volume = {221},
       number = {2},
          eid = {8},
        pages = {8},
          doi = {10.1088/0067-0049/221/2/8},
archivePrefix = {arXiv},
       eprint = {1506.08268},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2015ApJS..221....8Y},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
From the LAMOST website (http://www.lamost.org/):

The LAMOST (Large Sky Area Multi-Object Fiber Spectroscopic Telescope) is a 
National Major Scientific Project built by the Chinese Academy of Sciences.
Funding for the project has been provided by the National Development and Reform 
Commission. LAMOST is operated and managed by the National Astronomical 
Observatories, Chinese Academy of Sciences.

We acknowledge the use of data from the LAMOST (Large Sky Area Multi-Object 
Fiber Spectroscopic Telescope) survey. LAMOST is a National Major Scientific 
Project built by the Chinese Academy of Sciences. Funding for the project has 
been provided by the National Development and Reform Commission. LAMOST is 
operated and managed by the National Astronomical Observatories, Chinese 
Academy of Sciences.
"""

_DESCRIPTION = """\
The Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST) is a 
Chinese national scientific research facility operated by the National Astronomical 
Observatories, Chinese Academy of Sciences. LAMOST can observe about 4000 celestial 
objects simultaneously in each exposure with its 4000 optical fibers. The spectral 
resolution is R ~ 1800, and the wavelength coverage is 3700-9000 Å. This dataset 
contains optical spectra from the LAMOST survey with associated stellar parameters 
and classifications.
"""

_HOMEPAGE = "http://www.lamost.org/"

_LICENSE = "Custom - see LAMOST data policy"

_VERSION = "1.0.0"

_FLOAT_FEATURES = [
    "ra",
    "dec",
    "snr",
    "mag_u",
    "mag_g", 
    "mag_r",
    "mag_i",
    "mag_z",
    "teff",
    "logg",
    "feh",
    "rv",
    "teff_err",
    "logg_err",
    "feh_err",
    "rv_err"
]

_STRING_FEATURES = [
    "obsid",
    "designation",
    "class",
    "subclass"
]

_BOOL_FEATURES = [
    "restframe"
]


class LAMOST(datasets.GeneratorBasedBuilder):
    """
    Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST)
    """

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="lamost",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["./lamost/healpix=*/*.hdf5"]}
            ),
            description="LAMOST survey optical spectra.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "lamost"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to spectral dataset
        features = {
            "spectrum": Sequence(
                {
                    "flux": Value(dtype="float32"),
                    "ivar": Value(dtype="float32"),
                    "lambda": Value(dtype="float32"),
                    "mask": Value(dtype="bool"),
                    "norm_flux": Value(dtype="float32"),
                    "norm_ivar": Value(dtype="float32"),
                }
            )
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

        # Adding string features
        for f in _STRING_FEATURES:
            features[f] = Value("string")

        # Adding all boolean flags
        for f in _BOOL_FEATURES:
            features[f] = Value("bool")

        features["object_id"] = Value("string")

        # Format acknowledgements to have % at the beginning of each line
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
                            "flux": data["spectrum_flux"][i],
                            "ivar": data["spectrum_ivar"][i],
                            "lambda": data["spectrum_lambda"][i],
                            "mask": data["spectrum_mask"][i],
                            "norm_flux": data["spectrum_norm_flux"][i],
                            "norm_ivar": data["spectrum_norm_ivar"][i],
                        }
                    }
                    
                    # Add all float features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    # Add all string features  
                    for f in _STRING_FEATURES:
                        example[f] = str(data[f][i])

                    # Add all boolean features
                    for f in _BOOL_FEATURES:
                        example[f] = bool(data[f][i])

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example