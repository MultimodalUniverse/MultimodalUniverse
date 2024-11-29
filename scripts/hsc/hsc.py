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

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@article{Aihara_2017,
   title={The Hyper Suprime-Cam SSP Survey: Overview and survey design},
   volume={70},
   ISSN={2053-051X},
   url={http://dx.doi.org/10.1093/pasj/psx066},
   DOI={10.1093/pasj/psx066},
   number={SP1},
   journal={Publications of the Astronomical Society of Japan},
   publisher={Oxford University Press (OUP)},
   author={Aihara, Hiroaki and Arimoto, Nobuo and Armstrong, Robert and Arnouts, Stéphane and Bahcall, Neta A and Bickerton, Steven and Bosch, James and Bundy, Kevin and Capak, Peter L and Chan, James H H and Chiba, Masashi and Coupon, Jean and Egami, Eiichi and Enoki, Motohiro and Finet, Francois and Fujimori, Hiroki and Fujimoto, Seiji and Furusawa, Hisanori and Furusawa, Junko and Goto, Tomotsugu and Goulding, Andy and Greco, Johnny P and Greene, Jenny E and Gunn, James E and Hamana, Takashi and Harikane, Yuichi and Hashimoto, Yasuhiro and Hattori, Takashi and Hayashi, Masao and Hayashi, Yusuke and Hełminiak, Krzysztof G and Higuchi, Ryo and Hikage, Chiaki and Ho, Paul T P and Hsieh, Bau-Ching and Huang, Kuiyun and Huang, Song and Ikeda, Hiroyuki and Imanishi, Masatoshi and Inoue, Akio K and Iwasawa, Kazushi and Iwata, Ikuru and Jaelani, Anton T and Jian, Hung-Yu and Kamata, Yukiko and Karoji, Hiroshi and Kashikawa, Nobunari and Katayama, Nobuhiko and Kawanomoto, Satoshi and Kayo, Issha and Koda, Jin and Koike, Michitaro and Kojima, Takashi and Komiyama, Yutaka and Konno, Akira and Koshida, Shintaro and Koyama, Yusei and Kusakabe, Haruka and Leauthaud, Alexie and Lee, Chien-Hsiu and Lin, Lihwai and Lin, Yen-Ting and Lupton, Robert H and Mandelbaum, Rachel and Matsuoka, Yoshiki and Medezinski, Elinor and Mineo, Sogo and Miyama, Shoken and Miyatake, Hironao and Miyazaki, Satoshi and Momose, Rieko and More, Anupreeta and More, Surhud and Moritani, Yuki and Moriya, Takashi J and Morokuma, Tomoki and Mukae, Shiro and Murata, Ryoma and Murayama, Hitoshi and Nagao, Tohru and Nakata, Fumiaki and Niida, Mana and Niikura, Hiroko and Nishizawa, Atsushi J and Obuchi, Yoshiyuki and Oguri, Masamune and Oishi, Yukie and Okabe, Nobuhiro and Okamoto, Sakurako and Okura, Yuki and Ono, Yoshiaki and Onodera, Masato and Onoue, Masafusa and Osato, Ken and Ouchi, Masami and Price, Paul A and Pyo, Tae-Soo and Sako, Masao and Sawicki, Marcin and Shibuya, Takatoshi and Shimasaku, Kazuhiro and Shimono, Atsushi and Shirasaki, Masato and Silverman, John D and Simet, Melanie and Speagle, Joshua and Spergel, David N and Strauss, Michael A and Sugahara, Yuma and Sugiyama, Naoshi and Suto, Yasushi and Suyu, Sherry H and Suzuki, Nao and Tait, Philip J and Takada, Masahiro and Takata, Tadafumi and Tamura, Naoyuki and Tanaka, Manobu M and Tanaka, Masaomi and Tanaka, Masayuki and Tanaka, Yoko and Terai, Tsuyoshi and Terashima, Yuichi and Toba, Yoshiki and Tominaga, Nozomu and Toshikawa, Jun and Turner, Edwin L and Uchida, Tomohisa and Uchiyama, Hisakazu and Umetsu, Keiichi and Uraguchi, Fumihiro and Urata, Yuji and Usuda, Tomonori and Utsumi, Yousuke and Wang, Shiang-Yu and Wang, Wei-Hao and Wong, Kenneth C and Yabe, Kiyoto and Yamada, Yoshihiko and Yamanoi, Hitomi and Yasuda, Naoki and Yeh, Sherry and Yonehara, Atsunori and Yuma, Suraphong},
   year={2017},
   month=sep
}

@article{Aihara_2022,
   title={Third data release of the Hyper Suprime-Cam Subaru Strategic Program},
   volume={74},
   ISSN={2053-051X},
   url={http://dx.doi.org/10.1093/pasj/psab122},
   DOI={10.1093/pasj/psab122},
   number={2},
   journal={Publications of the Astronomical Society of Japan},
   publisher={Oxford University Press (OUP)},
   author={Aihara, Hiroaki and AlSayyad, Yusra and Ando, Makoto and Armstrong, Robert and Bosch, James and Egami, Eiichi and Furusawa, Hisanori and Furusawa, Junko and Harasawa, Sumiko and Harikane, Yuichi and Hsieh, Bau-Ching and Ikeda, Hiroyuki and Ito, Kei and Iwata, Ikuru and Kodama, Tadayuki and Koike, Michitaro and Kokubo, Mitsuru and Komiyama, Yutaka and Li, Xiangchong and Liang, Yongming and Lin, Yen-Ting and Lupton, Robert H and Lust, Nate B and MacArthur, Lauren A and Mawatari, Ken and Mineo, Sogo and Miyatake, Hironao and Miyazaki, Satoshi and More, Surhud and Morishima, Takahiro and Murayama, Hitoshi and Nakajima, Kimihiko and Nakata, Fumiaki and Nishizawa, Atsushi J and Oguri, Masamune and Okabe, Nobuhiro and Okura, Yuki and Ono, Yoshiaki and Osato, Ken and Ouchi, Masami and Pan, Yen-Chen and Plazas Malagón, Andrés A and Price, Paul A and Reed, Sophie L and Rykoff, Eli S and Shibuya, Takatoshi and Simunovic, Mirko and Strauss, Michael A and Sugimori, Kanako and Suto, Yasushi and Suzuki, Nao and Takada, Masahiro and Takagi, Yuhei and Takata, Tadafumi and Takita, Satoshi and Tanaka, Masayuki and Tang, Shenli and Taranu, Dan S and Terai, Tsuyoshi and Toba, Yoshiki and Turner, Edwin L and Uchiyama, Hisakazu and Vijarnwannaluk, Bovornpratch and Waters, Christopher Z and Yamada, Yoshihiko and Yamamoto, Naoaki and Yamashita, Takuji},
   year={2022},
   month=feb, pages={247–272}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
Materials on this website, including images from hscMap, can be used without prior permission within the following scopes.

Extent of free use stipulated by Japanese copyright law (private use, educational use, news reporting, etc.)
Usage in academic research, education, and learning activities
Usage by news organizations
Usage in printed media
Usage in websites and social networks

In all the cases, please explicitly include the credit, “NAOJ / HSC Collaboration“.
See also the following web page about “Guide to Using NAOJ Website Copyrighted Materials” for details.
https://www.nao.ac.jp/en/terms/copyright.html (in English)
"""

# TODO: Add description of the dataset here
# You can copy an official description
_DESCRIPTION = """\
Image dataset based on HSC SSP PRD3.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = "https://hsc-release.mtk.nao.ac.jp/doc/"

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = "See Acknowledgements / Citation"

_VERSION = "0.0.1"

_FLOAT_FEATURES = [
    'a_g',
    'a_r',
    'a_i',
    'a_z',
    'a_y',
    'g_extendedness_value',
    'r_extendedness_value',
    'i_extendedness_value',
    'z_extendedness_value',
    'y_extendedness_value',
    # 'g_variance_value',
    # 'r_variance_value',
    # 'i_variance_value',
    # 'z_variance_value',
    # 'y_variance_value',
    'g_cmodel_mag',
    'g_cmodel_magerr',
    'r_cmodel_mag',
    'r_cmodel_magerr',
    'i_cmodel_mag',
    'i_cmodel_magerr',
    'z_cmodel_mag',
    'z_cmodel_magerr',
    'y_cmodel_mag',
    'y_cmodel_magerr',
    # 'g_cmodel_flux',
    # 'g_cmodel_fluxerr',
    # 'r_cmodel_flux',
    # 'r_cmodel_fluxerr',
    # 'i_cmodel_flux',
    # 'i_cmodel_fluxerr',
    # 'z_cmodel_flux',
    # 'z_cmodel_fluxerr',
    # 'y_cmodel_flux',
    # 'y_cmodel_fluxerr',
    'g_sdssshape_psf_shape11',
    'g_sdssshape_psf_shape22',
    'g_sdssshape_psf_shape12',
    'r_sdssshape_psf_shape11',
    'r_sdssshape_psf_shape22',
    'r_sdssshape_psf_shape12',
    'i_sdssshape_psf_shape11',
    'i_sdssshape_psf_shape22',
    'i_sdssshape_psf_shape12',
    'z_sdssshape_psf_shape11',
    'z_sdssshape_psf_shape22',
    'z_sdssshape_psf_shape12',
    'y_sdssshape_psf_shape11',
    'y_sdssshape_psf_shape22',
    'y_sdssshape_psf_shape12',
    'g_sdssshape_shape11',
    'g_sdssshape_shape22',
    'g_sdssshape_shape12',
    'r_sdssshape_shape11',
    'r_sdssshape_shape22',
    'r_sdssshape_shape12',
    'i_sdssshape_shape11',
    'i_sdssshape_shape22',
    'i_sdssshape_shape12',
    'z_sdssshape_shape11',
    'z_sdssshape_shape22',
    'z_sdssshape_shape12',
    'y_sdssshape_shape11',
    'y_sdssshape_shape22',
    'y_sdssshape_shape12'
    ]


class HSC(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(name="pdr3_dud_22.5", 
                               version=VERSION, 
                               data_files=DataFilesPatternsDict.from_patterns({'train': ['pdr3_dud_22.5/healpix=*/*.hdf5']}),
                               description="Deep / Ultra Deep sample from PDR3 up to 22.5 imag."),
    ]

    DEFAULT_CONFIG_NAME = "pdr3_dud_22.5"

    _image_size = 160

    _bands = ['G', 'R', 'I', 'Z', 'Y']

    @classmethod
    def _info(self):
        """ Defines the features available in this dataset.
        """
        # Starting with all features common to image datasets
        features = {
            'image': Sequence(feature={
                'band': Value('string'),
                'flux': Array2D(shape=(self._image_size, self._image_size), dtype='float32'),
                'ivar': Array2D(shape=(self._image_size, self._image_size), dtype='float32'),
                'mask': Array2D(shape=(self._image_size, self._image_size), dtype='bool'),
                'psf_fwhm': Value('float32'),
                'scale': Value('float32'),
            })
        }
        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value('float32')

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
                    # Parse image data
                    example = {'image':  [{'band': data['image_band'][i][j].decode('utf-8'),
                               'flux': data['image_array'][i][j],
                               'ivar': data['image_ivar'][i][j],
                               'mask': data['image_mask'][i][j],
                               'psf_fwhm': data['image_psf_fwhm'][i][j],
                               'scale': data['image_scale'][i][j]} for j, _ in enumerate( self._bands )]
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype('float32')
                    
                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data['object_id'][i]), example