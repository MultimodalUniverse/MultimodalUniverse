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
import h5py
import numpy as np
from datasets import Array2D, Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict

# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@ARTICLE{2023ApJ...947...20V,
       author = {{Valentino}, Francesco and {Brammer}, Gabriel and {Gould}, Katriona M.~L. and {Kokorev}, Vasily and {Fujimoto}, Seiji and {Jespersen}, Christian Kragh and {Vijayan}, Aswin P. and {Weaver}, John R. and {Ito}, Kei and {Tanaka}, Masayuki and {Ilbert}, Olivier and {Magdis}, Georgios E. and {Whitaker}, Katherine E. and {Faisst}, Andreas L. and {Gallazzi}, Anna and {Gillman}, Steven and {Gim{\'e}nez-Arteaga}, Clara and {G{\'o}mez-Guijarro}, Carlos and {Kubo}, Mariko and {Heintz}, Kasper E. and {Hirschmann}, Michaela and {Oesch}, Pascal and {Onodera}, Masato and {Rizzo}, Francesca and {Lee}, Minju and {Strait}, Victoria and {Toft}, Sune},
        title = "{An Atlas of Color-selected Quiescent Galaxies at z > 3 in Public JWST Fields}",
      journal = {\apj},
     keywords = {Galaxy evolution, High-redshift galaxies, Galaxy quenching, Quenched galaxies, Post-starburst galaxies, Surveys, 594, 734, 2040, 2016, 2176, 1671, Astrophysics - Astrophysics of Galaxies},
         year = 2023,
        month = apr,
       volume = {947},
       number = {1},
          eid = {20},
        pages = {20},
          doi = {10.3847/1538-4357/acbefa},
archivePrefix = {arXiv},
       eprint = {2302.10936},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023ApJ...947...20V},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2024ApJ...965L...6B,
       author = {{Bagley}, Micaela B. and {Pirzkal}, Nor and {Finkelstein}, Steven L. and {Papovich}, Casey and {Berg}, Danielle A. and {Lotz}, Jennifer M. and {Leung}, Gene C.~K. and {Ferguson}, Henry C. and {Koekemoer}, Anton M. and {Dickinson}, Mark and {Kartaltepe}, Jeyhan S. and {Kocevski}, Dale D. and {Somerville}, Rachel S. and {Yung}, L.~Y. Aaron and {Backhaus}, Bren E. and {Casey}, Caitlin M. and {Castellano}, Marco and {Ch{\'a}vez Ortiz}, {\'O}scar A. and {Chworowsky}, Katherine and {Cox}, Isabella G. and {Dav{\'e}}, Romeel and {Davis}, Kelcey and {Estrada-Carpenter}, Vicente and {Fontana}, Adriano and {Fujimoto}, Seiji and {Gardner}, Jonathan P. and {Giavalisco}, Mauro and {Grazian}, Andrea and {Grogin}, Norman A. and {Hathi}, Nimish P. and {Hutchison}, Taylor A. and {Jaskot}, Anne E. and {Jung}, Intae and {Kewley}, Lisa J. and {Kirkpatrick}, Allison and {Larson}, Rebecca L. and {Matharu}, Jasleen and {Natarajan}, Priyamvada and {Pentericci}, Laura and {P{\'e}rez-Gonz{\'a}lez}, Pablo G. and {Ravindranath}, Swara and {Rothberg}, Barry and {Ryan}, Russell and {Shen}, Lu and {Simons}, Raymond C. and {Snyder}, Gregory F. and {Trump}, Jonathan R. and {Wilkins}, Stephen M.},
        title = "{The Next Generation Deep Extragalactic Exploratory Public (NGDEEP) Survey}",
      journal = {\apjl},
     keywords = {Early universe, Galaxy formation, Galaxy evolution, Galaxy chemical evolution, 435, 595, 594, 580, Astrophysics - Astrophysics of Galaxies},
         year = 2024,
        month = apr,
       volume = {965},
       number = {1},
          eid = {L6},
        pages = {L6},
          doi = {10.3847/2041-8213/ad2f31},
archivePrefix = {arXiv},
       eprint = {2302.05466},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2024ApJ...965L...6B},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2023ApJ...946L..12B,
       author = {{Bagley}, Micaela B. and {Finkelstein}, Steven L. and {Koekemoer}, Anton M. and {Ferguson}, Henry C. and {Arrabal Haro}, Pablo and {Dickinson}, Mark and {Kartaltepe}, Jeyhan S. and {Papovich}, Casey and {P{\'e}rez-Gonz{\'a}lez}, Pablo G. and {Pirzkal}, Nor and {Somerville}, Rachel S. and {Willmer}, Christopher N.~A. and {Yang}, Guang and {Yung}, L.~Y. Aaron and {Fontana}, Adriano and {Grazian}, Andrea and {Grogin}, Norman A. and {Hirschmann}, Michaela and {Kewley}, Lisa J. and {Kirkpatrick}, Allison and {Kocevski}, Dale D. and {Lotz}, Jennifer M. and {Medrano}, Aubrey and {Morales}, Alexa M. and {Pentericci}, Laura and {Ravindranath}, Swara and {Trump}, Jonathan R. and {Wilkins}, Stephen M. and {Calabr{\`o}}, Antonello and {Cooper}, M.~C. and {Costantin}, Luca and {de la Vega}, Alexander and {Hilbert}, Bryan and {Hutchison}, Taylor A. and {Larson}, Rebecca L. and {Lucas}, Ray A. and {McGrath}, Elizabeth J. and {Ryan}, Russell and {Wang}, Xin and {Wuyts}, Stijn},
        title = "{CEERS Epoch 1 NIRCam Imaging: Reduction Methods and Simulations Enabling Early JWST Science Results}",
      journal = {\apjl},
     keywords = {Near infrared astronomy, Direct imaging, Astronomy data reduction, 1093, 387, 1861, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Astrophysics of Galaxies},
         year = 2023,
        month = mar,
       volume = {946},
       number = {1},
          eid = {L12},
        pages = {L12},
          doi = {10.3847/2041-8213/acbb08},
archivePrefix = {arXiv},
       eprint = {2211.02495},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023ApJ...946L..12B},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}

@ARTICLE{2023arXiv230602465E,
       author = {{Eisenstein}, Daniel J. and {Willott}, Chris and {Alberts}, Stacey and {Arribas}, Santiago and {Bonaventura}, Nina and {Bunker}, Andrew J. and {Cameron}, Alex J. and {Carniani}, Stefano and {Charlot}, Stephane and {Curtis-Lake}, Emma and {D'Eugenio}, Francesco and {Endsley}, Ryan and {Ferruit}, Pierre and {Giardino}, Giovanna and {Hainline}, Kevin and {Hausen}, Ryan and {Jakobsen}, Peter and {Johnson}, Benjamin D. and {Maiolino}, Roberto and {Rieke}, Marcia and {Rieke}, George and {Rix}, Hans-Walter and {Robertson}, Brant and {Stark}, Daniel P. and {Tacchella}, Sandro and {Williams}, Christina C. and {Willmer}, Christopher N.~A. and {Baker}, William M. and {Baum}, Stefi and {Bhatawdekar}, Rachana and {Boyett}, Kristan and {Chen}, Zuyi and {Chevallard}, Jacopo and {Circosta}, Chiara and {Curti}, Mirko and {Danhaive}, A. Lola and {DeCoursey}, Christa and {de Graaff}, Anna and {Dressler}, Alan and {Egami}, Eiichi and {Helton}, Jakob M. and {Hviding}, Raphael E. and {Ji}, Zhiyuan and {Jones}, Gareth C. and {Kumari}, Nimisha and {L{\"u}tzgendorf}, Nora and {Laseter}, Isaac and {Looser}, Tobias J. and {Lyu}, Jianwei and {Maseda}, Michael V. and {Nelson}, Erica and {Parlanti}, Eleonora and {Perna}, Michele and {Pusk{\'a}s}, D{\'a}vid and {Rawle}, Tim and {Rodr{\'\i}guez Del Pino}, Bruno and {Sandles}, Lester and {Saxena}, Aayush and {Scholtz}, Jan and {Sharpe}, Katherine and {Shivaei}, Irene and {Silcock}, Maddie S. and {Simmonds}, Charlotte and {Skarbinski}, Maya and {Smit}, Renske and {Stone}, Meredith and {Suess}, Katherine A. and {Sun}, Fengwu and {Tang}, Mengtao and {Topping}, Michael W. and {{\"U}bler}, Hannah and {Villanueva}, Natalia C. and {Wallace}, Imaan E.~B. and {Whitler}, Lily and {Witstok}, Joris and {Woodrum}, Charity},
        title = "{Overview of the JWST Advanced Deep Extragalactic Survey (JADES)}",
      journal = {arXiv e-prints},
     keywords = {Astrophysics - Astrophysics of Galaxies},
         year = 2023,
        month = jun,
          eid = {arXiv:2306.02465},
        pages = {arXiv:2306.02465},
          doi = {10.48550/arXiv.2306.02465},
archivePrefix = {arXiv},
       eprint = {2306.02465},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023arXiv230602465E},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% From: https://dawn-cph.github.io/dja/index.html
We kindly request all scientific papers based on data or products downloaded from the Dawn JWST Archive (DJA) to include the following acknowledgement:

(Some of) The data products presented herein were retrieved from the Dawn JWST Archive (DJA). DJA is an initiative of the Cosmic Dawn Center (DAWN), which is funded by the Danish National Research Foundation under grant DNRF140.

% From: https://archive.stsci.edu/publishing/mission-acknowledgements
This work is based [in part] on observations made with the NASA/ESA/CSA James Webb Space Telescope. The data were obtained from the Mikulski Archive for Space Telescopes at the Space Telescope Science Institute, which is operated by the Association of Universities for Research in Astronomy, Inc., under NASA contract NAS 5-03127 for JWST. These observations are associated with program #____.
"""

_DESCRIPTION = """\
Image dataset based on a combination of JWST deep fields from DJA: CEERS, NGDEEP, JADES, PRIMER
"""

_HOMEPAGE = "https://dawn-cph.github.io/dja/index.html"

_LICENSE = "We kindly request all scientific papers based on data or products downloaded from the Dawn JWST Archive (DJA) to include the following acknowledgement:(Some of) The data products presented herein were retrieved from the Dawn JWST Archive (DJA). DJA is an initiative of the Cosmic Dawn Center (DAWN), which is funded by the Danish National Research Foundation under grant DNRF140."

_VERSION = "1.0.0"

class CustomBuilderConfig(datasets.BuilderConfig):
    def __init__(self, image_size=96, 
                 bands=['f090w', 'f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'], 
                 **kwargs):
        """Custom builder config for JWST dataset.

        Args:
            image_size: The size of the images.
            bands: A list of bands for the dataset.
            **kwargs: Keyword arguments forwarded to super.
        """
        super().__init__(**kwargs)
        self.image_size = image_size
        self.bands = bands


class JWST(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        CustomBuilderConfig(
            name="primer-cosmos",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["primer-cosmos/healpix=*/*.hdf5"]}
            ),
            description="PRIMER-COSMOS",
        ),
        CustomBuilderConfig(
            name="primer-uds",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["primer-uds/healpix=*/*.hdf5"]}
            ),
            description="PRIMER-UDS",
        ),
        CustomBuilderConfig(
            name="ceers",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["ceers/healpix=*/*.hdf5"]}
            ),
            description="CEERS",
        ),
        CustomBuilderConfig(
            name="ngdeep",
            version=VERSION,
            bands=['f115w', 'f150w', 'f200w', 'f277w', 'f356w', 'f444w'],
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["ngdeep/healpix=*/*.hdf5"]}
            ),
            description="NGDEEP",
        ),
        CustomBuilderConfig(
            name="gds",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["gds/healpix=*/*.hdf5"]}
            ),
            description="JADES GOODS-S",
        ),
        CustomBuilderConfig(
            name="gdn",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["gdn/healpix=*/*.hdf5"]}
            ),
            description="JADES GOODS-N",
        ),
        CustomBuilderConfig(
            name="all",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["*/healpix=*/*.hdf5"]}
            ),
            description="All JWST datasets",
        ),
    ]

    DEFAULT_CONFIG_NAME = "all"

    _float_features = ['mag_auto', 'flux_radius', 
                       'flux_auto', 'fluxerr_auto',
                       'cxx_image', 'cyy_image', 'cxy_image']
    
    def _info(self):
        """Defines the features available in this dataset."""

        # Starting with all features common to image datasets
        features = {
            "image": Sequence(
                feature={
                    "band": Value("string"),
                    "flux": Array2D(
                        shape=(self.config.image_size, self.config.image_size), dtype="float32"
                    ),
                    "ivar": Array2D(
                        shape=(self.config.image_size, self.config.image_size), dtype="float32"
                    ),
                    "mask": Array2D(
                        shape=(self.config.image_size, self.config.image_size), dtype="bool"
                    ),
                    "psf_fwhm": Value("float32"),
                    "scale": Value("float32"),
                }
            )
        }
        # Adding all values from the catalog
        for f in self._float_features:
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
            print(f"Processing file: {file}")
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
                    
                    # Check if the found object_id matches the requested one
                    if data["object_id"][i] != k:
                        print(f"Warning: Object {k} not found in this chunk. Skipping.")
                        continue

                    # Parse image data
                    example = {
                        "image": [
                            {
                                "band": data["image_band"][i][j].decode("utf-8"),
                                "flux": data["image_flux"][i][j],
                                "ivar": data["image_ivar"][i][j],
                                "mask": data["image_mask"][i][j].astype(bool),
                                "psf_fwhm": data["image_psf_fwhm"][i][j],
                                "scale": data["image_scale"][i][j],
                            }
                            for j in range(len(data["image_band"][i]))
                        ]
                    }

                    # Add all other requested features
                    for f in self._float_features:
                        try:
                            value = data[f][i]
                            example[f] = float(value) if np.isscalar(value) else 0.0
                        except KeyError:
                            print(f"Warning: Feature '{f}' not found in the dataset.")
                            example[f] = 0.0

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
