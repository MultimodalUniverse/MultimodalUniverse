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
import os


# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@article{Scolnic_2018,
    doi = {10.3847/1538-4357/aab9bb},
    url = {https://dx.doi.org/10.3847/1538-4357/aab9bb},
    year = {2018},
    month = {may},
    publisher = {The American Astronomical Society},
    volume = {859},
    number = {2},
    pages = {101},
    author = {D. M. Scolnic and D. O. Jones and A. Rest and Y. C. Pan and R. Chornock and R. J. Foley and M. E. Huber and R. Kessler and G. Narayan and A. G. Riess and S. Rodney and E. Berger and D. J. Brout and P. J. Challis and M. Drout and D. Finkbeiner and R. Lunnan and R. P. Kirshner and N. E. Sanders and E. Schlafly and S. Smartt and C. W. Stubbs and J. Tonry and W. M. Wood-Vasey and M. Foley and J. Hand and E. Johnson and W. S. Burgett and K. C. Chambers and P. W. Draper and K. W. Hodapp and N. Kaiser and R. P. Kudritzki and E. A. Magnier and N. Metcalfe and F. Bresolin and E. Gall and R. Kotak and M. McCrum and K. W. Smith},
    title = {The Complete Light-curve Sample of Spectroscopically Confirmed SNe Ia from Pan-STARRS1 and Cosmological Constraints from the Combined Pantheon Sample},
    journal = {The Astrophysical Journal},
    abstract = {We present optical light curves, redshifts, and classifications for  spectroscopically confirmed Type Ia supernovae (SNe Ia) discovered by the Pan-STARRS1 (PS1) Medium Deep Survey. We detail improvements to the PS1 SN photometry, astrometry, and calibration that reduce the systematic uncertainties in the PS1 SN Ia distances. We combine the subset of  PS1 SNe Ia (0.03 &lt; z &lt; 0.68) with useful distance estimates of SNe Ia from the Sloan Digital Sky Survey (SDSS), SNLS, and various low-z and Hubble Space Telescope samples to form the largest combined sample of SNe Ia, consisting of a total of  SNe Ia in the range of 0.01 &lt; z &lt; 2.3, which we call the “Pantheon Sample.” When combining Planck 2015 cosmic microwave background (CMB) measurements with the Pantheon SN sample, we find  and  for the wCDM model. When the SN and CMB constraints are combined with constraints from BAO and local H0 measurements, the analysis yields the most precise measurement of dark energy to date:  and  for the CDM model. Tension with a cosmological constant previously seen in an analysis of PS1 and low-z SNe has diminished after an increase of 2× in the statistics of the PS1 sample, improved calibration and photometry, and stricter light-curve quality cuts. We find that the systematic uncertainties in our measurements of dark energy are almost as large as the statistical uncertainties, primarily due to limitations of modeling the low-redshift sample. This must be addressed for future progress in using SNe Ia to measure dark energy.}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
Here is the text for acknowledging PS1 in your publications:

The Pan-STARRS1 Surveys (PS1) and the PS1 public science archive have been made possible through contributions by the Institute for Astronomy, the University of Hawaii, the Pan-STARRS Project Office, the Max-Planck Society and its participating institutes, the Max Planck Institute for Astronomy, Heidelberg and the Max Planck Institute for Extraterrestrial Physics, Garching, The Johns Hopkins University, Durham University, the University of Edinburgh, the Queen's University Belfast, the Harvard-Smithsonian Center for Astrophysics, the Las Cumbres Observatory Global Telescope Network Incorporated, the National Central University of Taiwan, the Space Telescope Science Institute, the National Aeronautics and Space Administration under Grant No. NNX08AR22G issued through the Planetary Science Division of the NASA Science Mission Directorate, the National Science Foundation Grant No. AST-1238877, the University of Maryland, Eotvos Lorand University (ELTE), the Los Alamos National Laboratory, and the Gordon and Betty Moore Foundation.

In addition, please cite the following papers describing the instrument, survey, and data analysis as appropriate:

The Pan-STARRS1 Surveys, Chambers, K.C., et al.
Pan-STARRS Data Processing System, Magnier, E. A., et al.
Pan-STARRS Pixel Processing: Detrending, Warping, Stacking, Waters, C. Z., et al.
Pan-STARRS Pixel Analysis: Source Detection and Characterization, Magnier, E. A., et al.
Pan-STARRS Photometric and Astrometric Calibration, Magnier, E. A., et al.
The Pan-STARRS1 Database and Data Products, Flewelling, H. A., et al.
"""


# You can copy an official description
_DESCRIPTION = """\
Time-series dataset from the Pan-STARRS1 (PS1).
"""

_HOMEPAGE = "https://iopscience.iop.org/article/10.3847/1538-4357/aab9bb/pdf"


_LICENSE = "CC BY 4.0"

_VERSION = "1.0.0"

_STR_FEATURES = [
    "object_id",
    "obj_type"
]

_FLOAT_FEATURES = [
    "redshift",
    "host_log_mass"
]


class PS1SNIa(datasets.GeneratorBasedBuilder):
    """"""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="ps1_sne_ia",
            version=VERSION,
        data_files=DataFilesPatternsDict.from_patterns({"train": ["ps1_sne_ia/healpix=*/*.hdf5"]}), # This seems fairly inflexible. Probably a massive failure point.
            description="Light curves from Pan-STARRS1 (PS1)",
        ),
    ]

    DEFAULT_CONFIG_NAME = "ps1_sne_ia"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to light curve datasets
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
            features[f] = Value("float32")
        for f in _STR_FEATURES:
            features[f] = Value("string")

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
        if object_ids is not None:
            files = [f for f in files if os.path.split(f)[-1][:-5] in object_ids]
            # Filter files by object_id
        for file in files:
            with h5py.File(file, "r") as data:
                # Parse data
                idxs = np.arange(0, data["flux"].shape[0])
                band_idxs = idxs.repeat(data["flux"].shape[-1]).reshape(
                    len(data["bands"][()].decode('utf-8').split(",")), -1
                )
                bands = data["bands"][()].decode('utf-8').split(",")
                example = {
                    "lightcurve": {
                        "band": np.asarray([bands[band_number] for band_number in band_idxs.flatten().astype("int32")]).astype("str"),
                        "time": np.asarray(data["time"]).flatten().astype("float32"),
                        "flux": np.asarray(data["flux"]).flatten().astype("float32"),
                        "flux_err": np.asarray(data["flux_err"]).flatten().astype("float32"),
                    }
                }
                    
                # Add remaining features
                for f in _FLOAT_FEATURES:
                    example[f] = np.asarray(data[f]).astype("float32")
                for f in _STR_FEATURES:
                    example[f] = data[f][()].decode('utf-8')

                yield str(data["object_id"][()]), example