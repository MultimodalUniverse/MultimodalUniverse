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
from datasets import Features, Sequence, Value
from datasets.data_files import DataFilesPatternsDict

_CITATION = r"""% CITATION
@ARTICLE{Zhao2012,
       author = {{Zhao}, Gang and {Zhao}, Yong-Heng and {Chu}, Yao-Quan and {Jing}, Yi-Peng and {Deng}, Li-Cai},
        title = "{LAMOST spectral survey {\textemdash} An overview}",
      journal = {Research in Astronomy and Astrophysics},
         year = 2012,
        month = jul,
       volume = {12},
       number = {7},
        pages = {723-734},
          doi = {10.1088/1674-4527/12/7/002},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2012RAA....12..723Z},
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

# Common features shared across all LAMOST catalogues
_COMMON_FEATURES = {
    "obsid": "int64",
    "uid": "string",
    "gp_id": "int64",
    "designation": "string",
    "obsdate": "string",
    "lmjd": "int64",
    "mjd": "int64",
    "planid": "string",
    "spid": "int64",
    "fiberid": "int64",
    "ra_obs": "float32",
    "dec_obs": "float32",
    "snru": "float32",
    "snrg": "float32",
    "snrr": "float32",
    "snri": "float32",
    "snrz": "float32",
    "class": "string",
    "subclass": "string",
    "z": "float32",
    "z_err": "float32",
    "ps_id": "int64",
    "mag_ps_g": "float32",
    "mag_ps_r": "float32",
    "mag_ps_i": "float32",
    "mag_ps_z": "float32",
    "mag_ps_y": "float32",
    "gaia_source_id": "string",
    "gaia_g_mean_mag": "float32",
    "fibertype": "string",
    "offsets": "int64",
    "offsets_v": "float32",
    "ra": "float32",
    "dec": "float32",
}

_EXTRA_FEATURES = {
    "lrs_catalogue": {
        # Catalogue-specific features only (common features are in _COMMON_FEATURES)
        "fibermask": "int64",
        "with_norm_flux": "int64",
    },
    "lrs_stellar": {
        # Stellar-specific features only (common features are in _COMMON_FEATURES)
        "teff": "float32",
        "teff_err": "float32",
        "logg": "float32",
        "logg_err": "float32",
        "feh": "float32",
        "feh_err": "float32",
        "rv": "float32",
        "rv_err": "float32",
        "alpha_m": "float32",
        "alpha_m_err": "float32",
        "vsini_lasp": "float32",
    },
    "lrs_astellar": {
        # A-stellar-specific features only (common features are in _COMMON_FEATURES)
        "kp12": "float32",
        "kp18": "float32",
        "kp6": "float32",
        "hdelta12": "float32",
        "hdelta24": "float32",
        "hdelta48": "float32",
        "hdelta64": "float32",
        "hgamma12": "float32",
        "hgamma24": "float32",
        "hgamma48": "float32",
        "hgamma54": "float32",
        "hbeta12": "float32",
        "hbeta24": "float32",
        "hbeta48": "float32",
        "hbeta60": "float32",
        "halpha12": "float32",
        "halpha24": "float32",
        "halpha48": "float32",
        "halpha70": "float32",
        "paschen13": "float32",
        "paschen142": "float32",
        "paschen242": "float32",
        "halpha_d02": "float32",
        "hbeta_d02": "float32",
        "hgamma_d02": "float32",
        "hdelta_d02": "float32",
    },
    "lrs_mstellar": {
        # M-stellar-specific features only (common features are in _COMMON_FEATURES)
        "teff": "float32",
        "teff_err": "float32",
        "logg": "float32",
        "logg_err": "float32",
        "m_h": "float32",
        "m_h_err": "float32",
        "ewha": "float32",
        "ewha_err": "float32",
        "tio5": "float32",
        "cah2": "float32",
        "cah3": "float32",
        "tio1": "float32",
        "tio2": "float32",
        "tio3": "float32",
        "tio4": "float32",
        "cah1": "float32",
        "caoh": "float32",
        "tio5_err": "float32",
        "cah2_err": "float32",
        "cah3_err": "float32",
        "tio1_err": "float32",
        "tio2_err": "float32",
        "tio3_err": "float32",
        "tio4_err": "float32",
        "cah1_err": "float32",
        "caoh_err": "float32",
        "zeta": "float32",
        "zeta_err": "float32",
        "type": "float32",
        "na": "float32",
    },
    "lrs_qso": {
        "sn_ratio_conti": "float32",
        "fe_uv_norm": "float32",
        "fe_uv_fwhm": "float32",
        "fe_uv_shift": "float32",
        "fe_op_norm": "float32",
        "fe_op_fwhm": "float32",
        "fe_op_shift": "float32",
        "pl_norm": "float32",
        "pl_slope": "float32",
        "balmer_norm": "float32",
        "balmer_teff": "int64",
        "balmer_tau_e": "float32",
        "poly_a": "float32",
        "poly_b": "float32",
        "poly_c": "float32",
        "l1350": "float32",
        "l3000": "float32",
        "l5100": "float32",
        "line_name_1": "string",
        "fitting_status_1": "int64",
        "min_chi2_1": "float32",
        "red_chi2_1": "float32",
        "ndof_1": "int64",
        "line_name_2": "string",
        "fitting_status_2": "int64",
        "min_chi2_2": "float32",
        "red_chi2_2": "float32",
        "ndof_2": "int64",
        "line_name_3": "string",
        "fitting_status_3": "int64",
        "min_chi2_3": "float32",
        "red_chi2_3": "float32",
        "ndof_3": "int64",
        "line_name_4": "string",
        "fitting_status_4": "int64",
        "min_chi2_4": "float32",
        "red_chi2_4": "float32",
        "ndof_4": "int64",
        "line_name_5": "string",
        "fitting_status_5": "int64",
        "min_chi2_5": "int64",
        "red_chi2_5": "int64",
        "ndof_5": "int64",
        "line_name_6": "string",
        "fitting_status_6": "int64",
        "min_chi2_6": "int64",
        "red_chi2_6": "int64",
        "ndof_6": "int64",
        "lya_br_1_peak_flux": "float32",
        "lya_br_1_peak_wavelength": "float32",
        "lya_br_1_sigma": "float32",
        "lya_na_1_peak_flux": "float32",
        "lya_na_1_peak_wavelength": "float32",
        "lya_na_1_sigma": "float32",
        "civ_br_1_peak_flux": "float32",
        "civ_br_1_peak_wavelength": "float32",
        "civ_br_1_sigma": "float32",
        "civ_na_1_peak_flux": "float32",
        "civ_na_1_peak_wavelength": "float32",
        "civ_na_1_sigma": "float32",
        "ciii_br_1_peak_flux": "float32",
        "ciii_br_1_peak_wavelength": "float32",
        "ciii_br_1_sigma": "float32",
        "ciii_br_2_peak_flux": "float32",
        "ciii_br_2_peak_wavelength": "float32",
        "ciii_na_1_peak_flux": "int64",
        "ciii_na_1_peak_wavelength": "int64",
        "ciii_na_1_sigma": "int64",
        "ciii_na_2_peak_flux": "int64",
        "ciii_na_2_peak_wavelength": "int64",
        "ciii_na_2_sigma": "int64",
        "mgii_br_1_peak_flux": "float32",
        "mgii_br_1_peak_wavelength": "float32",
        "mgii_br_1_sigma": "float32",
        "mgii_na_1_peak_flux": "float32",
        "mgii_na_1_peak_wavelength": "float32",
        "mgii_na_1_sigma": "float32",
        "mgii_na_2_peak_flux": "float32",
        "mgii_na_2_peak_wavelength": "float32",
        "mgii_na_2_sigma": "float32",
        "hb_br_1_peak_flux": "float32",
        "hb_br_1_peak_wavelength": "float32",
        "hb_br_1_sigma": "float32",
        "hb_na_1_peak_flux": "float32",
        "hb_na_1_peak_wavelength": "float32",
        "hb_na_1_sigma": "float32",
        "oiii4959_1_peak_flux": "float32",
        "oiii4959_1_peak_wavelength": "float32",
        "oiii4959_1_sigma": "float32",
        "oiii5007_1_peak_flux": "float32",
        "oiii5007_1_peak_wavelength": "float32",
        "oiii5007_1_sigma": "float32",
        "ha_br_1_peak_flux": "float32",
        "ha_br_1_peak_wavelength": "float32",
        "ha_br_1_sigma": "float32",
        "ha_br_2_peak_flux": "float32",
        "ha_br_2_peak_wavelength": "float32",
        "ha_br_2_sigma": "float32",
        "ha_br_3_peak_flux": "float32",
        "ha_br_3_peak_wavelength": "float32",
        "ha_br_3_sigma": "float32",
        "ha_na_1_peak_flux": "float32",
        "ha_na_1_peak_wavelength": "float32",
        "ha_na_1_sigma": "float32",
        "nii6549_1_peak_flux": "float32",
        "nii6549_1_peak_wavelength": "float32",
        "nii6549_1_sigma": "float32",
        "nii6585_1_peak_flux": "float32",
        "nii6585_1_peak_wavelength": "float32",
        "nii6585_1_sigma": "float32",
        "sii6718_1_peak_flux": "float32",
        "sii6718_1_peak_wavelength": "float32",
        "sii6718_1_sigma": "float32",
        "sii6732_1_peak_flux": "float32",
        "sii6732_1_peak_wavelength": "float32",
        "sii6732_1_sigma": "float32",
        "lya_fwhm": "float32",
        "lya_sigma": "float32",
        "lya_ew": "float32",
        "lya_peak_wavelength": "float32",
        "lya_area": "float32",
        "civ_fwhm": "float32",
        "civ_sigma": "float32",
        "civ_ew": "float32",
        "civ_peak_wavelength": "float32",
        "civ_area": "float32",
        "ciii_fwhm": "float32",
        "ciii_sigma": "float32",
        "ciii_ew": "float32",
        "ciii_peak_wavelength": "float32",
        "ciii_area": "float32",
        "mgii_fwhm": "float32",
        "mgii_sigma": "float32",
        "mgii_ew": "float32",
        "mgii_peak_wavelength": "float32",
        "mgii_area": "float32",
        "hb_fwhm": "float32",
        "hb_sigma": "float32",
        "hb_ew": "float32",
        "hb_peak_wavelength": "float32",
        "hb_area": "float32",
        "ha_fwhm": "float32",
        "ha_sigma": "float32",
        "ha_ew": "float32",
        "ha_peak_wavelength": "float32",
        "ha_area": "float32",
    },
    "lrs_galaxy": {
        "hbeta_flux": "float32",
        "hbeta_ew": "float32",
        "oiii_4960_flux": "float32",
        "oiii_4960_ew": "float32",
        "oiii_5008_flux": "float32",
        "oiii_5008_ew": "float32",
        "nii_6550_flux": "float32",
        "nii_6550_ew": "float32",
        "halpha_flux": "float32",
        "halpha_ew": "float32",
        "nii_6585_flux": "float32",
        "nii_6585_ew": "float32",
        "sii_6718_flux": "float32",
        "sii_6718_ew": "float32",
        "sii_6733_flux": "float32",
        "sii_6733_ew": "float32",
        "age_lw": "float32",
        "age_mw": "float32",
        "metal_lw": "float32",
        "metal_mw": "float32",
        "vsig": "float32",
        "vsig_err": "float32",
    },
    "lrs_wd": {
        "wd_subclass": "string",
        "teff": "float32",
        "teff_err": "float32",
        "logg": "float32",
        "logg_err": "float32",
    },
    "lrs_cv": {
        "class_liter": "string",
        "period_liter": "float32",
        "r": "int64",
        "abs_gmag": "float32",
        "abs_gmag_err": "float32",
    },
}

_CATALOG_DESCRIPTORS = dict(
    lrs_catalogue="LAMOST DR10 v2.0 LRS General Catalogue",
    lrs_stellar="LAMOST DR10 v2.0 LRS Stellar Catalogue",
    lrs_astellar="LAMOST DR10 v2.0 LRS A-type Stellar Catalogue",
    lrs_mstellar="LAMOST DR10 v2.0 LRS M-type Stellar Catalogue",
    lrs_qso="LAMOST DR10 v2.0 LRS Quasar Catalogue",
    lrs_galaxy="LAMOST DR10 v2.0 LRS Galaxy Catalogue",
    lrs_wd="LAMOST DR10 v2.0 LRS White Dwarf Catalogue",
    lrs_cv="LAMOST DR10 v2.0 LRS Cataclysmic Variable Catalogue",
)


class CustomBuilderConfig(datasets.BuilderConfig):
    def __init__(self, extra_features=None, **kwargs):
        super().__init__(**kwargs)
        self.extra_features = extra_features


class LAMOST(datasets.GeneratorBasedBuilder):
    """
    Large Sky Area Multi-Object Fiber Spectroscopic Telescope (LAMOST)
    """

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        CustomBuilderConfig(
            name=f"dr10_v20_{k}",
            version=_VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {
                    "train": [f"./dr10_v20_{k}/healpix=*/*.hdf5"]
                }  # hf doesnt like dots in filepath... so v2.0 -> v20
            ),
            description=desc,
            extra_features=_EXTRA_FEATURES[k],
        )
        for k, desc in _CATALOG_DESCRIPTORS.items()
    ]

    DEFAULT_CONFIG_NAME = "dr10_v20_lrs_catalogue"

    def _info(self):
        """Defines the features available in this dataset."""
        #  start with spectral features
        features = {
            "spectrum_flux": Sequence(Value("float32")),
            "spectrum_wavelength": Sequence(Value("float32")),
        }

        # Merge common features with catalogue-specific extra features
        all_features = {**_COMMON_FEATURES, **self.config.extra_features}
        for k, v in all_features.items():
            features[k] = Value(dtype=v)

        # Format acknowledgements to have % at the beginning of each line
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
                        "spectrum_flux": data["spectrum_flux"][i],
                        "spectrum_wavelength": data["spectrum_wavelength"][i],
                    }

                    # Add all other features
                    for feature in _COMMON_FEATURES.keys():
                        example[feature] = data[feature][i]
                    for feature in self.config.extra_features.keys():
                        example[feature] = data[feature][i]

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
