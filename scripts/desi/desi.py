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
import itertools
import h5py

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = """\
@InProceedings{huggingface:dataset,
title = {A great new dataset},
author={huggingface, Inc.
},
year={2020}
}
"""

# TODO: Add description of the dataset here
# You can copy an official description
_DESCRIPTION = """\
Spectra dataset based on DESI EDR SV3.
"""

# TODO: Add a link to an official homepage for the dataset here
_HOMEPAGE = ""

# TODO: Add the licence for the dataset here if you can find it
_LICENSE = ""

_VERSION = "0.0.1"

_FLOAT_FEATURES = [
    # "TARGETID",
    # "SURVEY",
    # "PROGRAM",
    # "HEALPIX",
    # "SPGRPVAL",
    "Z",
    "ZERR",
    # "ZWARN",
    # "CHI2",
    # "COEFF",
    # "NPIXELS",
    # "SPECTYPE",
    # "SUBTYPE",
    # "NCOEFF",
    # "DELTACHI2",
    # "COADD_FIBERSTATUS",
    # "TARGET_RA",
    # "TARGET_DEC",
    # "PMRA",
    # "PMDEC",
    # "REF_EPOCH",
    # "FA_TARGET",
    # "FA_TYPE",
    # "OBJTYPE",
    # "SUBPRIORITY",
    # "OBSCONDITIONS",
    # "RELEASE",
    # "BRICKNAME",
    # "BRICKID",
    # "BRICK_OBJID",
    # "MORPHTYPE",
    "EBV",
    "FLUX_G",
    "FLUX_R",
    "FLUX_Z",
    "FLUX_W1",
    "FLUX_W2",
    "FLUX_IVAR_G",
    "FLUX_IVAR_R",
    "FLUX_IVAR_Z",
    "FLUX_IVAR_W1",
    "FLUX_IVAR_W2",
    "FIBERFLUX_G",
    "FIBERFLUX_R",
    "FIBERFLUX_Z",
    "FIBERTOTFLUX_G",
    "FIBERTOTFLUX_R",
    "FIBERTOTFLUX_Z",
    # "MASKBITS",
    # "SERSIC",
    # "SHAPE_R",
    # "SHAPE_E1",
    # "SHAPE_E2",
    # "REF_ID",
    # "REF_CAT",
    # "GAIA_PHOT_G_MEAN_MAG",
    # "GAIA_PHOT_BP_MEAN_MAG",
    # "GAIA_PHOT_RP_MEAN_MAG",
    # "PARALLAX",
    # "PHOTSYS",
    # "PRIORITY_INIT",
    # "NUMOBS_INIT",
    # "CMX_TARGET",
    # "DESI_TARGET",
    # "BGS_TARGET",
    # "MWS_TARGET",
    # "SCND_TARGET",
    # "SV1_DESI_TARGET",
    # "SV1_BGS_TARGET",
    # "SV1_MWS_TARGET",
    # "SV1_SCND_TARGET",
    # "SV2_DESI_TARGET",
    # "SV2_BGS_TARGET",
    # "SV2_MWS_TARGET",
    # "SV2_SCND_TARGET",
    # "SV3_DESI_TARGET",
    # "SV3_BGS_TARGET",
    # "SV3_MWS_TARGET",
    # "SV3_SCND_TARGET",
    # "PLATE_RA",
    # "PLATE_DEC",
    # "COADD_NUMEXP",
    # "COADD_EXPTIME",
    # "COADD_NUMNIGHT",
    # "COADD_NUMTILE",
    # "MEAN_DELTA_X",
    # "RMS_DELTA_X",
    # "MEAN_DELTA_Y",
    # "RMS_DELTA_Y",
    # "MEAN_FIBER_RA",
    # "STD_FIBER_RA",
    # "MEAN_FIBER_DEC",
    # "STD_FIBER_DEC",
    # "MEAN_PSF_TO_FIBER_SPECFLUX",
    # "TSNR2_GPBDARK_B",
    # "TSNR2_ELG_B",
    # "TSNR2_GPBBRIGHT_B",
    # "TSNR2_LYA_B",
    # "TSNR2_BGS_B",
    # "TSNR2_GPBBACKUP_B",
    # "TSNR2_QSO_B",
    # "TSNR2_LRG_B",
    # "TSNR2_GPBDARK_R",
    # "TSNR2_ELG_R",
    # "TSNR2_GPBBRIGHT_R",
    # "TSNR2_LYA_R",
    # "TSNR2_BGS_R",
    # "TSNR2_GPBBACKUP_R",
    # "TSNR2_QSO_R",
    # "TSNR2_LRG_R",
    # "TSNR2_GPBDARK_Z",
    # "TSNR2_ELG_Z",
    # "TSNR2_GPBBRIGHT_Z",
    # "TSNR2_LYA_Z",
    # "TSNR2_BGS_Z",
    # "TSNR2_GPBBACKUP_Z",
    # "TSNR2_QSO_Z",
    # "TSNR2_LRG_Z",
    # "TSNR2_GPBDARK",
    # "TSNR2_ELG",
    # "TSNR2_GPBBRIGHT",
    # "TSNR2_LYA",
    # "TSNR2_BGS",
    # "TSNR2_GPBBACKUP",
    # "TSNR2_QSO",
    # "TSNR2_LRG",
    # "SV_NSPEC",
    # "SV_PRIMARY",
    # "ZCAT_NSPEC",
    # "ZCAT_PRIMARY",
]

class DESI(datasets.GeneratorBasedBuilder):
    """TODO: Short description of my dataset."""

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
            "spectrum": {
                "flux": Array2D(shape=(self._spectrum_length, 1), dtype="float32"),
                "ivar": Array2D(shape=(self._spectrum_length, 1), dtype="float32"),
                "lsf": Array2D( shape=(11, 1), dtype="float32"),
                "lsf_sigma": Value("float32"),
                "lambda_min": Value("float32"),
                "lambda_max": Value("float32"),
            }
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")

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
        for i, file in enumerate(itertools.chain.from_iterable(files)):
            with h5py.File(file, "r") as data:
                for i in range(len(data["object_id"])):
                    # Parse spectrum data
                    example = {
                        "spectrum": 
                            {
                                "flux": data['spectrum_flux'][i].reshape(-1, 1), 
                                "ivar": data['spectrum_ivar'][i].reshape(-1, 1),
                                "lsf": data['spectrum_lsf'][i].reshape(-1, 1),
                                "lsf_sigma": data['spectrum_lsf_sigma'][i],
                                "lambda_min": data['spectrum_lambda_min'][i],
                                "lambda_max": data['spectrum_lambda_max'][i],
                            }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    yield str(data["object_id"][i]), example
