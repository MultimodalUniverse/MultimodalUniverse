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

import numpy as np

import datasets
from datasets import Features, Value
from datasets.data_files import DataFilesPatternsDict
import h5py

_CITATION = """\
@ARTICLE{2017ApJS..230...24B,
       author = {{Bianchi}, Luciana and {Shiao}, Bernie and {Thilker}, David},
        title = "{Revised Catalog of GALEX Ultraviolet Sources. I. The All-Sky Survey: GUVcat\_AIS}",
      journal = {\apjs},
     keywords = {catalogs, Galaxy: stellar content, stars: AGB and post-AGB, stars: early-type, surveys, ultraviolet: general, Astrophysics - Astrophysics of Galaxies, Astrophysics - Solar and Stellar Astrophysics},
         year = 2017,
        month = jun,
       volume = {230},
       number = {2},
          eid = {24},
        pages = {24},
          doi = {10.3847/1538-4365/aa7053},
archivePrefix = {arXiv},
       eprint = {1704.05903},
 primaryClass = {astro-ph.GA},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2017ApJS..230...24B},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}


"""

_DESCRIPTION = """\
The Galaxy Evolution Explorer (GALEX) imaged the sky in two ultraviolet (UV) bands, far-UV (FUV, λ eff ∼ 1528 Å), and near-UV (NUV, λ eff ∼ 2310 Å), delivering the first comprehensive sky surveys at these wavelengths. This GUVcat_AIS catalog is a science-enhanced, “clean” catalog of GALEX UV sources. 
"""

_HOMEPAGE = ""

_LICENSE = ""

_VERSION = "0.0.1"

_mapping = {
    'avaspdec': 'float64',
    'avaspra': 'float64',
    'band': 'int32',
    'chkobj_type': 'int32',
    'corv': 'string',
    'dec': 'float64',
    'difffuv': 'float32',
    'difffuvdist': 'float32',
    'diffnuv': 'float32',
    'diffnuvdist': 'float32',
    'e_bv': 'float32',
    'fexptime': 'float32',
    'fov_radius': 'float32',
    'fuv_a_world': 'float32',
    'fuv_artifact': 'int32',
    'fuv_b_world': 'float32',
    'fuv_class_star': 'float32',
    'fuv_ellipticity': 'float32',
    'fuv_errtheta_j2000': 'float32',
    'fuv_flags': 'int32',
    'fuv_flux': 'float32',
    'fuv_fluxerr': 'float32',
    'fuv_fwhm_image': 'float32',
    'fuv_fwhm_world': 'float32',
    'fuv_kron_radius': 'float32',
    'fuv_mag': 'float32',
    'fuv_mag_aper_4': 'float32',
    'fuv_mag_aper_6': 'float32',
    'fuv_mag_auto': 'float32',
    'fuv_magerr': 'float32',
    'fuv_magerr_aper_4': 'float32',
    'fuv_magerr_aper_6': 'float32',
    'fuv_magerr_auto': 'float32',
    'fuv_ncat_flux_radius_3': 'float32',
    'fuv_ncat_fwhm_image': 'float32',
    'fuv_poserr': 'float32',
    'fuv_pperr': 'float32',
    'fuv_theta_j2000': 'float32',
    'fuv_weight': 'float32',
    'fuv_x_image': 'float32',
    'fuv_y_image': 'float32',
    'glat': 'float64',
    'glon': 'float64',
    'grank': 'int16',
    'grankdist': 'int16',
    'groupgid': 'string',
    'groupgiddist': 'string',
    'groupgidtot': 'string',
    'healpix': 'int64',
    'ib_poserr': 'float32',
    'img': 'int32',
    'inlargeobj': 'string',
    'istherespectrum': 'string',
    'largeobjsize': 'float32',
    'mpstype': 'string',
    'nexptime': 'float32',
    'ngrank': 'int16',
    'ngrankdist': 'int16',
    'nuv_a_world': 'float32',
    'nuv_artifact': 'int32',
    'nuv_b_world': 'float32',
    'nuv_class_star': 'float32',
    'nuv_ellipticity': 'float32',
    'nuv_errtheta_j2000': 'float32',
    'nuv_flags': 'int32',
    'nuv_flux': 'float32',
    'nuv_fluxerr': 'float32',
    'nuv_fwhm_image': 'float32',
    'nuv_fwhm_world': 'float32',
    'nuv_kron_radius': 'float32',
    'nuv_mag': 'float32',
    'nuv_mag_aper_4': 'float32',
    'nuv_mag_aper_6': 'float32',
    'nuv_mag_auto': 'float32',
    'nuv_magerr': 'float32',
    'nuv_magerr_aper_4': 'float32',
    'nuv_magerr_aper_6': 'float32',
    'nuv_magerr_auto': 'float32',
    'nuv_poserr': 'float32',
    'nuv_pperr': 'float32',
    'nuv_theta_j2000': 'float32',
    'nuv_weight': 'float32',
    'nuv_x_image': 'float32',
    'nuv_y_image': 'float32',
    'object_id': 'string',
    'photoextractid': 'string',
    'primgid': 'string',
    'primgiddist': 'string',
    'prob': 'float32',
    'ra': 'float64',
    'sep': 'float32',
    'sepas': 'float32',
    'sepasdist': 'float32',
    'subvisit': 'int32',
    'tilenum': 'int32',
    'type': 'int32',
}


class AllWISE(datasets.GeneratorBasedBuilder):
    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="ais",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["./ais/healpix=*/*.hdf5"]}
            ),
            description="GALEX GUVcat_AIS catalog.",
        ),
    ]

    DEFAULT_CONFIG_NAME = "ais"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""

        features = {k: Value(dtype=v, id=None) for k, v in _mapping.items()}

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

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(itertools.chain.from_iterable(files)):
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

                    s_id = data["object_id"][i]

                    example = {k: data[k][i] for k in _mapping}

                    for k, v in example.items():
                        if isinstance(v, bytes):
                            example[k] = v.decode("utf-8")
                        if isinstance(v, float) and np.isnan(v):
                            example[k] = None

                    yield int(s_id), example

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
