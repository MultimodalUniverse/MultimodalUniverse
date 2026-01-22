"""Streaming data pipelines for cross-matched MMU datasets.

This module provides PyTorch IterDataPipe implementations for efficiently
streaming and cross-matching large astronomical datasets without loading
everything into memory.
"""

import os
from argparse import Namespace
from functools import partial
from typing import List, Optional

import h5py
import numpy as np
import torch
import torch.distributed as dist
from astropy import units as u
from astropy.coordinates import SkyCoord
from torch.utils.data import DataLoader
from torch.utils.data.datapipes.datapipe import IterDataPipe

import lightning as L
from datasets import Sequence, load_dataset_builder


# =============================================================================
# Environment utilities
# =============================================================================

def format_with_env(s):
    """Replace environment variable placeholders in strings with their values.

    Supports {VAR_NAME} syntax for environment variable substitution.
    Works recursively on strings, dicts, lists, and Namespace objects.
    """
    if isinstance(s, str):
        for key, value in os.environ.items():
            s = s.replace("{" + key + "}", value)
        return s
    elif isinstance(s, dict):
        return {k: format_with_env(v) for k, v in s.items()}
    elif isinstance(s, list):
        return [format_with_env(v) for v in s]
    elif isinstance(s, Namespace):
        return type(s)(**{k: format_with_env(v) for k, v in s.__dict__.items()})
    else:
        return s


# =============================================================================
# Dataset-specific formatting functions
# =============================================================================

def _format_sdss(data):
    """Format SDSS spectroscopy data."""
    _FLOAT_FEATURES = ["VDISP", "VDISP_ERR", "Z", "Z_ERR"]
    example = {
        "spectrum": {
            "flux": data["spectrum_flux"],
            "ivar": data["spectrum_ivar"],
            "lsf_sigma": data["spectrum_lsf_sigma"],
            "lambda": data["spectrum_lambda"],
            "mask": data["spectrum_mask"],
        }
    }
    for k in _FLOAT_FEATURES:
        example[k] = data[k].astype("float32")
    example["object_id"] = str(data["object_id"])
    return example


def _format_desi(data):
    """Format DESI spectroscopy data."""
    _FLOAT_FEATURES = [
        "Z", "ZERR", "EBV", "FLUX_G", "FLUX_R", "FLUX_Z",
        "FLUX_IVAR_G", "FLUX_IVAR_R", "FLUX_IVAR_Z",
        "FIBERFLUX_G", "FIBERFLUX_R", "FIBERFLUX_Z",
        "FIBERTOTFLUX_G", "FIBERTOTFLUX_R", "FIBERTOTFLUX_Z",
    ]
    example = {
        "spectrum": {
            "flux": data["spectrum_flux"],
            "ivar": data["spectrum_ivar"],
            "lsf_sigma": data["spectrum_lsf_sigma"],
            "lambda": data["spectrum_lambda"],
            "mask": data["spectrum_mask"],
        }
    }
    for k in _FLOAT_FEATURES:
        example[k] = data[k].astype("float32")
    example["object_id"] = str(data["object_id"])
    return example


def _format_decals(data):
    """Format DECaLS imaging data."""
    _bands = ["DES-G", "DES-R", "DES-Z"]
    _FLOAT_FEATURES = [
        "ebv", "flux_g", "flux_r", "flux_z",
        "fiberflux_g", "fiberflux_r", "fiberflux_z",
        "psfdepth_g", "psfdepth_r", "psfdepth_z", "z_spec",
    ]
    example = {
        "image": {
            "band": _bands,
            "flux": data["image_array"],
            "psf_fwhm": data["image_psf_fwhm"],
            "scale": data["image_scale"],
        }
    }
    for f in _FLOAT_FEATURES:
        example[f] = data[f].astype("float32")
    example["object_id"] = str(data["object_id"])
    return example


def _format_legacysurvey(data):
    """Format Legacy Survey imaging data."""
    _bands = ["DES-G", "DES-R", "DES-I", "DES-Z"]
    _FLOAT_FEATURES = [
        "EBV", "FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z",
        "FLUX_W1", "FLUX_W2", "FLUX_W3", "FLUX_W4",
        "SHAPE_R", "SHAPE_E1", "SHAPE_E2",
    ]
    _CATALOG_PREFIX = "catalog_"
    _CATALOG_FEATURES = ["X", "Y", "SHAPE_E1", "SHAPE_E2", "SHAPE_R"]

    example = {
        "image": {
            "band": _bands,
            "flux": data["image_array"],
            "ivar": data["image_ivar"],
            "mask": data["image_mask"],
            "psf_fwhm": data["image_psf_fwhm"],
            "scale": data["image_scale"],
            "rgb": data["image_rgb"],
        },
        "object_mask": data["object_mask"],
    }
    for f in _FLOAT_FEATURES:
        example[f] = data[f].astype("float32")
    example["object_id"] = str(data["object_id"])
    example["catalog"] = {f: data[_CATALOG_PREFIX + f] for f in _CATALOG_FEATURES}
    for f in _CATALOG_FEATURES:
        if f in ["X", "Y"]:
            example["catalog"][f] = example["catalog"][f].astype(np.int16)
        else:
            example["catalog"][f] = example["catalog"][f].astype(np.float32)
    return example


def _format_hsc(data):
    """Format Hyper Suprime-Cam imaging data."""
    _bands = ["HSC-G", "HSC-R", "HSC-I", "HSC-Z", "HSC-Y"]
    _FLOAT_FEATURES = [
        "a_g", "a_r", "a_i", "a_z", "a_y",
        "g_extendedness_value", "r_extendedness_value",
        "i_extendedness_value", "z_extendedness_value", "y_extendedness_value",
        "g_cmodel_mag", "g_cmodel_magerr", "r_cmodel_mag", "r_cmodel_magerr",
        "i_cmodel_mag", "i_cmodel_magerr", "z_cmodel_mag", "z_cmodel_magerr",
        "y_cmodel_mag", "y_cmodel_magerr",
        "g_sdssshape_psf_shape11", "g_sdssshape_psf_shape22", "g_sdssshape_psf_shape12",
        "r_sdssshape_psf_shape11", "r_sdssshape_psf_shape22", "r_sdssshape_psf_shape12",
        "i_sdssshape_psf_shape11", "i_sdssshape_psf_shape22", "i_sdssshape_psf_shape12",
        "z_sdssshape_psf_shape11", "z_sdssshape_psf_shape22", "z_sdssshape_psf_shape12",
        "y_sdssshape_psf_shape11", "y_sdssshape_psf_shape22", "y_sdssshape_psf_shape12",
        "g_sdssshape_shape11", "g_sdssshape_shape22", "g_sdssshape_shape12",
        "r_sdssshape_shape11", "r_sdssshape_shape22", "r_sdssshape_shape12",
        "i_sdssshape_shape11", "i_sdssshape_shape22", "i_sdssshape_shape12",
        "z_sdssshape_shape11", "z_sdssshape_shape22", "z_sdssshape_shape12",
        "y_sdssshape_shape11", "y_sdssshape_shape22", "y_sdssshape_shape12",
    ]
    example = {
        "image": {
            "band": _bands,
            "flux": data["image_array"],
            "ivar": data["image_ivar"],
            "mask": data["image_mask"],
            "psf_fwhm": data["image_psf_fwhm"],
            "scale": data["image_scale"],
        }
    }
    for f in _FLOAT_FEATURES:
        example[f] = data[f].astype("float32")
    example["object_id"] = str(data["object_id"])
    return example


def _format_gz3d(data):
    """Format Galaxy Zoo 3D segmentation data."""
    all_channels = ["center", "star", "spiral", "bar"]
    example = {
        "gz_total_classifications": data["gz_total_classifications"].astype(np.uint8),
        "gz_spiral_votes": data["gz_spiral_votes"].astype(np.uint8),
        "gz_bar_votes": data["gz_bar_votes"].astype(np.uint8),
        "rgb": data["false_color"].astype(np.uint8),
        "segmentation": {
            "class": [channel for channel in all_channels],
            "array": np.stack([data[channel].astype(np.uint8) for channel in all_channels]),
        },
    }
    example["object_id"] = str(data["object_id"])
    return example


def _format_gz10(data):
    """Format Galaxy Zoo 10 classification data."""
    example = {
        "label": data["ans"],
        "object_id": str(data["object_id"]),
        "rgb": data["images"].transpose(2, 0, 1),
        "scale": data["pxscale"].astype("float32"),
    }
    return example


def _format_gzclumps(data):
    """Format Galaxy Zoo Clumps data."""
    _CATALOG_FEATURES = {
        "clump_ra": 'RA', "clump_dec": 'DEC',
        "SHAPE_E1": 'SHAPE_E1', "SHAPE_E2": 'SHAPE_E2', "SHAPE_R": 'SHAPE_R'
    }
    example = {}
    example["catalog"] = {v: data[k].astype(np.float64) for k, v in _CATALOG_FEATURES.items()}
    example["object_id"] = str(data["object_id"])
    return example


def _format_tess(data):
    """Format TESS light curve data."""
    def truncate_or_pad(arr, size=11264):
        if len(arr) > size:
            arr = arr[:size]
        elif len(arr) < size:
            arr = np.pad(arr, (0, size - len(arr)), mode="constant", constant_values=0)
        return arr.astype(np.float32)

    data["flux"] = truncate_or_pad(data["flux"])
    data["flux_err"] = truncate_or_pad(data["flux_err"])
    data["time"] = truncate_or_pad(data["time"])

    example = {
        "lightcurve": {
            "flux": data["flux"],
            "flux_err": data["flux_err"],
            "time": data["time"],
        }
    }
    example["object_id"] = str(data["object_id"])
    return example


def _format_jwst(data):
    """Format JWST imaging data."""
    _bands = ["JWST-F090W", "JWST-F115W", "JWST-F150W", "JWST-F200W",
              "JWST-F277W", "JWST-F356W", "JWST-F444W"]
    _FLOAT_FEATURES = ["mag_auto", "flux_radius", "flux_auto", "fluxerr_auto",
                       "cxx_image", "cyy_image", "cxy_image"]
    example = {
        "image": {
            "band": _bands,
            "flux": data["image_flux"],
            "ivar": data["image_ivar"],
            "mask": data["image_mask"],
            "psf_fwhm": data["image_psf_fwhm"],
            "scale": data["image_scale"],
        }
    }
    for f in _FLOAT_FEATURES:
        example[f] = data[f].astype("float32")
    example["object_id"] = str(data["object_id"])
    return example


def _format_allwise(data):
    """Format AllWISE photometry data."""
    _FLOAT_FEATURES = ["w1mpro", "w1sigmpro", "w2mpro", "w2sigmpro",
                       "w3mpro", "w3sigmpro", "w4mpro", "w4sigmpro"]
    example = {f: data[f].astype("float32") for f in _FLOAT_FEATURES}
    example["object_id"] = str(data["object_id"])
    return example


def _format_twomass(data):
    """Format 2MASS photometry data."""
    _FLOAT_FEATURES = ["j_m", "j_msigcom", "h_m", "h_msigcom", "k_m", "k_msigcom"]
    example = {f: data[f].astype("float32") for f in _FLOAT_FEATURES}
    example["object_id"] = str(data["object_id"])
    return example


def _format_provabgs(data):
    """Format PROVABGS stellar mass and SFR data."""
    _FLOAT_FEATURES = [
        "Z_HP", "Z_MW", "TAGE_MW", "AVG_SFR", "ZERR", "TSNR2_BGS",
        "MAG_G", "MAG_R", "MAG_Z", "MAG_W1", "FIBMAG_R", "HPIX_64",
        "PROVABGS_Z_MAX", "SCHLEGEL_COLOR", "PROVABGS_W_ZFAIL", "PROVABGS_W_FIBASSIGN",
    ]
    example = {
        "ra": data["ra"],
        "dec": data["dec"],
        "PROVABGS_LOGMSTAR_BF": data["PROVABGS_LOGMSTAR_BF"].astype(np.float32),
    }
    for key in _FLOAT_FEATURES:
        example[key] = data[key].astype(np.float32).squeeze()
    example["object_id"] = str(data["object_id"])
    return example


def _format_gaia_xp(data):
    """Format Gaia XP spectra coefficients."""
    _features = ["bp_coefficients", "rp_coefficients",
                 "bp_coefficient_errors", "rp_coefficient_errors"]
    example = {"object_id": str(data["object_id"])}
    for f in _features:
        example[f] = data[f].astype("float32")
    return example


def _format_gaia_ap_gspphot(data):
    """Format Gaia AstrophysicalParameters table with gspphot measurements."""
    _features = [
        "abp_gspphot", "ag_gspphot", "arp_gspphot", "azero_gspphot",
        "distance_gspphot", "ebpminrp_gspphot", "logg_gspphot",
        "mg_gspphot", "mh_gspphot", "radius_gspphot", "teff_gspphot",
    ]
    example = {"object_id": str(data["object_id"])}
    for f in _features:
        example[f] = data[f].astype("float32")
        example[f + "_lower"] = data[f + "_lower"].astype("float32")
        example[f + "_upper"] = data[f + "_upper"].astype("float32")
    return example


def _format_gaia_source_photometry(data):
    """Format Gaia Source table photometry measurements."""
    _features = [
        "phot_g_mean_flux", "phot_g_mean_flux_error",
        "phot_bp_mean_flux", "phot_bp_mean_flux_error",
        "phot_rp_mean_flux", "phot_rp_mean_flux_error",
        "bp_rp", "bp_g", "g_rp",
    ]
    example = {"object_id": str(data["object_id"])}
    for f in _features:
        example[f] = data[f].astype("float32")
    return example


def _format_gaia_parallax_sample(data):
    """Format Gaia parallax sample data."""
    _features = [
        "phot_g_mean_flux", "phot_bp_mean_flux", "phot_rp_mean_flux",
        "bp_coefficients", "rp_coefficients", "parallax", "ra", "dec",
    ]
    example = {"object_id": str(data["object_id"])}
    for f in _features:
        example[f] = data[f].astype("float32")
    return example


# =============================================================================
# CrossMatchedMMUDataPipe
# =============================================================================

class CrossMatchedMMUDataPipe(IterDataPipe):
    """Streaming data pipe for cross-matched Multimodal Universe datasets.

    This data pipe efficiently streams cross-matched data from two MMU datasets
    by performing on-the-fly spatial cross-matching using HEALPix regions. It
    supports distributed training via DDP partitioning.

    Args:
        left_dataset_path: Path to the left dataset.
        right_dataset_path: Path to the right dataset.
        left_dataset_name: Optional config name for left dataset (e.g., 'dr3_ap' for Gaia).
        right_dataset_name: Optional config name for right dataset.
        matching_radius: Radius in arcseconds to match objects between datasets.
        exclude_healpix: List of HEALPix regions to exclude from cross-matching.
        include_healpix: List of HEALPix regions to include (if specified, only these are used).

    Notes:
        - DDP partitioning: In distributed mode, HEALPix regions are split between nodes.
        - Memory efficient: Data is streamed from HDF5 files without loading entire datasets.

    Example:
        >>> pipe = CrossMatchedMMUDataPipe(
        ...     left_dataset_path="/path/to/legacysurvey",
        ...     right_dataset_path="/path/to/sdss",
        ...     matching_radius=1.0,
        ... )
        >>> for example in pipe:
        ...     print(example["object_id"], example["left"], example["right"])
    """

    def __init__(
        self,
        left_dataset_path: str,
        right_dataset_path: str,
        left_dataset_name: Optional[str] = None,
        right_dataset_name: Optional[str] = None,
        matching_radius: float = 1.0,
        exclude_healpix: Optional[List[int]] = None,
        include_healpix: Optional[List[int]] = None,
    ):
        super().__init__()

        # Apply filter for special dataset names
        if left_dataset_name is not None:
            if "dr3_ap" in left_dataset_name:
                left_dataset_name = "dr3_ap"
            elif "dr3_source" in left_dataset_name:
                left_dataset_name = "dr3_source"
        if right_dataset_name is not None:
            if "dr3_ap" in right_dataset_name:
                right_dataset_name = "dr3_ap"
            elif "dr3_source" in right_dataset_name:
                right_dataset_name = "dr3_source"

        left = load_dataset_builder(
            format_with_env(left_dataset_path),
            name=left_dataset_name,
            trust_remote_code=True,
        )
        right = load_dataset_builder(
            format_with_env(right_dataset_path),
            name=right_dataset_name,
            trust_remote_code=True,
        )
        self.exclude_healpix = exclude_healpix
        self.include_healpix = include_healpix
        self.matching_radius = matching_radius

        # Get the intersecting healpix regions between the two datasets
        (
            common_healpix,
            self.left_files,
            self.right_files,
        ) = self._get_healpix_intersection(left, right)

        # If we are in distributed mode, we split the shards between nodes
        if dist.is_available() and dist.is_initialized():
            world_size = dist.get_world_size()
            rank = dist.get_rank()
            n_files = len(common_healpix)
            if n_files < world_size:
                raise ValueError(
                    "Number of files is smaller than the number of processes."
                )
            files_per_node = n_files // world_size
            if rank == world_size - 1:
                self.healpix = common_healpix[rank * files_per_node:]
            else:
                self.healpix = common_healpix[
                    rank * files_per_node: (rank + 1) * files_per_node
                ]
        else:
            self.healpix = common_healpix

        # Identify the features to be loaded
        left_name, left_input_features, left_example_formatting = self._get_features(left)
        self.left_name = left_name
        self.left_input_features = left_input_features
        self.left_example_formatting = left_example_formatting

        right_name, right_input_features, right_example_formatting = self._get_features(right)
        self.right_name = right_name
        self.right_input_features = right_input_features
        self.right_example_formatting = right_example_formatting

    def _get_healpix_intersection(self, left, right):
        """Return intersecting HEALPix regions between two datasets and their files."""
        left_healpix = [
            int(f.split("healpix=")[1].split("/")[0])
            for f in left.config.data_files["train"]
        ]
        right_healpix = [
            int(f.split("healpix=")[1].split("/")[0])
            for f in right.config.data_files["train"]
        ]

        if self.exclude_healpix is not None:
            left_healpix = [f for f in left_healpix if f not in self.exclude_healpix]
            right_healpix = [f for f in right_healpix if f not in self.exclude_healpix]
        if self.include_healpix is not None:
            left_healpix = [f for f in left_healpix if f in self.include_healpix]
            right_healpix = [f for f in right_healpix if f in self.include_healpix]

        common_healpix = list(set(left_healpix).intersection(right_healpix))
        print("Number of common healpix regions: ", len(common_healpix))

        left_files = {
            h: [f for f in left.config.data_files["train"]
                if int(f.split("healpix=")[1].split("/")[0]) == h]
            for h in common_healpix
        }
        right_files = {
            h: [f for f in right.config.data_files["train"]
                if int(f.split("healpix=")[1].split("/")[0]) == h]
            for h in common_healpix
        }
        return common_healpix, left_files, right_files

    def _get_features(self, dataset):
        """Extract features and formatter for a dataset."""
        dset_name = dataset.dataset_name
        dset_features = dataset.info.features
        input_features = {}

        for k in dset_features.keys():
            if isinstance(dset_features[k], dict) or isinstance(dset_features[k], Sequence):
                if isinstance(dset_features[k], Sequence):
                    dset_features[k] = dset_features[k].feature
                    if not hasattr(dset_features[k], "__iter__"):
                        input_features[k] = dset_features[k]
                        continue
                for kk in dset_features[k]:
                    key = kk if dset_name in ["tess", "kepler", "spoc"] else k + "_" + kk
                    input_features[key] = dset_features[k][kk]
            else:
                input_features[k] = dset_features[k]

        input_features = {
            k: v for k, v in input_features.items()
            if not any(x in k for x in ["SPECTRO", "filter_indices", "flux_unit"])
        }

        if dset_name in ["sdss16b", "sdss"]:
            _example_formatting = _format_sdss
        elif dset_name == "desi":
            _example_formatting = _format_desi
        elif dset_name in ["decals16", "decals"]:
            _example_formatting = _format_decals
        elif dset_name == "hsc":
            _example_formatting = _format_hsc
            if "image_flux" in input_features:
                input_features["image_array"] = input_features.pop("image_flux")
        elif dset_name == "legacysurvey":
            _example_formatting = _format_legacysurvey
            input_features["image_rgb"] = input_features.pop("rgb")
            input_features["image_array"] = input_features.pop("image_flux")
        elif dset_name == "jwst":
            _example_formatting = _format_jwst
        elif dset_name == "provabgs":
            _example_formatting = _format_provabgs
        elif dset_name == "twomass":
            _example_formatting = _format_twomass
        elif dset_name == "allwise":
            _example_formatting = _format_allwise
        elif dset_name == "gz3d":
            from datasets import Array2D, Value
            all_channels = ["center", "star", "spiral", "bar"]
            image_size = 525
            input_features = {
                "object_id": Value("string"),
                "gz_total_classifications": Value("uint8"),
                "gz_spiral_votes": Value("uint8"),
                "gz_bar_votes": Value("uint8"),
                "false_color": Array2D(shape=(image_size, image_size), dtype="uint8"),
                **{channel: Array2D(shape=(image_size, image_size), dtype="uint8")
                   for channel in all_channels},
            }
            _example_formatting = _format_gz3d
        elif dset_name == "gz10":
            _example_formatting = _format_gz10
            for source, target in [("gz10_label", "ans"), ("rgb_image", "images"),
                                   ("rgb_pixel_scale", "pxscale")]:
                input_features[target] = input_features[source]
                del input_features[source]
        elif "gaia" in dset_name:
            if "xp" in dataset.config.name:
                _example_formatting = _format_gaia_xp
            elif "ap" in dataset.config.name:
                _example_formatting = _format_gaia_ap_gspphot
            elif "source" in dataset.config.name:
                _example_formatting = _format_gaia_source_photometry
            elif "parallax_sample" in dataset.config.name:
                _example_formatting = _format_gaia_parallax_sample
            else:
                raise ValueError(f"Unknown Gaia config: {dataset.config.name}")
        elif dset_name == "gzclumps":
            input_features = {
                'clump_ra': input_features['catalog_clump_ra'],
                'clump_dec': input_features['catalog_clump_dec'],
                'SHAPE_E1': input_features['catalog_SHAPE_E1'],
                'SHAPE_E2': input_features['catalog_SHAPE_E2'],
                'SHAPE_R': input_features['catalog_SHAPE_R'],
                'gCFlux': input_features['catalog_gCFlux'],
                'rCFlux': input_features['catalog_rCFlux'],
                'iCFlux': input_features['catalog_iCFlux'],
                'zCFlux': input_features['catalog_zCFlux'],
                'object_id': input_features['object_id']
            }
            _example_formatting = _format_gzclumps
        elif dset_name in ["tess", "spoc"]:
            _example_formatting = _format_tess
        else:
            raise ValueError(
                f"Unknown dataset name: {dset_name}, please implement a formatting function."
            )

        return dset_name, input_features, _example_formatting

    def _example_generator(self, left_files, right_files, worker_info):
        """Generate cross-matched examples from HDF5 files."""
        for left_file in left_files:
            with h5py.File(left_file, "r") as left_data:
                left_ra = left_data["ra"][:]
                left_dec = left_data["dec"][:]
                left_sc = SkyCoord(left_ra, left_dec, unit="deg")

                for right_file in right_files:
                    with h5py.File(right_file, "r") as right_data:
                        right_ra = right_data["ra"][:]
                        right_dec = right_data["dec"][:]

                        right_sc = SkyCoord(right_ra, right_dec, unit="deg")
                        idx, sep2d, _ = left_sc.match_to_catalog_sky(right_sc)
                        mask = sep2d < self.matching_radius * u.arcsec

                        left_idx = np.arange(len(left_ra))[mask]
                        right_idx = idx[mask]
                        assert len(left_idx) == len(right_idx)
                        n_examples = len(left_idx)

                        if worker_info is None:
                            range_start, range_end = 0, n_examples
                        else:
                            per_worker = n_examples // worker_info.num_workers
                            worker_id = worker_info.id
                            range_start = worker_id * per_worker
                            range_end = (worker_id + 1) * per_worker
                            if worker_id == worker_info.num_workers - 1:
                                range_end = n_examples

                        for n in range(range_start, range_end):
                            example_left = {
                                k: left_data[k][left_idx[n]]
                                for k in self.left_input_features
                            }
                            example_left = self.left_example_formatting(example_left)
                            example_left["ra_center"] = left_ra[left_idx[n]]
                            example_left["dec_center"] = left_dec[left_idx[n]]
                            example_left["ra"] = example_left["ra_center"]
                            example_left["dec"] = example_left["dec_center"]

                            example_right = {
                                k: right_data[k][right_idx[n]]
                                for k in self.right_input_features
                            }
                            example_right = self.right_example_formatting(example_right)
                            example_right["ra_center"] = left_ra[left_idx[n]]
                            example_right["dec_center"] = left_dec[left_idx[n]]
                            example_right["ra"] = right_ra[right_idx[n]]
                            example_right["dec"] = right_dec[right_idx[n]]

                            has_nan = False
                            for formatted_example in [example_left, example_right]:
                                for k in formatted_example:
                                    if np.issubdtype(type(formatted_example[k]), np.floating) \
                                       and np.isnan(formatted_example[k]):
                                        has_nan = True
                                        break
                                    if "spectrum" in k:
                                        spec = formatted_example[k]
                                        if (np.all(np.isnan(spec["flux"]))
                                            or np.allclose(spec["ivar"], 0.0)
                                            or np.allclose(spec["flux"], 0.0)
                                            or np.std(spec["flux"]) < 1e-9
                                            or np.all(spec["mask"])):
                                            has_nan = True
                                            break
                                if has_nan:
                                    break

                            if has_nan:
                                continue

                            yield {
                                "object_id": str(example_left["object_id"]),
                                "left": example_left,
                                "right": example_right,
                            }

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        for healpix in self.healpix:
            yield from self._example_generator(
                self.left_files[healpix], self.right_files[healpix], worker_info
            )


# =============================================================================
# CrossMatchedDataLoader (Lightning DataModule)
# =============================================================================

class CrossMatchedDataLoader(L.LightningDataModule):
    """Lightning DataModule for streaming cross-matched MMU datasets.

    This DataModule wraps CrossMatchedMMUDataPipe to provide a Lightning-compatible
    interface for training with cross-matched astronomical datasets.

    Args:
        left_dataset_path: Path to the left dataset.
        right_dataset_path: Path to the right dataset.
        left_dataset_name: Optional config name for left dataset.
        right_dataset_name: Optional config name for right dataset.
        formatting_fns: List of formatting functions to apply to both datasets.
        left_formatting_fns: Formatting functions specific to left dataset.
        right_formatting_fns: Formatting functions specific to right dataset.
        batch_size: Global batch size (divided by number of GPUs in distributed mode).
        num_workers: Number of DataLoader workers.
        exclude_healpix: List of HEALPix regions to exclude.
        include_healpix: List of HEALPix regions to include.

    Example:
        >>> loader = CrossMatchedDataLoader(
        ...     left_dataset_path="/path/to/legacysurvey",
        ...     right_dataset_path="/path/to/sdss",
        ...     batch_size=256,
        ...     num_workers=4,
        ... )
        >>> trainer = L.Trainer()
        >>> trainer.fit(model, loader)
    """

    def __init__(
        self,
        left_dataset_path: str,
        right_dataset_path: str,
        left_dataset_name: Optional[str] = None,
        right_dataset_name: Optional[str] = None,
        formatting_fns: Optional[List] = None,
        left_formatting_fns: Optional[List] = None,
        right_formatting_fns: Optional[List] = None,
        batch_size: int = 512,
        num_workers: int = 10,
        exclude_healpix: Optional[List[int]] = None,
        include_healpix: Optional[List[int]] = None,
    ) -> None:
        super().__init__()
        self.save_hyperparameters()

    def setup(self, stage):
        self.is_distributed = dist.is_available() and dist.is_initialized()
        self.batch_size = (
            self.hparams.batch_size // dist.get_world_size()
            if self.is_distributed
            else self.hparams.batch_size
        )

        dset = CrossMatchedMMUDataPipe(
            left_dataset_path=self.hparams.left_dataset_path,
            right_dataset_path=self.hparams.right_dataset_path,
            left_dataset_name=self.hparams.left_dataset_name,
            right_dataset_name=self.hparams.right_dataset_name,
            exclude_healpix=self.hparams.exclude_healpix,
            include_healpix=self.hparams.include_healpix,
        )
        left_name = dset.left_name
        right_name = dset.right_name

        def _apply_fn(ex, fn):
            return {"object_id": ex["object_id"], "left": fn(ex["left"]), "right": fn(ex["right"])}

        def _apply_left_fn(ex, fn):
            return {"object_id": ex["object_id"], "left": fn(ex["left"]), "right": ex["right"]}

        def _apply_right_fn(ex, fn):
            return {"object_id": ex["object_id"], "left": ex["left"], "right": fn(ex["right"])}

        if self.hparams.formatting_fns is not None:
            for fn in self.hparams.formatting_fns:
                dset = dset.map(partial(_apply_fn, fn=fn))

        if self.hparams.left_formatting_fns is not None:
            for fn in self.hparams.left_formatting_fns:
                dset = dset.map(partial(_apply_left_fn, fn=fn))

        if self.hparams.right_formatting_fns is not None:
            for fn in self.hparams.right_formatting_fns:
                dset = dset.map(partial(_apply_right_fn, fn=fn))

        def _combine_fn(x):
            """Combine the two datasets into a single dictionary."""
            i = x["object_id"]
            l = x["left"]
            r = x["right"]
            left = {left_name + "_" + k: v for k, v in l.items()}
            right = {right_name + "_" + k: v for k, v in r.items()}
            return {"object_id": str(i), **left, **right}

        dset = dset.map(_combine_fn)
        self.dset = dset

    def train_dataloader(self):
        return DataLoader(
            self.dset,
            batch_size=self.batch_size,
            num_workers=self.hparams.num_workers,
            pin_memory=True,
            persistent_workers=False,
            drop_last=False,
        )
