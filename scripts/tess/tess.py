import datasets
from datasets import Features, Value, Sequence
from datasets.data_files import DataFilesPatternsDict
import numpy as np 
import itertools
import h5py
import os
from astropy.io import fits
from astropy.table import Row

# TESS Sectors
PM = list(range(1, 26 + 1)) # Primary Mission
EM1 = list(range(27, 55 + 1)) # Extended Mission 1
EM2 = list(range(56, 96 + 1)) # Extended Mission 2

_CITATION = r"""% CITATION
% TESS SPOC PIPELINE
@article{Caldwell_2020,
    doi = {10.3847/2515-5172/abc9b3},
    url = {https://dx.doi.org/10.3847/2515-5172/abc9b3},
    year = {2020},
    month = {nov},
    publisher = {The American Astronomical Society},
    volume = {4},
    number = {11},
    pages = {201},
    author = {Caldwell, Douglas A. and Tenenbaum, Peter and Twicken, Joseph D. and Jenkins, Jon M. and Ting, Eric and Smith, Jeffrey C. and Hedges, Christina and Fausnaugh, Michael M. and Rose, Mark and Burke, Christopher},
    title = {TESS Science Processing Operations Center FFI Target List Products},
    journal = {Research Notes of the AAS},
    abstract = {We report the delivery to the Mikulski Archive for Space Telescopes of target pixel and light curve files for up to 160,000 targets selected from full-frame images (FFI) for each TESS Northern hemisphere observing sector. The data include calibrated target pixels, simple aperture photometry flux time series, and presearch data conditioning corrected flux time series. These data provide TESS users with high quality, uniform pipeline products for a selection of FFI targets, that would otherwise not be readily available. Additionally, we deliver cotrending basis vectors derived from the FFI targets to allow users to perform their own systematic error corrections. The selected targets include all 2 minute targets and additional targets selected from the TESS Input Catalog with a maximum of 10,000 targets per sector on each of the sixteen TESS CCDs. The data products are in the same format as the project-delivered files for the TESS 2 minute targets.}
}

% TESS QLP PIPELINE
@ARTICLE{2020RNAAS...4..204H,
       author = {{Huang}, Chelsea X. and {Vanderburg}, Andrew and {P{\'a}l}, Andras and {Sha}, Lizhou and {Yu}, Liang and {Fong}, Willie and {Fausnaugh}, Michael and {Shporer}, Avi and {Guerrero}, Natalia and {Vanderspek}, Roland and {Ricker}, George},
        title = "{Photometry of 10 Million Stars from the First Two Years of TESS Full Frame Images: Part I}",
      journal = {Research Notes of the American Astronomical Society},
     keywords = {Space observatories, Astronomy data analysis, 1543, 1858},
         year = 2020,
         month = nov,
       volume = {4},
       number = {11},
          eid = {204},
        pages = {204},
          doi = {10.3847/2515-5172/abca2e}
}

% TESS TGLC PIPELINE
@ARTICLE{2023AJ....165...71H,
       author = {{Han}, Te and {Brandt}, Timothy D.},
        title = "{TESS-Gaia Light Curve: A PSF-based TESS FFI Light-curve Product}",
      journal = {\aj},
     keywords = {Light curves, Astronomy software, Astronomy databases, Exoplanet astronomy, Variable stars, Eclipsing binary stars, 918, 1855, 83, 486, 1761, 444, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Earth and Planetary Astrophysics, Astrophysics - Solar and Stellar Astrophysics},
         year = 2023,
        month = feb,
       volume = {165},
       number = {2},
          eid = {71},
        pages = {71},
          doi = {10.3847/1538-3881/acaaa7},
archivePrefix = {arXiv},
       eprint = {2301.03704},
 primaryClass = {astro-ph.IM},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2023AJ....165...71H},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_DESCRIPTION = "TESS Full Frame Image Light Curves from multiple pipelines (SPOC, QLP, TGLC)"
_HOMEPAGE = "https://archive.stsci.edu/tess/"
_LICENSE = "CC BY 4.0"
_VERSION = "0.0.1"
_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENTS
% From: https://archive.stsci.edu/publishing/mission-acknowledgements#:~:text=org/abs/1612.05243-,TESS,-The%20acknowledgement%20text
This paper includes data collected with the TESS mission, obtained from the MAST data archive at the Space Telescope Science Institute (STScI). 
Funding for the TESS mission is provided by the NASA Explorer Program. 
STScI is operated by the Association of Universities for Research in Astronomy, Inc., under NASA contract NAS 5–26555.
"""


class CustomBuilderConfig(datasets.BuilderConfig):
    def __init__(
        self,
        lc_features = None,
        base_features = None,
        **kwargs,
    ):
        """BuilderConfig for TESS.
        
        Args:
            pipeline: One of ['spoc', 'qlp', 'tglc']
            data_files: Dict or list of files to load
        """
        super().__init__(**kwargs)
        for pipeline in ['spoc', 'qlp', 'tglc']:
            if pipeline in self.name:
                self.pipeline = pipeline
                break
        self.lc_features = lc_features
        self.base_features = base_features

class TESS(datasets.GeneratorBasedBuilder):
    """TESS Full Frame Image Light Curves Dataset.
    
    This dataset provides light curves from the TESS space telescope processed through
    multiple pipelines (SPOC, QLP, TGLC). Each entry contains time series photometry
    and associated metadata for individual stars.
    """

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        CustomBuilderConfig(
            name="spoc",
            version=VERSION,
            description="SPOC pipeline light curves",
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["spoc/*/healpix=*/*.hdf5"]}
            ),
        ),
        CustomBuilderConfig(
            name="spoc-tiny",
            version=VERSION,
            description="Tiny SPOC pipeline light curves",
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["spoc*tiny*/*/healpix=*/*.hdf5"]}
            ),
        ),
        CustomBuilderConfig(
            name="qlp", 
            version=VERSION,
            description="QLP pipeline light curves",
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["qlp/*/healpix=*/*.hdf5"]}
            ),
        ),
        CustomBuilderConfig(
            name="qlp-tiny", 
            version=VERSION,
            description="Tiny QLP pipeline light curves",
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["qlp*tiny*/*/healpix=*/*.hdf5"]}
            ),
        ),
        CustomBuilderConfig(
            name="tglc",
            version=VERSION,
            description="TGLC pipeline light curves",
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["tglc/*/healpix=*/*.hdf5"]}
            ),
        ),
        CustomBuilderConfig(
            name="tglc-tiny",
            version=VERSION,
            description="Tiny TGLC pipeline light curves",
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["tglc*tiny*/*/healpix=*/*.hdf5"]}
            ),
        ),
    ]


    DEFAULT_CONFIG_NAME = "spoc"

    def _get_feature_dict(self):
        """Get features based on pipeline type"""
        # Define common light curve features that all pipelines should have
        common_lc_features = {
            'time': Value(dtype="float32"),
            'flux': Value(dtype="float32"),
            'flux_err': Value(dtype="float32"),
        }
        
        # Pipeline-specific additional features
        features = {
            "spoc": {
                "lightcurve": {
                    **common_lc_features,
                    'quality': Value(dtype="float32")
                },
                "base": {}
            },
            "qlp": {
                "lightcurve": {
                    **common_lc_features,
                    'quality': Value(dtype="float32"),
                    'sap_flux': Value(dtype="float32"),
                    'orbitid': Value(dtype="float32"),
                    'sap_x': Value(dtype="float32"),
                    'sap_y': Value(dtype="float32"),
                    'sap_bkg': Value(dtype="float32"),
                    'sap_bkg_err': Value(dtype="float32"),
                    'kspsap_flux_sml': Value(dtype="float32"),
                    'kspsap_flux_lag': Value(dtype="float32")
                },
                "base": {
                    'tess_mag': Value(dtype="float32"),
                    'radius': Value(dtype="float32"),
                    'teff': Value(dtype="float32"),
                    'logg': Value(dtype="float32"),
                    'mh': Value(dtype="float32")
                }
            },
            "tglc": {
                "lightcurve": {
                    **common_lc_features,
                    'aper_flux': Value(dtype="float32"),
                    'tess_flags': Value(dtype="float32"),
                    'tglc_flags': Value(dtype="float32")
                },
                "base": {
                    'GAIADR3_ID': Value(dtype="string"),
                    'aper_flux_err': Value(dtype="float32"),
                    'psf_flux_err': Value(dtype="float32"),
                }
            }
        }

        base_features = {
            'RA': Value(dtype="float32"),
            'DEC': Value(dtype="float32"),
            'object_id': Value(dtype="string")
        }
        base_features.update(features[self.config.pipeline]["base"])
   
        features = {
            "lightcurve": Sequence(features[self.config.pipeline]["lightcurve"]),
            **base_features
        }
        return features

    def _info(self):
        features = self._get_feature_dict()
        
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features=Features(features),
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION
        )

    def _split_generators(self, dl_manager):
        """Returns SplitGenerators."""
        if not self.config.data_files:
            raise ValueError(
                f"At least one data file must be specified, but got data_files={self.config.data_files}"
            )
        data_files = dl_manager.download_and_extract(self.config.data_files)
        if isinstance(data_files, (str, list, tuple)):
            files = data_files
            if isinstance(files, str):
                files = [files]
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
            files = [dl_manager.iter_files(file) for file in files]
            splits.append(
                datasets.SplitGenerator(name=split_name, gen_kwargs={"files": files})
            )
        return splits

    def _generate_examples(self, files, object_ids=None):
        """Yields examples as (key, example) tuples."""
        for j, file in enumerate(itertools.chain.from_iterable(files)):
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
                    
                    # Build example based on pipeline type
                    example = self._build_example(data, i)
                    yield str(data["object_id"][i]), example
    
    def _build_example(self, data, i):
        """Build example for pipeline"""
        if self.config.pipeline == "spoc":
            return self._build_spoc_example(data, i)
        elif self.config.pipeline == "qlp":
            return self._build_qlp_example(data, i)
        elif self.config.pipeline == "tglc":
            return self._build_tglc_example(data, i)
        else:
            raise ValueError(f"Pipeline {self.config.pipeline} not supported")

    def _build_spoc_example(self, data, i):
        """Build example for SPOC pipeline"""
        return {
            "lightcurve": {
                'time': data["time"][i],
                'flux': data["flux"][i],
                'flux_err': data["flux_err"][i],
                'quality': data["quality"][i]
            },
            'RA': data["RA"][i],
            'DEC': data["DEC"][i],
            'object_id': data["object_id"][i]
        }

    def _build_qlp_example(self, data, i):
        """Build example for QLP pipeline"""
        try:
            return {
                "lightcurve": {
                    'time': data["time"][i],
                    'flux': data["kspsap_flux"][i],
                    'flux_err': data["kspsap_flux_err"][i],
                    'quality': data["quality"][i],
                    # Keep additional QLP-specific fields
                    'sap_flux': data["sap_flux"][i],
                    'orbitid': data["orbitid"][i],
                    'sap_x': data["sap_x"][i],
                    'sap_y': data["sap_y"][i],
                    'sap_bkg': data["sap_bkg"][i],
                    'sap_bkg_err': data["sap_bkg_err"][i],
                    'kspsap_flux_sml': data["kspsap_flux_sml"][i],
                    'kspsap_flux_lag': data["kspsap_flux_lag"][i]
                },
                'RA': data["RA"][i],
                'DEC': data["DEC"][i],
                'object_id': data["object_id"][i],
                'tess_mag': data["tess_mag"][i],
                'radius': data["radius"][i],
                'teff': data["teff"][i],
                'logg': data["logg"][i],
                'mh': data["mh"][i]
            }
        except Exception as e:
            print(f"Error in QLP example building: {str(e)}")
            print(f"Available keys: {list(data.keys())}")
            raise

    def _build_tglc_example(self, data, i):
        """Build example for TGLC pipeline"""
        try:
            return {
                "lightcurve": {
                    'time': data["time"][i],
                    'flux': data["psf_flux"][i],
                    'flux_err': data["psf_flux_err"][i],
                    'aper_flux': data["aper_flux"][i],
                    'tess_flags': data["tess_flags"][i],
                    'tglc_flags': data["tglc_flags"][i]
                },
                'RA': data["RA"][i],
                'DEC': data["DEC"][i],
                'object_id': data["object_id"][i],
                # 'GAIADR3_ID': data["GAIADR3_ID"][i],
                'aper_flux_err': data["aper_flux_err"][i]
            }
        except Exception as e:
            print(f"Error in TGLC example building: {str(e)}")
            print(f"Available keys: {list(data.keys())}")
            raise
