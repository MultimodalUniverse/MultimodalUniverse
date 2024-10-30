import datasets
from datasets import Features, Value, Sequence
from datasets.data_files import DataFilesPatternsDict
import numpy as np 
import itertools
import h5py

# Some data cleaning is required here especially for the NaN values.

# TESS Sectors
PM = list(range(1, 26 + 1)) # Primary Mission
EM1 = list(range(27, 55 + 1)) # Extended Mission 1
EM2 = list(range(56, 96 + 1)) # Extended Mission 2

_CITATION = """\

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
_DESCRIPTION = """
TESS-Gaia Light Curve: Light curves from TESS FFI images using GAIA based priors.
"""
_HOMEPAGE = "https://archive.stsci.edu/hlsp/tglc"
_LICENSE = "CC BY 4.0"
_VERSION = "0.0.1"


class TESS(datasets.GeneratorBasedBuilder):
    VERSION = "0.0.1"
    DEFAULT_CONFIG_NAME = "all"

    BUILDER_CONFIGS = [
    datasets.BuilderConfig(
        name="all",
        version=VERSION,
        data_files=DataFilesPatternsDict.from_patterns(
            {"train": ["./TGLC/healpix=*/*.hdf5"]} 
        ),
        description="TGLC light curves (S0023)",
        )
    ]

    
    @classmethod
    def _info(self):
        features = {"lightcurve" : Sequence({
            'time': Value(dtype="float32"),
            'psf_flux': Value(dtype="float32"),
            'aper_flux':  Value(dtype="float32"),
            'tess_flags':  Value(dtype="float32"),
            'tglc_flags':  Value(dtype="float32"),
            
        }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string"),
            'gaiadr3_id': Value(dtype="string"),
            'aper_flux_err':  Value(dtype="float32"),
            'psf_flux_err': Value(dtype="float32"),
        }

        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            features = Features(features),
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION
        )

    def _split_generators(self, dl_manager):
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
        for j, file in enumerate(itertools.chain.from_iterable(files)):

            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[j]
                else:
                    keys = data["TIC_ID"][:]
                
                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["TIC_ID"][:])
                sorted_ids = data["TIC_ID"][:][sort_index]
               
                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]
                    example = {
                        "lightcurve" : {
                            'time': data["time"][i],
                            'psf_flux': data["psf_flux"][i],
                            'aper_flux':  data["aper_flux"][i],
                            'tess_flags':  data["tess_flags"][i],
                            'tglc_flags':  data["tglc_flags"][i]
                        }, 
                        'RA':  data["RA"][i],
                        'DEC':  data["DEC"][i],
                        'TIC_ID': data["TIC_ID"][i],
                        'gaiadr3_id': data["gaiadr3_id"][i],
                        'psf_flux_err': data["psf_flux_err"][i],
                        'aper_flux_err':  data["aper_flux_err"][i]
                    }
                    yield str(data["TIC_ID"][i]), example