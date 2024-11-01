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
@ARTICLE{2020RNAAS...4..204H,
       author = {{Huang}, Chelsea X. and {Vanderburg}, Andrew and {P{\'a}l}, Andras and {Sha}, Lizhou and {Yu}, Liang and {Fong}, Willie and {Fausnaugh}, Michael and {Shporer}, Avi and {Guerrero}, Natalia and {Vanderspek}, Roland and {Ricker}, George},
        title = "{Photometry of 10 Million Stars from the First Two Years of TESS Full Frame Images: Part I}",
      journal = {Research Notes of the American Astronomical Society},
     keywords = {Space observatories, Astronomy data analysis, 1543, 1858, Astrophysics - Earth and Planetary Astrophysics, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Solar and Stellar Astrophysics},
         year = 2020,
        month = nov,
       volume = {4},
       number = {11},
          eid = {204},
        pages = {204},
          doi = {10.3847/2515-5172/abca2e},
archivePrefix = {arXiv},
       eprint = {2011.06459},
 primaryClass = {astro-ph.EP},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2020RNAAS...4..204H},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}


"""
_DESCRIPTION = """
Quick look-up pipeline: Light curves from TESS FFI images.
"""
_HOMEPAGE = "https://archive.stsci.edu/hlsp/qlp"
_LICENSE = "CC BY 4.0"
_VERSION = "0.0.1"

class QLP_Config(datasets.BuilderConfig):
    """BuilderConfig for QLP."""

    def __init__(self, data_files, **kwargs):
        """BuilderConfig for QLP.

        Parameters
        ----------

        Returns
        -------
        None
        """
        super().__init__(**kwargs)

        self.data_files = data_files
        self.citation = _CITATION
        self.homepage = _HOMEPAGE
        self.version = _VERSION
        self.license = _LICENSE

class QLP(datasets.GeneratorBasedBuilder):
    VERSION = "0.0.1"
    DEFAULT_CONFIG_NAME = "all"

    BUILDER_CONFIG_CLASS = QLP_Config
        
    @classmethod
    def _info(self):
        features = {"lightcurve" : Sequence({
            'time': Value(dtype="float32"),
            'sap_flux': Value(dtype="float32"),
            'kspsap_flux':  Value(dtype="float32"),
            'kspsap_flux_err':  Value(dtype="float32"),
            'quality':  Value(dtype="float32"), 
            'orbitid':  Value(dtype="float32"),
            'sap_x':  Value(dtype="float32"),
            'sap_y':  Value(dtype="float32"),
            'sap_bkg':  Value(dtype="float32"),
            'sap_bkg_err':  Value(dtype="float32"),
            'kspsap_flux_sml':  Value(dtype="float32"),
            'kspsap_flux_lag':  Value(dtype="float32"),
            }),
            'RA':  Value(dtype="float32"),
            'DEC':  Value(dtype="float32"),
            'TIC_ID': Value(dtype="string"),
            'tess_mag': Value(dtype="float32"),
            'radius':  Value(dtype="float32"),
            'teff': Value(dtype="float32"),
            'logg': Value(dtype="float32"),
            'mh': Value(dtype="float32")
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
                   
                    example = {"lightcurve" : {
                        'time':  data["time"][i],
                        'sap_flux': data["sap_flux"][i],
                        'kspsap_flux':  data["kspsap_flux"][i],
                        'kspsap_flux_err':  data["kspsap_flux_err"][i],
                        'quality':  data["quality"][i], 
                        'orbitid':   data["orbitid"][i],
                        'sap_x':  data["sap_x"][i],
                        'sap_y':  data["sap_y"][i],
                        'sap_bkg':  data["sap_bkg"][i],
                        'sap_bkg_err':  data["sap_bkg_err"][i],
                        'kspsap_flux_sml':  data["kspsap_flux_sml"][i],
                        'kspsap_flux_lag':  data["kspsap_flux_lag"][i],
                        },
                        'RA':  data["RA"][i],
                        'DEC':  data["DEC"][i],
                        'TIC_ID': data["TIC_ID"][i],
                        'tess_mag': data["tess_mag"][i],
                        'radius':  data["radius"][i],
                        'teff': data["teff"][i],
                        'logg': data["logg"][i],
                        'mh': data["mh"][i]
                    }
                    yield str(data["TIC_ID"][i]), example 
                    
                    # Note here that there is duplication between the TIC_ID in some cases causing the dataloader to fail.
                    # This is likely a problem with the TGLC data. Needs more investigation.
