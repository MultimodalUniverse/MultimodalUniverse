import datasets
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
import itertools
import h5py
import numpy as np

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@ARTICLE{2020RNAAS...4..201C,
       author = {{Caldwell}, Douglas A. and {Tenenbaum}, Peter and {Twicken}, Joseph D. and {Jenkins}, Jon M. and {Ting}, Eric and {Smith}, Jeffrey C. and {Hedges}, Christina and {Fausnaugh}, Michael M. and {Rose}, Mark and {Burke}, Christopher},
        title = "{TESS Science Processing Operations Center FFI Target List Products}",
      journal = {Research Notes of the American Astronomical Society},
     keywords = {Catalogs, CCD photometry, Stellar photometry, 205, 208, 1620, Astrophysics - Earth and Planetary Astrophysics, Astrophysics - Instrumentation and Methods for Astrophysics, Astrophysics - Solar and Stellar Astrophysics},
         year = 2020,
        month = nov,
       volume = {4},
       number = {11},
          eid = {201},
        pages = {201},
          doi = {10.3847/2515-5172/abc9b3},
archivePrefix = {arXiv},
       eprint = {2011.05495},
 primaryClass = {astro-ph.EP},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2020RNAAS...4..201C},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENT
% From: https://archive.stsci.edu/publishing/mission-acknowledgements
This paper includes data collected with the TESS mission, obtained from the MAST data archive at the Space Telescope Science Institute (STScI). Funding for the TESS mission is provided by the NASA Explorer Program. STScI is operated by the Association of Universities for Research in Astronomy, Inc., under NASA contract NAS 5–26555.
"""

# You can copy an official description
_DESCRIPTION = """\
TESS Light Curves From Full Frame Images ("TESS-SPOC")
"""

_HOMEPAGE = "https://archive.stsci.edu/hlsp/tess-spoc"

_LICENSE = "CC BY 4.0"

_VERSION = "0.0.1"

# Full list of features available here:
# https://archive.stsci.edu/files/live/sites/mast/files/home/missions-and-data/active-missions/tess/_documents/EXP-TESS-ARC-ICD-TM-0014-Rev-F.pdf
# https://cdsarc.cds.unistra.fr/ftp/IV/39/ReadMe
# TODO: add additional features from the TIC catalog

_FLOAT_FEATURES = ["RA", "DEC"]

# Features that correspond to fluxes
# _FLUX_FEATURES = [
#    "FLUX",
#    "FLUX_ERR",
# ]


class TESS(datasets.GeneratorBasedBuilder):
    """TESS Light Curves From Full Frame Images from the TESS Science Processing Operations Center ("TESS-SPOC")"""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="all",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["spoc/healpix=*/*.hdf5"]}  # Fix this path, inflexible
            ),
            description="TESS-SPOC light curves (S0064)",
        )
    ]

    DEFAULT_CONFIG_NAME = "all"

    @classmethod
    def _info(self):
        """Defines the features available in this dataset."""
        # Starting with all features common to time series datasets
        features = {
            "lightcurve": Sequence(
                {
                    "time": Value(dtype="float32"),
                    "flux": Value(dtype="float32"),
                    "flux_err": Value(dtype="float32"),
                }
            )
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
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
                    keys = data["object_id"][:]

                # Preparing an index for fast searching through the catalog
                sort_index = np.argsort(data["object_id"][:])
                sorted_ids = data["object_id"][:][sort_index]

                for k in keys:
                    # Extract the indices of requested ids in the catalog
                    i = sort_index[np.searchsorted(sorted_ids, k)]

                    # Parse light curve data
                    example = {
                        "lightcurve": {
                            "time": data["time"][i],
                            "flux": data["flux"][i],
                            "flux_err": data["flux_err"][i],
                        }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
