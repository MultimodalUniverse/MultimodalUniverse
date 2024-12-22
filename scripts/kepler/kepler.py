import datasets
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
import itertools
import h5py
import numpy as np

# TODO: Add BibTeX citation
# Find for instance the citation on arxiv or on the dataset repo/website
_CITATION = r"""% CITATION
@article{Mathur_2017,
doi = {10.3847/1538-4365/229/2/30},
url = {https://dx.doi.org/10.3847/1538-4365/229/2/30},
year = {2017},
month = {mar},
publisher = {The American Astronomical Society},
volume = {229},
number = {2},
pages = {30},
author = {Savita Mathur and Daniel Huber and Natalie M. Batalha and David R. Ciardi and Fabienne A. Bastien and Allyson Bieryla and Lars A. Buchhave and William D. Cochran and Michael Endl and Gilbert A. Esquerdo and Elise Furlan and Andrew Howard and Steve B. Howell and Howard Isaacson and David W. Latham and Phillip J. MacQueen and David R. Silva},
title = {Revised Stellar Properties of Kepler Targets for the Q1-17 (DR25) Transit Detection Run},
journal = {The Astrophysical Journal Supplement Series},
abstract = {The determination of exoplanet properties and occurrence rates using Kepler data critically depends on our knowledge of the fundamental properties (such as temperature, radius, and mass) of the observed stars. We present revised stellar properties for 197,096 Kepler targets observed between Quarters 1–17 (Q1-17), which were used for the final transiting planet search run by the Kepler Mission (Data Release 25, DR25). Similar to the Q1–16 catalog by Huber et al., the classifications are based on conditioning published atmospheric parameters on a grid of Dartmouth isochrones, with significant improvements in the adopted method and over 29,000 new sources for temperatures, surface gravities, or metallicities. In addition to fundamental stellar properties, the new catalog also includes distances and extinctions, and we provide posterior samples for each stellar parameter of each star. Typical uncertainties are ∼27% in radius, ∼17% in mass, and ∼51% in density, which is somewhat smaller than previous catalogs because of the larger number of improved  constraints and the inclusion of isochrone weighting when deriving stellar posterior distributions. On average, the catalog includes a significantly larger number of evolved solar-type stars, with an increase of 43.5% in the number of subgiants. We discuss the overall changes of radii and masses of Kepler targets as a function of spectral type, with a particular focus on exoplanet host stars.}
}
"""

_ACKNOWLEDGEMENTS = r"""% ACKNOWLEDGEMENT
% From: https://archive.stsci.edu/publishing/mission-acknowledgements
This paper includes data collected by the Kepler mission and obtained from the MAST data archive at the Space Telescope Science Institute (STScI). Funding for the Kepler mission is provided by the NASA Science Mission Directorate. STScI is operated by the Association of Universities for Research in Astronomy, Inc., under NASA contract NAS 5–26555."""

# You can copy an official description
_DESCRIPTION = """\
TESS Light Curves From Full Frame Images ("TESS-SPOC")
"""

_HOMEPAGE = "https://archive.stsci.edu/missions-and-data/kepler"

_LICENSE = "CC BY 4.0"

_VERSION = "1.0.0"

# Full list of features available here:
# "https://archive.stsci.edu/files/live/sites/mast/files/home/missions-and-data/k2/_documents/MAST_Kepler_Archive_Manual_2020.pdf"

_FLOAT_FEATURES = ["ra", "dec"]

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
                {"train": ["data/healpix=*/*.hdf5"]}  # Fix this path, inflexible
            ),
            description="Kepler light curves long cadence",
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
                    "pdcsap_flux": Value(dtype="float32"),
                    "pdcsap_flux_err": Value(dtype="float32"),
                    "sap_flux": Value(dtype="float32"),
                    "sap_flux_err": Value(dtype="float32"),
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
                            "pdcsap_flux": data["pdcsap_flux"][i],
                            "pdcsap_flux_err": data["pdcsap_flux_err"][i],
                            "sap_flux": data["sap_flux"][i],
                            "sap_flux_err": data["sap_flux_err"][i],
                        }
                    }
                    # Add all other requested features
                    for f in _FLOAT_FEATURES:
                        example[f] = data[f][i].astype("float32")

                    # Add object_id
                    example["object_id"] = str(data["object_id"][i])

                    yield str(data["object_id"][i]), example
