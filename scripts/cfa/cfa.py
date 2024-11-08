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
import h5py
import numpy as np
import datasets
from datasets import Features, Value, Sequence
from datasets.data_files import DataFilesPatternsDict

_VERSION = "0.0.1"

_HOMEPAGE = "https://lweb.cfa.harvard.edu/supernova/"
_LICENSE = "CC BY 4.0"

# Features common across all CFA datasets
_STR_FEATURES = ["object_id", "obj_type"]
_FLOAT_FEATURES = ["ra", "dec"]

# Dataset-specific descriptions and citations
_DESCRIPTIONS = {
    "cfa3": "Time-series dataset from the Center for Astronomy 3 Data Release.",
    "cfa4": "Time-series dataset from the Center for Astronomy 4 Data Release.",
    "cfa_SECCSN": "Time-series dataset from the Center for Astronomy 3 Stripped-envelope Core-collapse SN Data Release.",
    "cfa_snII": "Time-series dataset from the Center for Astronomy Type II Supernovae.",
}

_CITATIONS = {
    "cfa3": """@ARTICLE{2009ApJ...700..331H,
    author = {{Hicken}, Malcolm and {Challis}, Peter and {Jha}, Saurabh and {Kirshner}, Robert P. and {Matheson}, Tom and {Modjaz}, Maryam and {Rest}, Armin and {Wood-Vasey}, W. Michael and {Bakos}, Gaspar and {Barton}, Elizabeth J. and {Berlind}, Perry and {Bragg}, Ann and {Brice{\~n}o}, Cesar and {Brown}, Warren R. and {Caldwell}, Nelson and {Calkins}, Mike and {Cho}, Richard and {Ciupik}, Larry and {Contreras}, Maria and {Dendy}, Kristi-Concannon and {Dosaj}, Anil and {Durham}, Nick and {Eriksen}, Kris and {Esquerdo}, Gil and {Everett}, Mark and {Falco}, Emilio and {Fernandez}, Jose and {Gaba}, Alejandro and {Garnavich}, Peter and {Graves}, Genevieve and {Green}, Paul and {Groner}, Ted and {Hergenrother}, Carl and {Holman}, Matthew J. and {Hradecky}, Vit and {Huchra}, John and {Hutchison}, Bob and {Jerius}, Diab and {Jordan}, Andres and {Kilgard}, Roy and {Krauss}, Miriam and {Luhman}, Kevin and {Macri}, Lucas and {Marrone}, Daniel and {McDowell}, Jonathan and {McIntosh}, Daniel and {McNamara}, Brian and {Megeath}, Tom and {Mochejska}, Barbara and {Munoz}, Diego and {Muzerolle}, James and {Naranjo}, Orlando and {Narayan}, Gautham and {Pahre}, Michael and {Peters}, Wayne and {Peterson}, Dawn and {Rines}, Ken and {Ripman}, Ben and {Roussanova}, Anna and {Schild}, Rudolph and {Sicilia-Aguilar}, Aurora and {Sokoloski}, Jennifer and {Smalley}, Kyle and {Smith}, Andy and {Spahr}, Tim and {Stanek}, K.~Z. and {Barmby}, Pauline and {Blondin}, St{\'e}phane and {Stubbs}, Christopher W. and {Szentgyorgyi}, Andrew and {Torres}, Manuel A.~P. and {Vaz}, Amili and {Vikhlinin}, Alexey and {Wang}, Zhong and {Westover}, Mike and {Woods}, Deborah and {Zhao}, Ping},
    title = "{CfA3: 185 Type Ia Supernova Light Curves from the CfA}",
    journal = {\apj},
    keywords = {supernovae: general, Astrophysics - Cosmology and Extragalactic Astrophysics},
    year = 2009,
    month = jul,
    volume = {700},
    number = {1},
    pages = {331-357},
    doi = {10.1088/0004-637X/700/1/331},
    archivePrefix = {arXiv},
    eprint = {0901.4787},
    primaryClass = {astro-ph.CO},
    adsurl = {https://ui.adsabs.harvard.edu/abs/2009ApJ...700..331H},
    adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}""",
    "cfa4": """2012ApJS..200...12H,
    author = {{Hicken}, Malcolm and {Challis}, Peter and {Kirshner}, Robert P. and {Rest}, Armin and {Cramer}, Claire E. and {Wood-Vasey}, W. Michael and {Bakos}, Gaspar and {Berlind}, Perry and {Brown}, Warren R. and {Caldwell}, Nelson and {Calkins}, Mike and {Currie}, Thayne and {de Kleer}, Kathy and {Esquerdo}, Gil and {Everett}, Mark and {Falco}, Emilio and {Fernandez}, Jose and {Friedman}, Andrew S. and {Groner}, Ted and {Hartman}, Joel and {Holman}, Matthew J. and {Hutchins}, Robert and {Keys}, Sonia and {Kipping}, David and {Latham}, Dave and {Marion}, George H. and {Narayan}, Gautham and {Pahre}, Michael and {Pal}, Andras and {Peters}, Wayne and {Perumpilly}, Gopakumar and {Ripman}, Ben and {Sipocz}, Brigitta and {Szentgyorgyi}, Andrew and {Tang}, Sumin and {Torres}, Manuel A.~P. and {Vaz}, Amali and {Wolk}, Scott and {Zezas}, Andreas},
    title = "{CfA4: Light Curves for 94 Type Ia Supernovae}",
    journal = {\apjs},
    keywords = {supernovae: general, Astrophysics - Cosmology and Nongalactic Astrophysics},
    year = 2012,
    month = jun,
    volume = {200},
    number = {2},
    eid = {12},
    pages = {12},
    doi = {10.1088/0067-0049/200/2/12},
    archivePrefix = {arXiv},
    eprint = {1205.4493},
    primaryClass = {astro-ph.CO},
    adsurl = {https://ui.adsabs.harvard.edu/abs/2012ApJS..200...12H},
    adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}""",
    "cfa_SECCSN": """@ARTICLE{2014ApJS..213...19B,
    author = {{Bianco}, F.~B. and {Modjaz}, M. and {Hicken}, M. and {Friedman}, A. and {Kirshner}, R.~P. and {Bloom}, J.~S. and {Challis}, P. and {Marion}, G.~H. and {Wood-Vasey}, W.~M. and {Rest}, A.},
    title = "{Multi-color Optical and Near-infrared Light Curves of 64 Stripped-envelope Core-Collapse Supernovae}",
    journal = {\apjs},
    keywords = {supernovae: general, Astrophysics - Solar and Stellar Astrophysics, Astrophysics - High Energy Astrophysical Phenomena},
    year = 2014,
    month = aug,
    volume = {213},
    number = {2},
    eid = {19},
    pages = {19},
    doi = {10.1088/0067-0049/213/2/19},
    archivePrefix = {arXiv},
    eprint = {1405.1428},
    primaryClass = {astro-ph.SR},
    adsurl = {https://ui.adsabs.harvard.edu/abs/2014ApJS..213...19B},
    adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}""",
    "cfa_snII": """@ARTICLE{2017ApJS..233....6H,
    author = {{Hicken}, Malcolm and {Friedman}, Andrew S. and {Blondin}, Stephane and {Challis}, Peter and {Berlind}, Perry and {Calkins}, Mike and {Esquerdo}, Gil and {Matheson}, Thomas and {Modjaz}, Maryam and {Rest}, Armin and {Kirshner}, Robert P.},
    title = "{Type II Supernova Light Curves and Spectra from the CfA}",
    journal = {\apjs},
    keywords = {supernovae: general, Astrophysics - High Energy Astrophysical Phenomena},
    year = 2017,
    month = nov,
    volume = {233},
    number = {1},
    eid = {6},
    pages = {6},
    doi = {10.3847/1538-4365/aa8ef4},
    archivePrefix = {arXiv},
    eprint = {1706.01030},
    primaryClass = {astro-ph.HE},
    adsurl = {https://ui.adsabs.harvard.edu/abs/2017ApJS..233....6H},
    adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}""",
}

ACKNOWLEDGEMENTS = """This research has made use of the CfA Supernova Archive, which is funded in part by the National Science Foundation through grant AST 0907903."""

class CFA(datasets.GeneratorBasedBuilder):
    """CFA Supernova Light Curve Dataset Collection"""

    VERSION = _VERSION

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="cfa3",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./cfa3/healpix=*/*.hdf5"]}),
            description=_DESCRIPTIONS["cfa3"],
        ),
        datasets.BuilderConfig(
            name="cfa4", 
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./cfa4/healpix=*/*.hdf5"]}),
            description=_DESCRIPTIONS["cfa4"],
        ),
        datasets.BuilderConfig(
            name="cfa_SECCSN",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./cfa_SECCSN/healpix=*/*.hdf5"]}),
            description=_DESCRIPTIONS["cfa_SECCSN"],
        ),
        datasets.BuilderConfig(
            name="cfa_snII",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns({"train": ["./cfa_snII/healpix=*/*.hdf5"]}),
            description=_DESCRIPTIONS["cfa_snII"],
        ),
    ]

    DEFAULT_CONFIG_NAME = "cfa3"

    def _info(self):
        """Defines the features available in this dataset."""
        features = {
            "lightcurve": Sequence(
                feature={
                    "band": Value("string"),
                    "time": Value("float32"),
                    "mag": Value("float32"),
                    "mag_err": Value("float32"),
                }
            ),
        }

        # Adding all values from the catalog
        for f in _FLOAT_FEATURES:
            features[f] = Value("float32")
        for f in _STR_FEATURES:
            features[f] = Value("string")

        return datasets.DatasetInfo(
            description=_DESCRIPTIONS[self.config.name],
            features=Features(features),
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATIONS[self.config.name],
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
        for file_number, file in enumerate(itertools.chain.from_iterable(files)):
            with h5py.File(file, "r") as data:
                if object_ids is not None:
                    keys = object_ids[file_number]
                else:
                    keys = [data["object_id"][()]]

                for k in keys:
                    # Parse data
                    idxs = np.arange(0, data["mag"].shape[0])
                    band_idxs = idxs.repeat(data["mag"].shape[-1]).reshape(
                        len([bstr.decode("utf-8") for bstr in data["bands"][()]]), -1
                    )
                    example = {
                        "lightcurve": {
                            "band": np.asarray(
                                [
                                    data["bands"][()][band_number]
                                    for band_number in band_idxs.flatten().astype(
                                        "int32"
                                    )
                                ]
                            ).astype("str"),
                            "time": np.asarray(data["time"])
                            .flatten()
                            .astype("float32"),
                            "mag": np.asarray(data["mag"]).flatten().astype("float32"),
                            "mag_err": np.asarray(data["mag_err"])
                            .flatten()
                            .astype("float32"),
                        },
                    }
                    # Add remaining features
                    for f in _FLOAT_FEATURES:
                        example[f] = np.asarray(data[f]).astype("float32")
                    for f in _STR_FEATURES:
                        # Add band names shared across dataset to each sample. The bands are read from the hdf5 file.
                        if f == "bands":
                            example[f] = np.asarray(data[f]).astype("str")
                        else:
                            example[f] = data[f][()].astype("str")

                    yield str(data["object_id"][()]), example
