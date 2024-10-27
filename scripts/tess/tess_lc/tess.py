import datasets
from datasets import Features, Value, Array2D, Sequence
from datasets.data_files import DataFilesPatternsDict
import numpy as np 
import itertools
import h5py

class TESS(datasets.GeneratorBasedBuilder):
    VERSION = "0.0.1"

    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="all",
            version=VERSION,
            data_files=DataFilesPatternsDict.from_patterns(
                {"train": ["tess_data/s0023/lcs/healpix=*/*.hdf5"]}  # Fix this path, inflexible
            ),
            description="TGLC light curves (S0064)",
        )
    ]

    DEFAULT_CONFIG_NAME = "all"

    @classmethod
    def _info(self):
        features = {"lightcurve" : Sequence({
            'time': Value(dtype="float32"),
            'psf_flux': Value(dtype="float32"),
            'psf_flux_err': Value(dtype="float32"),
            'aper_flux':  Value(dtype="float32"),
            'aper_flux_err':  Value(dtype="float32"),
            'tess_flags':  Value(dtype="int8"),
            'tglc_flags':  Value(dtype="int8")
        }),
        'TIC_ID': Value(dtype="string"),
        'gaiadr3_id': Value(dtype="string"),
        'RA':  Value(dtype="float32"),
        'DEC':  Value(dtype="float32")
        }

        return datasets.DatasetInfo(
            description="TESS-GAIA light curves (S0023)",
            features = Features(features),
            description="FFI images from S23 TESS",
            # Add the other metadata
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
                            'TIC_ID': data["TIC_ID"][i],
                            'gaiadr3_id': data["gaiadr3_id"][i],
                            'time': data["time"][i],
                            'psf_flux': data["psf_flux"][i],
                            'psf_flux_err': data["psf_flux_err"][i],
                            'aper_flux':  data["aper_flux"][i],
                            'aper_flux_err':  data["aper_flux_err"][i],
                            'tess_flags':  data["tess_flags"][i],
                            'tglc_flags':  data["tglc_flags"][i],
                            'RA':  data["RA"][i],
                            'DEC':  data["DEC"][i]
                        }
                    }
                    yield str(data["TIC_ID"][i]), example
